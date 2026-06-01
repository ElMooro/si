"""1119 — schedule pump-radar-brief + invoke + show synthesis.

Also re-invokes pump-mechanics with the marketCap/price shares_outstanding fix
to populate float tiers in the live data.
"""
import json, pathlib, time, traceback
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1119_master_brief.json"
lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=350))
s3  = boto3.client("s3", region_name="us-east-1")
ebs = boto3.client("scheduler", region_name="us-east-1")


def phase(out, name, fn):
    try:
        r = fn()
        out["phases"].append({"name": name, "status": "ok", "result": r})
        return r
    except Exception as e:
        out["phases"].append({"name": name, "status": "ERROR",
                                "error": str(e)[:400],
                                "traceback": traceback.format_exc()[:1500]})
        return None


def setup_schedule(fn_name: str) -> dict:
    cfg = json.load(open(f"aws/lambdas/{fn_name}/config.json"))["eventbridge_scheduler"]
    name = cfg["schedule_name"]
    try:
        ebs.delete_schedule(Name=name); time.sleep(1)
    except ebs.exceptions.ResourceNotFoundException: pass
    ebs.create_schedule(
        Name=name, ScheduleExpression=cfg["cron"],
        ScheduleExpressionTimezone=cfg["timezone"],
        Description=cfg.get("description", ""), State="ENABLED",
        FlexibleTimeWindow={"Mode": "OFF"},
        Target={"Arn": f"arn:aws:lambda:us-east-1:857687956942:function:{fn_name}",
                  "RoleArn": cfg["role_arn"],
                  "Input": json.dumps({"source": "scheduler"})})
    return {"schedule": name, "cron": cfg["cron"]}


def invoke(fn_name: str) -> dict:
    t0 = time.time()
    r = lam.invoke(FunctionName=fn_name, InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    body = r["Payload"].read().decode("utf-8", errors="replace")
    try:
        p = json.loads(body)
        if isinstance(p.get("body"), str):
            try: return {"elapsed_s": elapsed, "summary": json.loads(p["body"])}
            except Exception: return {"elapsed_s": elapsed, "body": p["body"][:400]}
        return {"elapsed_s": elapsed, "p": str(p)[:400]}
    except Exception: return {"elapsed_s": elapsed, "raw": body[:600]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "phases": []}

    # Phase 1: verify brief lambda exists
    def check():
        d = lam.get_function(FunctionName="justhodl-pump-radar-brief")["Configuration"]
        return {
            "state":  d["State"],
            "code_kb": round(d["CodeSize"]/1024, 1),
            "memory": d["MemorySize"],
            "has_anth": bool((d.get("Environment", {}) or {}).get("Variables", {}).get("ANTHROPIC_API_KEY")),
        }
    phase(out, "check_brief_lambda", check)

    # Phase 2: setup schedule for brief
    phase(out, "setup_brief_schedule",
            lambda: setup_schedule("justhodl-pump-radar-brief"))

    # Phase 3: re-invoke pump-mechanics with shares_outstanding fix
    phase(out, "reinvoke_pump_mechanics",
            lambda: invoke("justhodl-pump-mechanics"))

    # Phase 4: verify mechanics float tiers now populate
    def verify_mechanics():
        time.sleep(2)
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/pump-mechanics.json")
        d = json.loads(obj["Body"].read())
        cands = d.get("candidates", [])
        # Count how many have a real float_tier (not "?")
        with_tier = [c["ticker"] for c in cands if (c.get("squeeze_profile") or {}).get("float_tier") != "?"]
        sample = []
        for c in cands[:6]:
            sq = c.get("squeeze_profile") or {}
            sample.append({
                "ticker":    c["ticker"],
                "tier":      sq.get("float_tier"),
                "shares_b":  round((sq.get("shares_outstanding") or 0)/1e9, 2),
                "score":     sq.get("squeeze_proxy_score"),
                "status":    sq.get("squeeze_potential"),
                "rotation":  sq.get("rotation_today"),
                "accel":     sq.get("rotation_accel"),
            })
        return {
            "total":        len(cands),
            "with_tier":    len(with_tier),
            "with_tier_tickers": with_tier,
            "samples":      sample,
        }
    phase(out, "verify_mechanics_fix", verify_mechanics)

    # Phase 5: invoke pump-radar-brief (this is the big one)
    phase(out, "invoke_brief", lambda: invoke("justhodl-pump-radar-brief"))

    # Phase 6: read brief output + extract narrative
    def read_brief():
        time.sleep(2)
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/pump-radar-brief.json")
        d = json.loads(obj["Body"].read())
        pathlib.Path("aws/ops/reports/1119_brief_full.json").write_text(
            json.dumps(d, indent=2, default=str))
        if d.get("status") == "error":
            return {"status": "error", "error": d.get("error"), "preview": (d.get("raw_preview") or "")[:300]}
        return {
            "size_kb":             round(obj["ContentLength"]/1024, 1),
            "elapsed_sec":         d.get("elapsed_sec"),
            "claude_elapsed":      d.get("claude_elapsed"),
            "conviction_grade":    d.get("conviction_grade"),
            "executive_summary":   d.get("executive_summary"),
            "macro_frame":         d.get("macro_frame"),
            "market_temperature":  d.get("market_temperature"),
            "n_long_ideas":        len(d.get("top_3_long_ideas", [])),
            "n_pair_trades":       len(d.get("top_2_pair_trades", [])),
            "n_watch_items":       len(d.get("what_to_watch_today", [])),
            "n_warnings":          len(d.get("risk_warnings", [])),
            "top_3_long_ideas":    d.get("top_3_long_ideas", []),
            "top_2_pair_trades":   d.get("top_2_pair_trades", []),
            "what_to_watch_today": d.get("what_to_watch_today", []),
            "risk_warnings":       d.get("risk_warnings", []),
            "whats_changed":       d.get("whats_changed_narrative"),
            "whats_changed_data":  d.get("whats_changed_data"),
            "source_versions":     d.get("source_versions"),
        }
    phase(out, "read_brief", read_brief)

    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1119] DONE")


if __name__ == "__main__":
    main()
