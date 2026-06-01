"""1122 — earliness chain: theme-classifier → velocity-acceleration → radar.

1. Schedule theme-classifier (6h) and velocity-acceleration (hourly :30)
2. Invoke theme-classifier first → builds data/themes.json
3. Invoke velocity-acceleration → builds data/velocity-acceleration.json + state
4. Re-invoke convergence-radar so it picks up the new engine on next run
5. Read everything and show what fired
"""
import json, pathlib, time, traceback
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1122_earliness_chain.json"
lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=350))
s3  = boto3.client("s3", region_name="us-east-1")
ebs = boto3.client("scheduler", region_name="us-east-1")


def phase(out, name, fn):
    try:
        r = fn()
        out["phases"].append({"name": name, "status": "ok", "result": r})
        return r
    except Exception as e:
        out["phases"].append({"name": name, "status": "ERROR", "error": str(e)[:400],
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
            except Exception: return {"elapsed_s": elapsed, "body": p["body"][:600]}
        return {"elapsed_s": elapsed, "p": str(p)[:600]}
    except Exception: return {"elapsed_s": elapsed, "raw": body[:600]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "phases": []}

    # Phase 1: verify both Lambdas exist
    for fn in ["justhodl-theme-classifier", "justhodl-velocity-acceleration"]:
        phase(out, f"check_{fn[9:]}", lambda fn=fn: (lambda d: {
            "state":    d["State"],
            "code_kb":  round(d["CodeSize"]/1024, 1),
            "memory":   d["MemorySize"],
            "timeout":  d["Timeout"],
        })(lam.get_function(FunctionName=fn)["Configuration"]))

    # Phase 2: schedule both
    phase(out, "schedule_themes", lambda: setup_schedule("justhodl-theme-classifier"))
    phase(out, "schedule_accel",  lambda: setup_schedule("justhodl-velocity-acceleration"))

    # Phase 3: invoke theme-classifier first
    phase(out, "invoke_themes", lambda: invoke("justhodl-theme-classifier"))

    # Phase 4: verify themes output
    def read_themes():
        time.sleep(2)
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/themes.json")
        d = json.loads(obj["Body"].read())
        pathlib.Path("aws/ops/reports/1122_themes_full.json").write_text(
            json.dumps(d, indent=2, default=str))
        if d.get("status") == "error":
            return {"status": "error", "error": d.get("error")}
        return {
            "size_kb":          round(obj["ContentLength"]/1024, 1),
            "elapsed_sec":      d.get("elapsed_sec"),
            "n_classified":    d.get("n_classified"),
            "n_active_themes": d.get("n_active_themes"),
            "themes_summary": [
                {
                    "label":        t.get("label"),
                    "industry":     k,
                    "n_leaders":    t.get("n_leaders"),
                    "avg_momentum": t.get("avg_momentum"),
                    "top_ticker":   t.get("top_ticker"),
                    "tickers":      t.get("tickers", [])[:10],
                }
                for k, t in list((d.get("themes") or {}).items())[:8]
            ],
            "unclassified":     d.get("unclassified", []),
        }
    phase(out, "read_themes", read_themes)

    # Phase 5: invoke velocity-acceleration
    phase(out, "invoke_accel", lambda: invoke("justhodl-velocity-acceleration"))

    # Phase 6: verify acceleration output
    def read_accel():
        time.sleep(2)
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/velocity-acceleration.json")
        d = json.loads(obj["Body"].read())
        pathlib.Path("aws/ops/reports/1122_accel_full.json").write_text(
            json.dumps(d, indent=2, default=str))
        if d.get("status") == "error":
            return {"status": "error", "error": d.get("error")}
        return {
            "size_kb":           round(obj["ContentLength"]/1024, 1),
            "elapsed_sec":       d.get("elapsed_sec"),
            "trading_date":      d.get("trading_date"),
            "new_session":       d.get("new_session"),
            "universe_size":     d.get("universe_size"),
            "n_fired":           d.get("n_fired"),
            "n_fresh":           d.get("n_fresh"),
            "n_confirmed_today": d.get("n_confirmed_today"),
            "n_aging":           d.get("n_aging"),
            "n_expired_today":   d.get("n_expired_today"),
            "n_actionable":      d.get("n_actionable"),
            "actionable":        d.get("actionable_tickers", []),
            "by_tier":           d.get("by_tier"),
            "fresh_top_10":      [
                {k: v for k, v in r.items()
                 if k in ("ticker","tier","theme_label","current_score","momentum_score",
                          "n_confirmations","confirmations","accel_components")}
                for r in (d.get("fresh_fires") or [])[:10]
            ],
            "confirmed_today":   [
                {k: v for k, v in r.items()
                 if k in ("ticker","tier","theme_label","current_score","momentum_score","confirmations")}
                for r in (d.get("confirmed_today") or [])
            ],
            "aging":             [
                {k: v for k, v in r.items()
                 if k in ("ticker","tier","theme_label","current_score","sessions_pending")}
                for r in (d.get("aging") or [])[:10]
            ],
            "config":            d.get("config"),
        }
    phase(out, "read_accel", read_accel)

    # Phase 7: re-invoke convergence-radar so it picks up the new engine
    phase(out, "reinvoke_radar", lambda: invoke("justhodl-convergence-radar"))

    # Phase 8: verify radar now includes velocity-acceleration engine
    def verify_radar_integration():
        time.sleep(2)
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/convergence-radar.json")
        d = json.loads(obj["Body"].read())
        # Search for tickers that have velocity-acceleration in their engine list
        with_accel = []
        for c in (d.get("pump_candidates") or []):
            engines = [e.get("engine") for e in (c.get("all_engines") or c.get("bullish_engines") or [])]
            if any("velocity-acceleration" in (e or "") for e in engines):
                with_accel.append(c["ticker"])
        return {
            "n_pump_candidates":   len(d.get("pump_candidates") or []),
            "with_accel_engine":   with_accel,
            "engine_count_in_dict": len(d.get("engine_ages") or {}),
            "has_accel_engine":    "velocity-acceleration" in (d.get("engine_ages") or {}),
        }
    phase(out, "verify_radar_includes_accel", verify_radar_integration)

    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1122] DONE")


if __name__ == "__main__":
    main()
