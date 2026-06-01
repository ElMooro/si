"""1121 — pump-hunter mode end-to-end chain.

1. Set up momentum-leaders schedule (hourly :25)
2. Invoke momentum-leaders → populates data/momentum-leaders.json
3. Invoke pump-positioning → now reads momentum + outputs BOTH baskets
4. Invoke pump-radar-brief → reads everything + produces aggressive-mode brief
5. Verify each layer + extract key samples
"""
import json, pathlib, time, traceback
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1121_pump_hunter_chain.json"
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
            except Exception: return {"elapsed_s": elapsed, "body": p["body"][:400]}
        return {"elapsed_s": elapsed, "p": str(p)[:400]}
    except Exception: return {"elapsed_s": elapsed, "raw": body[:600]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "phases": []}

    # Phase 1: confirm new Lambda exists
    def check_momentum():
        d = lam.get_function(FunctionName="justhodl-momentum-leaders")["Configuration"]
        return {"state": d["State"], "code_kb": round(d["CodeSize"]/1024, 1),
                "memory": d["MemorySize"], "timeout": d["Timeout"]}
    phase(out, "check_momentum_lambda", check_momentum)

    # Phase 2: setup schedule
    phase(out, "setup_momentum_schedule",
            lambda: setup_schedule("justhodl-momentum-leaders"))

    # Phase 3: invoke momentum-leaders FIRST (positioning + brief depend on it)
    phase(out, "invoke_momentum_leaders",
            lambda: invoke("justhodl-momentum-leaders"))

    # Phase 4: read momentum output + show top leaders
    def read_momentum():
        time.sleep(2)
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/momentum-leaders.json")
        d = json.loads(obj["Body"].read())
        pathlib.Path("aws/ops/reports/1121_momentum_full.json").write_text(
            json.dumps(d, indent=2, default=str))
        if d.get("status") == "error":
            return {"status": "error", "error": d.get("error")}
        leaders = d.get("leaders", [])
        pump_conf = d.get("pump_confirmed", [])
        return {
            "size_kb":           round(obj["ContentLength"]/1024, 1),
            "elapsed_sec":       d.get("elapsed_sec"),
            "n_scored":          d.get("n_scored"),
            "n_leaders":         d.get("n_leaders"),
            "n_pump_confirmed":  d.get("n_pump_confirmed"),
            "universe_sources":  (d.get("metadata") or {}).get("universe_sources"),
            "spy_perf_20d":      (d.get("metadata") or {}).get("spy_perf_20d"),
            "top_10_leaders": [
                {
                    "ticker":         l["ticker"],
                    "rank":           l["rank"],
                    "momentum_score": l["momentum_score"],
                    "perf_5d":        l.get("perf_5d_pct"),
                    "perf_20d":       l.get("perf_20d_pct"),
                    "perf_60d":       l.get("perf_60d_pct"),
                    "rs_spy_20d":     l.get("rs_spy_20d_pct"),
                    "wk52_prox":      l.get("wk52_proximity"),
                    "vol_surge":      l.get("volume_surge"),
                    "tags":           l.get("tags", []),
                    "in_pump_cands":  l.get("in_pump_candidates"),
                    "n_engines":      l.get("n_engines"),
                    "pump_likelihood": l.get("pump_likelihood"),
                }
                for l in leaders[:10]
            ],
            "pump_confirmed": [
                {
                    "ticker":         p["ticker"],
                    "momentum_score": p.get("momentum_score"),
                    "pump_likelihood": p.get("pump_likelihood"),
                    "n_engines":      p.get("n_engines"),
                    "tags":           p.get("tags", []),
                }
                for p in pump_conf
            ],
        }
    phase(out, "read_momentum_leaders", read_momentum)

    # Phase 5: re-invoke positioning (now reads momentum, builds aggressive basket)
    phase(out, "reinvoke_positioning",
            lambda: invoke("justhodl-pump-positioning"))

    # Phase 6: read positioning + show BOTH baskets
    def read_positioning():
        time.sleep(2)
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/pump-positioning.json")
        d = json.loads(obj["Body"].read())
        pathlib.Path("aws/ops/reports/1121_positioning_full.json").write_text(
            json.dumps(d, indent=2, default=str))
        conservative = d.get("portfolio_basket") or {}
        aggressive = d.get("aggressive_basket") or {}
        return {
            "size_kb":         round(obj["ContentLength"]/1024, 1),
            "conservative": {
                "n_positions":    conservative.get("n_positions"),
                "total_exposure": conservative.get("total_exposure"),
                "cash_pct":       conservative.get("cash_pct"),
                "top_3":          [(p.get("ticker"), p.get("position_pct")) for p in (conservative.get("positions") or [])[:3]],
            },
            "aggressive": {
                "n_positions":    aggressive.get("n_positions"),
                "total_exposure": aggressive.get("total_exposure"),
                "cash_pct":       aggressive.get("cash_pct"),
                "max_risk_at_stops_pct": aggressive.get("max_risk_at_stops_pct"),
                "sector_breakdown": aggressive.get("sector_breakdown"),
                "positions":      aggressive.get("positions", []),
            },
        }
    phase(out, "read_positioning_with_agg", read_positioning)

    # Phase 7: re-invoke brief (now sees momentum + aggressive basket + pump-hunter prompt)
    phase(out, "reinvoke_brief", lambda: invoke("justhodl-pump-radar-brief"))

    # Phase 8: read updated brief
    def read_brief():
        time.sleep(2)
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/pump-radar-brief.json")
        d = json.loads(obj["Body"].read())
        pathlib.Path("aws/ops/reports/1121_brief_full.json").write_text(
            json.dumps(d, indent=2, default=str))
        if d.get("status") == "error":
            return {"status": "error", "error": d.get("error")}
        return {
            "size_kb":             round(obj["ContentLength"]/1024, 1),
            "claude_elapsed":      d.get("claude_elapsed"),
            "conviction_grade":    d.get("conviction_grade"),
            "executive_summary":   d.get("executive_summary"),
            "macro_frame":         d.get("macro_frame"),
            "market_temperature":  d.get("market_temperature"),
            "top_3_long_ideas":    d.get("top_3_long_ideas", []),
            "top_2_pair_trades":   d.get("top_2_pair_trades", []),
            "what_to_watch_today": d.get("what_to_watch_today", []),
            "risk_warnings":       d.get("risk_warnings", []),
            "whats_changed":       d.get("whats_changed_narrative"),
        }
    phase(out, "read_brief_aggressive", read_brief)

    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1121] DONE")


if __name__ == "__main__":
    main()
