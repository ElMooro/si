"""1116 — deploy schedules + invoke pump-positioning + radar-backtest.

Phase 1: Set up EventBridge schedules for both
Phase 2: Invoke pump-positioning (synchronous, see basket + first ticker framework)
Phase 3: Invoke radar-backtest (synchronous, see hit rates per tier)
Phase 4: Read outputs + report sample data
"""
import json, pathlib, time, traceback
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1116_hedgefund_layer.json"
lam = boto3.client("lambda", region_name="us-east-1",
                    config=Config(read_timeout=350))
s3 = boto3.client("s3", region_name="us-east-1")
ebs = boto3.client("scheduler", region_name="us-east-1")


def phase(out, name, fn):
    try:
        r = fn()
        out["phases"].append({"name": name, "status": "ok", "result": r})
        return r
    except Exception as e:
        out["phases"].append({
            "name": name, "status": "ERROR",
            "error": str(e)[:400],
            "traceback": traceback.format_exc()[:1500],
        })
        return None


def setup_schedule(fn_name: str) -> dict:
    """Set or replace EventBridge schedule from the Lambda's config.json."""
    cfg = json.load(open(f"aws/lambdas/{fn_name}/config.json"))["eventbridge_scheduler"]
    name = cfg["schedule_name"]
    try:
        ebs.delete_schedule(Name=name)
        time.sleep(1)
    except ebs.exceptions.ResourceNotFoundException:
        pass
    ebs.create_schedule(
        Name=name,
        ScheduleExpression=cfg["cron"],
        ScheduleExpressionTimezone=cfg["timezone"],
        Description=cfg.get("description", ""),
        State="ENABLED",
        FlexibleTimeWindow={"Mode": "OFF"},
        Target={
            "Arn":     f"arn:aws:lambda:us-east-1:857687956942:function:{fn_name}",
            "RoleArn": cfg["role_arn"],
            "Input":   json.dumps({"source": "scheduler"}),
        },
    )
    return {"schedule": name, "cron": cfg["cron"]}


def invoke_and_parse(fn_name: str) -> dict:
    t0 = time.time()
    r = lam.invoke(FunctionName=fn_name,
                    InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    body = r["Payload"].read().decode("utf-8", errors="replace")
    try:
        p = json.loads(body)
        if isinstance(p.get("body"), str):
            try:
                return {"elapsed_s": elapsed, "summary": json.loads(p["body"])}
            except Exception:
                return {"elapsed_s": elapsed, "body": p["body"][:400]}
        return {"elapsed_s": elapsed, "p": str(p)[:400]}
    except Exception:
        return {"elapsed_s": elapsed, "raw": body[:600]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "phases": []}

    # Phase 1: confirm both Lambdas are deployed
    def check_both():
        info = {}
        for fn_name in ("justhodl-pump-positioning", "justhodl-radar-backtest"):
            try:
                d = lam.get_function(FunctionName=fn_name)["Configuration"]
                info[fn_name] = {
                    "state":         d["State"],
                    "code_size_kb":  round(d["CodeSize"]/1024, 1),
                    "memory":        d["MemorySize"],
                    "timeout":       d["Timeout"],
                    "has_fmp_key":   bool((d.get("Environment", {}) or {}).get("Variables", {}).get("FMP_KEY")),
                }
            except Exception as e:
                info[fn_name] = {"err": str(e)[:200]}
        return info
    state = phase(out, "check_both_lambdas", check_both)
    if not state:
        pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
        return

    # Phase 2: set up both schedules
    def setup_both_schedules():
        results = {}
        for fn_name in ("justhodl-pump-positioning", "justhodl-radar-backtest"):
            try:
                results[fn_name] = setup_schedule(fn_name)
            except Exception as e:
                results[fn_name] = {"err": str(e)[:200]}
        return results
    phase(out, "setup_schedules", setup_both_schedules)

    # Phase 3: invoke pump-positioning (depends on convergence-radar being current)
    phase(out, "invoke_pump_positioning",
            lambda: invoke_and_parse("justhodl-pump-positioning"))

    # Phase 4: read pump-positioning output
    def read_positioning():
        time.sleep(2)
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="data/pump-positioning.json")
        d = json.loads(obj["Body"].read())
        if d.get("status") == "error":
            return {"status": "error", "error": d.get("error"), "size_kb": round(obj["ContentLength"]/1024, 1)}

        # Save full for offline review
        pathlib.Path("aws/ops/reports/1116_positioning_full.json").write_text(
            json.dumps(d, indent=2, default=str))

        # Summary slice
        cands = d.get("candidates", [])
        sample = []
        for c in cands[:5]:
            fw = c.get("trade_framework", {}) or {}
            ctx = c.get("context", {}) or {}
            pd = c.get("price_data", {}) or {}
            sample.append({
                "ticker":             c.get("ticker"),
                "pump_likelihood":    c.get("pump_likelihood"),
                "category":           c.get("pump_category"),
                "current_price":      pd.get("current"),
                "atr_14":             pd.get("atr_14"),
                "hv_30":              pd.get("hv_30"),
                "perf_5d_pct":        pd.get("perf_5d_pct"),
                "perf_20d_pct":       pd.get("perf_20d_pct"),
                "adv_dollars":        pd.get("adv_dollars"),
                "beta":               pd.get("beta_spy"),
                "stop_loss":          fw.get("stop_loss"),
                "stop_pct":           fw.get("stop_loss_pct"),
                "tp1":                (fw.get("tp_ladder") or [{}])[0].get("price"),
                "tp2":                (fw.get("tp_ladder") or [{}, {}])[1].get("price") if len(fw.get("tp_ladder") or []) > 1 else None,
                "tp3":                (fw.get("tp_ladder") or [{}, {}, {}])[2].get("price") if len(fw.get("tp_ladder") or []) > 2 else None,
                "position_size_pct":  fw.get("position_size_pct"),
                "kelly_fraction":     fw.get("kelly_fraction"),
                "rr_ratio":           fw.get("rr_ratio"),
                "sector":             ctx.get("sector"),
                "industry":           ctx.get("industry"),
                "days_to_earnings":   ctx.get("days_to_earnings"),
                "liquidity_tier":     ctx.get("liquidity_tier"),
                "warnings":           c.get("warnings", [])[:4],
            })
        basket = d.get("portfolio_basket", {})
        return {
            "size_kb":           round(obj["ContentLength"]/1024, 1),
            "elapsed_sec":       d.get("elapsed_sec"),
            "n_candidates":      d.get("n_candidates"),
            "macro_regime":      d.get("macro_regime"),
            "basket_positions":  basket.get("n_positions"),
            "total_exposure":    basket.get("total_exposure"),
            "cash_pct":          basket.get("cash_pct"),
            "sector_breakdown":  basket.get("sector_breakdown"),
            "basket_top_5":      basket.get("positions", [])[:5],
            "sample_candidates": sample,
        }
    phase(out, "read_positioning_output", read_positioning)

    # Phase 5: invoke radar-backtest (may take longer due to many FMP price fetches)
    phase(out, "invoke_radar_backtest",
            lambda: invoke_and_parse("justhodl-radar-backtest"))

    # Phase 6: read backtest output
    def read_backtest():
        time.sleep(2)
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="data/radar-backtest.json")
        d = json.loads(obj["Body"].read())
        if d.get("status") == "error":
            return {"status": "error", "error": d.get("error"), "size_kb": round(obj["ContentLength"]/1024, 1)}

        pathlib.Path("aws/ops/reports/1116_backtest_full.json").write_text(
            json.dumps(d, indent=2, default=str))

        return {
            "size_kb":         round(obj["ContentLength"]/1024, 1),
            "elapsed_sec":     d.get("elapsed_sec"),
            "lookback_days":   d.get("lookback_days"),
            "n_snapshots":     d.get("n_snapshots"),
            "n_unique_signals": d.get("n_unique_signals"),
            "n_with_returns": d.get("n_with_returns"),
            "n_unique_tickers": d.get("n_unique_tickers"),
            "overall_5d":     d.get("overall_5d"),
            "per_tier_5d":    d.get("per_tier_5d"),
            "transitions":    d.get("transitions"),
            "pump_categories": d.get("pump_categories"),
            "best_signals_top_5":  (d.get("best_signals") or [])[:5],
            "worst_signals_top_5": (d.get("worst_signals") or [])[:5],
        }
    phase(out, "read_backtest_output", read_backtest)

    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1116] DONE")


if __name__ == "__main__":
    main()
