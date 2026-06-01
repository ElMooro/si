"""1118 — schedule + invoke + verify all 4 new hedge-fund Lambdas."""
import json, pathlib, time, traceback
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1118_hedgefund_v2.json"
lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=350))
s3  = boto3.client("s3", region_name="us-east-1")
ebs = boto3.client("scheduler", region_name="us-east-1")

LAMBDAS = [
    "justhodl-pump-mechanics",
    "justhodl-portfolio-analytics",
    "justhodl-pair-trades",
    "justhodl-pump-earnings-nlp",
]
OUTPUT_KEYS = {
    "justhodl-pump-mechanics":      "data/pump-mechanics.json",
    "justhodl-portfolio-analytics": "data/portfolio-analytics.json",
    "justhodl-pair-trades":         "data/pair-trades.json",
    "justhodl-pump-earnings-nlp":   "data/pump-earnings-nlp.json",
}


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
        Target={
            "Arn":     f"arn:aws:lambda:us-east-1:857687956942:function:{fn_name}",
            "RoleArn": cfg["role_arn"],
            "Input":   json.dumps({"source": "scheduler"}),
        },
    )
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


def read_output(fn_name: str) -> dict:
    key = OUTPUT_KEYS[fn_name]
    time.sleep(2)
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
    d = json.loads(obj["Body"].read())

    base = {
        "key":           key,
        "size_kb":       round(obj["ContentLength"]/1024, 1),
        "last_modified": obj["LastModified"].isoformat(),
        "schema":        d.get("schema_version"),
        "elapsed_sec":   d.get("elapsed_sec"),
    }
    if d.get("status") == "error":
        base["status"] = "error"
        base["error"]  = d.get("error")
        base["preview"] = (d.get("raw_preview") or "")[:300]
        return base

    # Save full for offline review
    pathlib.Path(f"aws/ops/reports/1118_{fn_name.replace('justhodl-', '')}_full.json").write_text(
        json.dumps(d, indent=2, default=str))

    # Lambda-specific sample extraction
    if fn_name == "justhodl-pump-mechanics":
        cands = d.get("candidates", [])
        base["n_candidates"] = d.get("n_candidates")
        base["samples"] = []
        for c in cands[:5]:
            sq = c.get("squeeze_profile") or {}
            opt = c.get("options_structure") or {}
            base["samples"].append({
                "ticker":          c["ticker"],
                "pump_likelihood": c.get("pump_likelihood"),
                "float_tier":      sq.get("float_tier"),
                "shares_out":      sq.get("shares_outstanding"),
                "rotation_today":  sq.get("rotation_today"),
                "rotation_accel":  sq.get("rotation_accel"),
                "atr_pct":         sq.get("atr_pct"),
                "squeeze_score":   sq.get("squeeze_proxy_score"),
                "squeeze_tier":    sq.get("squeeze_potential"),
                "concentration":   sq.get("concentration_signals", [])[:2],
                "options_skew":    opt.get("skew") if opt.get("available") else None,
                "options_tier":    opt.get("tier") if opt.get("available") else None,
                "iv_rank":         opt.get("iv_rank_proxy") if opt.get("available") else None,
                "term_struct":     opt.get("term_structure") if opt.get("available") else None,
                "rv_5d":           opt.get("realized_vol_5d") if opt.get("available") else None,
                "rv_30d":          opt.get("realized_vol_30d") if opt.get("available") else None,
            })

    elif fn_name == "justhodl-portfolio-analytics":
        corr = d.get("correlations", {})
        fact = d.get("factor_exposure", {})
        base["n_candidates"] = d.get("n_candidates")
        base["clusters"]     = (corr.get("clusters") or [])[:8]
        base["most_divers"]  = corr.get("most_diversifying_top_5", [])
        base["most_correl"]  = corr.get("most_correlated_top_5", [])
        # Sample 3 tickers' factor exposure
        per = fact.get("per_ticker") or {}
        base["factor_sample"] = {}
        for t in list(per.keys())[:5]:
            base["factor_sample"][t] = per[t]
        # Sample correlation row for top 1
        if corr.get("matrix"):
            first_t = list(corr["matrix"].keys())[0]
            row = corr["matrix"][first_t]
            # Top 5 correlated to this ticker
            top = sorted(((k, v) for k, v in row.items() if k != first_t and v is not None),
                            key=lambda x: -x[1])[:5]
            base["sample_correlation_row"] = {"ticker": first_t, "top_5_with_corr": top}

    elif fn_name == "justhodl-pair-trades":
        pairs = d.get("pairs", [])
        base["n_pairs"] = d.get("n_pairs")
        base["samples"] = []
        for p in pairs[:5]:
            base["samples"].append({
                "long":              p.get("long_ticker"),
                "long_perf_20d":     p.get("long_perf_20d"),
                "long_pump":         p.get("long_pump_likelihood"),
                "short":             p.get("short_ticker"),
                "short_company":     p.get("short_company", "")[:32],
                "short_perf_20d":    p.get("short_perf_20d"),
                "spread_20d":        p.get("spread_20d_pct"),
                "correlation_90d":   p.get("correlation_90d"),
                "hedge_quality":     p.get("hedge_quality"),
                "ratio":             p.get("ratio_long_short"),
                "expected_alpha_1m": p.get("expected_alpha_1m"),
                "thesis":            (p.get("thesis_one_liner") or "")[:200],
            })

    elif fn_name == "justhodl-pump-earnings-nlp":
        research = d.get("research", {})
        base["n_tickers"] = d.get("n_tickers")
        base["claude_elapsed"] = d.get("claude_elapsed")
        base["samples"] = []
        for t in list(research.keys())[:3]:
            r = research[t]
            base["samples"].append({
                "ticker":            t,
                "transcripts":       [f"{x['period']} {x['year']}" for x in r.get("transcripts_used", [])],
                "tone_trajectory":   r.get("tone_trajectory"),
                "tone_delta":        r.get("tone_delta"),
                "tone_per_quarter":  r.get("tone_per_quarter"),
                "emerging_themes":   r.get("emerging_themes", [])[:3],
                "fading_themes":     r.get("fading_themes", [])[:2],
                "cautionary":        r.get("cautionary_signals", [])[:3],
                "growth_freq":       r.get("growth_language_freq"),
                "guidance":          r.get("forward_guidance_posture"),
                "qa_pressure":       r.get("qa_pressure_topics", [])[:3],
                "key_quotes":        r.get("key_quotes", [])[:2],
                "pump_implication":  (r.get("pump_implication") or "")[:200],
                "ai_synthesis":      r.get("ai_synthesis"),
            })

    return base


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "phases": []}

    # Phase 1: confirm all 4 Lambdas exist + check key env
    def check_all():
        info = {}
        for fn_name in LAMBDAS:
            try:
                d = lam.get_function(FunctionName=fn_name)["Configuration"]
                env = (d.get("Environment", {}) or {}).get("Variables", {})
                info[fn_name] = {
                    "state":        d["State"],
                    "code_size_kb": round(d["CodeSize"]/1024, 1),
                    "memory":       d["MemorySize"],
                    "timeout":      d["Timeout"],
                    "has_fmp_key":  bool(env.get("FMP_KEY")),
                    "has_anth_key": bool(env.get("ANTHROPIC_API_KEY")),
                }
            except Exception as e:
                info[fn_name] = {"err": str(e)[:200]}
        return info
    phase(out, "check_lambdas", check_all)

    # Phase 2: set up all 4 schedules
    def setup_all_schedules():
        res = {}
        for fn_name in LAMBDAS:
            try:
                res[fn_name] = setup_schedule(fn_name)
            except Exception as e:
                res[fn_name] = {"err": str(e)[:200]}
        return res
    phase(out, "setup_schedules", setup_all_schedules)

    # Phase 3: invoke each Lambda (sequentially — pump-earnings-nlp needs Claude)
    for fn_name in LAMBDAS:
        phase(out, f"invoke_{fn_name.replace('justhodl-', '')}",
                lambda fn=fn_name: invoke(fn))

    # Phase 4: read each output
    for fn_name in LAMBDAS:
        phase(out, f"read_{fn_name.replace('justhodl-', '')}",
                lambda fn=fn_name: read_output(fn))

    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1118] DONE")


if __name__ == "__main__":
    main()
