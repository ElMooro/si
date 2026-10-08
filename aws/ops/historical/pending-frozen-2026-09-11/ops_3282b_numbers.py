"""ops 3282b — why two invokes wrote nothing, then the fix proven.
Section 0 reads the engine's CloudWatch logs (timeout vs traceback —
the definitive cause). Fix shipped regardless: enrichment results
persist in the cusip-map doc (7d TTL) and a remaining-time budget
guard stops fresh lookups at T-150s so the S3 write ALWAYS lands
(partial cache resumes next run). Deploy → invoke → the quarter's
numbers finally print: global net$/risk + per-fund table."""
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
LOGS = boto3.client("logs", region_name=REGION)
FN = "justhodl-13f-positions"
AWS_DIR = Path(__file__).resolve().parents[2]


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3282b_numbers") as rep:
    fails, warns = [], []
    rep.section("0. Why the last two invokes wrote nothing")
    try:
        start = int((datetime.now(timezone.utc)
                     - timedelta(hours=3)).timestamp() * 1000)
        evs = LOGS.filter_log_events(
            logGroupName=f"/aws/lambda/{FN}", startTime=start,
            filterPattern='?"Task timed out" ?"[13f]" ?"ERROR" '
                          '?"Traceback"',
            limit=30).get("events") or []
        seen = []
        for e in evs[-14:]:
            m = e["message"].strip()[:150]
            if m not in seen:
                seen.append(m)
                rep.log("  " + m)
        if any("Task timed out" in m for m in seen):
            rep.ok("cause: Lambda TIMEOUT (enrichment loop) — fix "
                   "below")
    except Exception as e:
        warns.append(f"log read: {str(e)[:60]}")

    live_cfg = LAM.get_function_configuration(FunctionName=FN)
    env = (live_cfg.get("Environment") or {}).get("Variables") or {}
    try:
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=AWS_DIR / "lambdas" / FN / "source",
                      env_vars=env, eb_rule_name=None, eb_schedule=None,
                      timeout=900,
                      memory=int(live_cfg.get("MemorySize") or 1536),
                      description=str(live_cfg.get("Description")
                                      or "")[:250], smoke=False)
        LAM.get_waiter("function_updated_v2").wait(
            FunctionName=FN, WaiterConfig={"Delay": 2,
                                           "MaxAttempts": 40})
    except Exception as e:
        fails.append(f"deploy: {str(e)[:80]}")

    if not fails:
        rep.section("1. THE QUARTER'S NUMBERS")
        mark = datetime.now(timezone.utc).isoformat()
        LAM.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        d = None
        for _ in range(90):
            time.sleep(10)
            x = s3_json("data/13f-positions.json") or {}
            if str(x.get("generated_at", "")) > mark:
                d = x
                break
        if not d:
            fails.append("feed STILL not fresh — read logs above")
        else:
            f = d.get("flow_summary") or {}
            ra = d.get("risk_appetite") or {}
            rep.kv(net_usd=f.get("net_usd"),
                   buys_usd=f.get("total_buy_usd"),
                   sells_usd=f.get("total_sell_usd"),
                   funds_buying=f.get("n_funds_net_buying"),
                   funds_selling=f.get("n_funds_net_selling"),
                   risk_score=ra.get("score"),
                   risk_verdict=ra.get("verdict"),
                   put_rows=ra.get("n_put_rows"),
                   call_rows=ra.get("n_call_rows"),
                   mcap_enriched=d.get("mcap_enriched"))
            rep.log(f"  components: "
                    f"{json.dumps(ra.get('components'))}")
            rep.log("  top buyers: " + ", ".join(
                f"{k} ${v/1e9:+.1f}B"
                for k, v in (f.get("top_net_buyers") or [])))
            rep.log("  top sellers: " + ", ".join(
                f"{k} ${v/1e9:+.1f}B"
                for k, v in (f.get("top_net_sellers") or [])))
            bf = d.get("by_fund") or {}
            rows = [(v.get("fund_name") or k,
                     (v.get("flow") or {}).get("net_usd") or 0,
                     (v.get("risk") or {}).get("verdict"),
                     (v.get("risk") or {}).get("score"))
                    for k, v in bf.items() if v.get("flow")]
            rep.kv(funds_with_flow=len(rows))
            for nm, net, vd, sc in sorted(rows,
                                          key=lambda r: -r[1]):
                rep.log(f"  {str(nm)[:26]:<26} net "
                        f"${net/1e9:+6.2f}B · {vd} ({sc:+.0f})")
            if not f or len(rows) < 10:
                fails.append("flow/per-fund fields incomplete")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
