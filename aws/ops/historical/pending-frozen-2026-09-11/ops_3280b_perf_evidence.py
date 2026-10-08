"""ops 3280b — evidence-driven convergence: tighter ladders (6s),
budgets 120/80, phase timers. Deploy → invoke → poll fresh → read the
function's own '[13f]' phase lines + any 'Task timed out' since the
mark. PASS = fresh feed + benchmarks priced (funds converge next
scheduled runs; anchors_pending reported)."""
import json
import sys
import time
from datetime import datetime, timezone
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


with report("3280b_perf_evidence") as rep:
    fails = []
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

    d = None
    t_mark = int(datetime.now(timezone.utc).timestamp() * 1000)
    if not fails:
        mark = datetime.now(timezone.utc).isoformat()
        LAM.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        for _ in range(66):
            time.sleep(10)
            x = s3_json("data/13f-positions.json") or {}
            if str(x.get("generated_at", "")) > mark:
                d = x
                break

    rep.section("Function's own phase evidence")
    try:
        evs = LOGS.filter_log_events(
            logGroupName=f"/aws/lambda/{FN}", startTime=t_mark,
            filterPattern='?"[13f]" ?"Task timed out" ?"Traceback"',
            limit=40).get("events") or []
        for e in evs[:24]:
            rep.log("  " + e["message"].strip()[:140])
    except Exception as e:
        rep.log(f"  log read: {str(e)[:60]}")

    if not d:
        fails.append("feed not fresh — see phase evidence above")
    else:
        pf = d.get("performance") or {}
        B = pf.get("benchmarks") or {}
        priced = sum(1 for v in (pf.get("by_fund") or {}).values()
                     if v.get("ytd") is not None)
        rep.kv(funds_priced=priced,
               anchors_pending=pf.get("anchors_pending"),
               anchors_fetched=pf.get("anchors_fetched"))
        for b in ("SPY", "IEF", "BTCUSD"):
            v = B.get(b) or {}
            rep.log(f"  BENCH {b:<7} MTD={v.get('mtd')} "
                    f"QTD={v.get('qtd')} YTD={v.get('ytd')}")
        if any(((B.get(b) or {}).get("ytd") is None)
               for b in ("SPY", "IEF", "BTCUSD")):
            fails.append("benchmarks unpriced")
        brk = (pf.get("by_fund") or {}).get("BERKSHIRE") or {}
        if brk.get("ytd") is not None:
            rep.ok(f"BERKSHIRE clone: MTD={brk.get('mtd')} "
                   f"QTD={brk.get('qtd')} YTD={brk.get('ytd')} "
                   f"cov={brk.get('coverage_pct')}%")
    rep.kv(n_fails=len(fails),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
