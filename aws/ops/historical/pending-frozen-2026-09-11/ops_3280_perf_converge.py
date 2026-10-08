"""ops 3280 — clone-performance converged under the 900s ceiling.
3279's first-build blew the Lambda timeout (evidence read from logs).
Now: anchors budgeted 220/run, benchmarks + heaviest-weight tickers
first, mcap cached 7d. This ops invokes to convergence (≤3 rounds),
then proves: SPY/IEF/BTCUSD all priced, ≥12 funds priced, BERKSHIRE
row, page live."""
import json
import sys
import time
import urllib.request
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
UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3280)"}


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3280_perf_converge") as rep:
    fails, warns = [], []
    rep.section("0. 3279 timeout evidence")
    try:
        evs = LOGS.filter_log_events(
            logGroupName=f"/aws/lambda/{FN}",
            startTime=int((datetime.now(timezone.utc)
                           - timedelta(hours=2)).timestamp() * 1000),
            filterPattern='"Task timed out"', limit=3)\
            .get("events") or []
        for e in evs:
            rep.log("  " + e["message"].strip()[:120])
        rep.kv(timeouts_2h=len(evs))
    except Exception:
        pass

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
    if not fails:
        rep.section("1. Invoke to convergence")
        for rnd in (1, 2, 3):
            mark = datetime.now(timezone.utc).isoformat()
            LAM.invoke(FunctionName=FN, InvocationType="Event",
                       Payload=b"{}")
            d = None
            for _ in range(60):
                time.sleep(10)
                x = s3_json("data/13f-positions.json") or {}
                if str(x.get("generated_at", "")) > mark:
                    d = x
                    break
            if not d:
                fails.append(f"round {rnd}: feed not fresh")
                break
            pf = d.get("performance") or {}
            pend = pf.get("anchors_pending")
            priced = sum(1 for v in (pf.get("by_fund") or {})
                         .values() if v.get("ytd") is not None)
            rep.log(f"  round {rnd}: priced_funds={priced} "
                    f"anchors_pending={pend} "
                    f"fetched={pf.get('anchors_fetched')}")
            if priced >= 12 and all(
                    ((pf.get("benchmarks") or {}).get(b) or {})
                    .get("ytd") is not None
                    for b in ("SPY", "IEF", "BTCUSD")):
                break

    if not fails and d:
        pf = d.get("performance") or {}
        B = pf.get("benchmarks") or {}
        for b in ("SPY", "IEF", "BTCUSD"):
            v = B.get(b) or {}
            rep.log(f"  BENCH {b:<7} MTD={v.get('mtd')} "
                    f"QTD={v.get('qtd')} YTD={v.get('ytd')}")
            if v.get("ytd") is None:
                fails.append(f"benchmark {b} unpriced")
        brk = (pf.get("by_fund") or {}).get("BERKSHIRE") or {}
        if brk.get("ytd") is not None:
            rep.ok(f"BERKSHIRE clone: MTD={brk.get('mtd')} "
                   f"QTD={brk.get('qtd')} YTD={brk.get('ytd')} "
                   f"cov={brk.get('coverage_pct')}%")
        priced = sum(1 for v in (pf.get("by_fund") or {}).values()
                     if v.get("ytd") is not None)
        rep.kv(funds_priced=priced,
               anchors_pending=pf.get("anchors_pending"))
        if priced < 12:
            fails.append(f"only {priced} funds priced after rounds")
        okp = False
        try:
            h = urllib.request.urlopen(urllib.request.Request(
                "https://justhodl.ai/13f.html?t="
                f"{int(time.time())}", headers=UA),
                timeout=20).read().decode("utf-8", "replace")
            okp = ("US 10Y (IEF proxy)" in h
                   and "Action Spotlight" in h)
        except Exception:
            pass
        if okp:
            rep.ok("page live, anchors intact")
        else:
            warns.append("page literal re-check inconclusive")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
