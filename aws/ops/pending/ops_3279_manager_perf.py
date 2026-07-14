"""ops 3279 — manager clone-performance vs benchmarks, proven.
Each manager card gains MTD/QTD/YTD (value-weighted disclosed longs,
static-since-filing clone, coverage shown) and a BENCHMARKS bar
compares SPY / US-10Y (IEF proxy) / BTCUSD on the same anchors.
Verify: benchmarks non-null, ≥12 funds priced, BERKSHIRE row printed,
page literals live, existing anchors intact."""
import json
import sys
import time
import urllib.request
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
FN = "justhodl-13f-positions"
AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3279)"}


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3279_manager_perf") as rep:
    fails, warns = [], []
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
        rep.section("1. Fresh feed with performance")
        mark = datetime.now(timezone.utc).isoformat()
        LAM.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        d = None
        for _ in range(70):
            time.sleep(10)
            x = s3_json("data/13f-positions.json") or {}
            if str(x.get("generated_at", "")) > mark:
                d = x
                break
        if not d:
            fails.append("feed not fresh in window")
        else:
            pf = d.get("performance") or {}
            B = pf.get("benchmarks") or {}
            bf = pf.get("by_fund") or {}
            priced = [k for k, v in bf.items()
                      if v.get("ytd") is not None]
            rep.kv(anchors=json.dumps(pf.get("anchors")),
                   funds_priced=len(priced),
                   anchor_fetches=pf.get("anchors_fetched"))
            for b in ("SPY", "IEF", "BTCUSD"):
                v = B.get(b) or {}
                rep.log(f"  BENCH {b:<7} MTD={v.get('mtd')} "
                        f"QTD={v.get('qtd')} YTD={v.get('ytd')}")
                if v.get("ytd") is None:
                    fails.append(f"benchmark {b} ytd missing")
            brk = bf.get("BERKSHIRE") or {}
            if brk:
                rep.ok(f"BERKSHIRE clone: MTD={brk.get('mtd')} "
                       f"QTD={brk.get('qtd')} YTD={brk.get('ytd')} "
                       f"cov={brk.get('coverage_pct')}%")
            if len(priced) < 12:
                fails.append(f"only {len(priced)} funds priced")

        rep.section("2. Page live, anchors intact")
        okp = False
        for i in range(22):
            try:
                h = urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/13f.html?t="
                    f"{int(time.time())}", headers=UA),
                    timeout=20).read().decode("utf-8", "replace")
                okp = ("US 10Y (IEF proxy)" in h
                       and "ops 3279" in h
                       and "perfCell" in h
                       and "Each manager" in h
                       and "Action Spotlight" in h)
            except Exception:
                pass
            if okp:
                rep.ok(f"live (~{(i + 1) * 15}s)")
                break
            time.sleep(15)
        if not okp:
            fails.append("page literals not live")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
