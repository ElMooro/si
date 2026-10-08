"""ops 3280 — NET FLOW headline + 13F RISK APPETITE, plus 3279c
self-heal. Section 0: current feed freshness + options census (the v4
re-parse outlived 3279c's window; per-fund caches make re-invoke
fast). Section 1: deploy flow/risk engine, invoke, poll long; print
net$, score, verdict, components, PUT/CALL rows. Section 2: banner
live atop the page, existing anchors intact."""
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
UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3280)"}


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3280_flow_risk") as rep:
    fails, warns = [], []
    rep.section("0. 3279c aftermath — is v4 already on S3?")
    cur = s3_json("data/13f-positions.json") or {}
    agg0 = cur.get("aggregate_by_ticker") or {}
    puts0 = sum(1 for a in agg0.values() if a.get("put_funds"))
    rep.kv(feed_at=str(cur.get("generated_at"))[:19],
           with_puts_now=puts0)

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
        rep.section("1. Flow + risk computed")
        mark = datetime.now(timezone.utc).isoformat()
        LAM.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        d = None
        for _ in range(100):
            time.sleep(10)
            x = s3_json("data/13f-positions.json") or {}
            if str(x.get("generated_at", "")) > mark:
                d = x
                break
        if not d:
            fails.append("feed not fresh in window")
        else:
            f = d.get("flow_summary") or {}
            ra = d.get("risk_appetite") or {}
            agg = d.get("aggregate_by_ticker") or {}
            puts = sorted(((k, a) for k, a in agg.items()
                           if a.get("put_funds")),
                          key=lambda kv: -len(kv[1]["put_funds"]))
            rep.kv(net_usd=f.get("net_usd"),
                   buys=f.get("total_buy_usd"),
                   sells=f.get("total_sell_usd"),
                   funds_buying=f.get("n_funds_net_buying"),
                   funds_selling=f.get("n_funds_net_selling"),
                   risk_score=ra.get("score"),
                   risk_verdict=ra.get("verdict"),
                   put_rows=ra.get("n_put_rows"),
                   call_rows=ra.get("n_call_rows"))
            rep.log(f"  components: {json.dumps(ra.get('components'))}")
            rep.log(f"  smid net ${(ra.get('net_smallmid_usd') or 0)/1e9:.2f}B"
                    f" · large ${(ra.get('net_large_usd') or 0)/1e9:.2f}B"
                    f" · cyc ${(ra.get('net_cyclical_usd') or 0)/1e9:.2f}B"
                    f" · def ${(ra.get('net_defensive_usd') or 0)/1e9:.2f}B")
            for k, a in puts[:3]:
                rep.log(f"  PUT {str(k)[:8]:<8} "
                        f"{str(a.get('name'))[:24]:<24} by "
                        + ", ".join(a["put_funds"][:3]))
            if not f or not ra:
                fails.append("flow/risk fields missing")
            if not puts:
                warns.append("options still zero — inspect one raw "
                             "filing XML next")

        rep.section("2. Banner live")
        okp = False
        for i in range(20):
            try:
                h = urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/13f.html?t="
                    f"{int(time.time())}", headers=UA),
                    timeout=20).read().decode("utf-8", "replace")
                okp = ("NET FLOW THIS QUARTER" in h
                       and "RISK APPETITE" in h
                       and "Action Spotlight" in h)
            except Exception:
                pass
            if okp:
                rep.ok(f"banner atop, anchors intact "
                       f"(~{(i + 1) * 15}s)")
                break
            time.sleep(15)
        if not okp:
            fails.append("banner not live")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
