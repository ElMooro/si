"""ops 3283 — hang-proof numbers pass. No deploy (sec-fixed code is
live), no fancy log OR-patterns. Invoke → poll 60×10s → print the
quarter's flow/risk + per-fund table; on staleness, two SIMPLE log
reads (ERROR, then Task timed) print the engine's own words."""
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
LOGS = boto3.client("logs", region_name=REGION)
FN = "justhodl-13f-positions"


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3283_direct_numbers") as rep:
    fails = []
    mark = datetime.now(timezone.utc).isoformat()
    LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    d = None
    for _ in range(60):
        time.sleep(10)
        x = s3_json("data/13f-positions.json") or {}
        if str(x.get("generated_at", "")) > mark:
            d = x
            break
    if not d:
        rep.section("engine's own words (last 40 min)")
        start = int((datetime.now(timezone.utc)
                     - timedelta(minutes=40)).timestamp() * 1000)
        for pat in ("ERROR", "Task timed", "Traceback",
                    "[13f]"):
            try:
                evs = LOGS.filter_log_events(
                    logGroupName=f"/aws/lambda/{FN}",
                    startTime=start, filterPattern=f'"{pat}"',
                    limit=8).get("events") or []
                for e in evs[-6:]:
                    rep.log("  " + e["message"].strip()[:160])
            except Exception as e2:
                rep.log(f"  log {pat}: {str(e2)[:50]}")
        fails.append("feed not fresh — engine words above")
    else:
        f = d.get("flow_summary") or {}
        ra = d.get("risk_appetite") or {}
        rep.kv(net_usd=f.get("net_usd"),
               buys=f.get("total_buy_usd"),
               sells=f.get("total_sell_usd"),
               funds_buying=f.get("n_funds_net_buying"),
               funds_selling=f.get("n_funds_net_selling"),
               risk_score=ra.get("score"),
               risk_verdict=ra.get("verdict"),
               put_rows=ra.get("n_put_rows"),
               call_rows=ra.get("n_call_rows"))
        rep.log("  components: "
                + json.dumps(ra.get("components")))
        rep.log("  buyers: " + ", ".join(
            f"{k} ${v/1e9:+.1f}B"
            for k, v in (f.get("top_net_buyers") or [])))
        rep.log("  sellers: " + ", ".join(
            f"{k} ${v/1e9:+.1f}B"
            for k, v in (f.get("top_net_sellers") or [])))
        bf = d.get("by_fund") or {}
        rows = [(v.get("fund_name") or k,
                 (v.get("flow") or {}).get("net_usd") or 0,
                 (v.get("risk") or {}).get("verdict"),
                 (v.get("risk") or {}).get("score"))
                for k, v in bf.items() if v.get("flow")]
        rep.kv(funds_with_flow=len(rows))
        for nm, net, vd, sc in sorted(rows, key=lambda r: -r[1]):
            rep.log(f"  {str(nm)[:26]:<26} ${net/1e9:+6.2f}B  "
                    f"{vd} ({sc:+.0f})")
        if not f or len(rows) < 10:
            fails.append("fields incomplete")
    rep.kv(verdict="PASS" if not fails else "FAIL")
    if fails:
        for x in fails:
            rep.fail(x)
        sys.exit(1)
