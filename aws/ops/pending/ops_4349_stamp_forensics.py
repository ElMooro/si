"""ops_4349 -- stamp forensics: the engine's own self-log testimony
from CloudWatch + an unfiltered post-anchor hunt (any best-setups
item, stamped or not)."""
import json, sys, time
from datetime import datetime, timezone, timedelta
import boto3
from ops_report import report
logs = boto3.client("logs", region_name="us-east-1")
ddb = boto3.resource("dynamodb", region_name="us-east-1")
anchor = (datetime.now(timezone.utc)
          - timedelta(minutes=50)).isoformat()[:19]
with report("4349_stamp_forensics") as r:
    r.heading("ops 4349 -- who swallowed the stamp")
    try:
        ev = logs.filter_log_events(
            logGroupName="/aws/lambda/justhodl-best-setups",
            startTime=int((time.time() - 3600) * 1000),
            filterPattern='"self-log"')
        for e in (ev.get("events") or [])[-6:]:
            r.log("engine: " + e["message"].strip()[:150])
    except Exception as e:
        r.warn("logs: %s" % str(e)[:80])
    tbl = ddb.Table("justhodl-signals")
    kw = {}
    scanned = 0
    any_bs = []
    while scanned < 18000 and len(any_bs) < 5:
        resp = tbl.scan(**kw)
        for it in resp.get("Items", []):
            scanned += 1
            md = it.get("metadata")
            la = str(it.get("logged_at", ""))
            if isinstance(md, dict) \
                    and md.get("engine") == "best-setups" \
                    and la[:19] > anchor:
                any_bs.append(it)
        if "LastEvaluatedKey" not in resp:
            break
        kw["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    r.ok("post-anchor best-setups items (any): %d "
         "(scanned %d, anchor %s)"
         % (len(any_bs), scanned, anchor))
    for it in any_bs[:3]:
        md = it.get("metadata") or {}
        r.log("  %s @ %s md_keys=%s"
              % (it.get("signal_value"),
                 str(it.get("logged_at"))[:19],
                 sorted(md)[:8]))
    r.ok("forensics complete")
    if False:
        sys.exit(1)
