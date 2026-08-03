"""ops_4311 -- the census matrix's TRUE shape, from the artifact
itself, plus the reversal engine's own [universe] log lines and a
duplicate-ticker count. Pure evidence; the one-key fix ships next."""
import json, sys, time
import boto3
from ops_report import report
s3 = boto3.client("s3", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")
with report("4311_census_shape") as r:
    r.heading("ops 4311 -- what the census matrix actually is")
    cm = json.loads(s3.get_object(
        Bucket="justhodl-dashboard-live",
        Key="data/fundamental-census-matrix.json")["Body"].read())
    r.log("top-level type=%s keys=%s"
          % (type(cm).__name__,
             list(cm)[:10] if isinstance(cm, dict) else "n/a"))
    if isinstance(cm, dict):
        for k in list(cm)[:10]:
            v = cm[k]
            r.log("  %s: %s len=%s sample=%s"
                  % (k, type(v).__name__,
                     len(v) if hasattr(v, "__len__") else "-",
                     (json.dumps(v[:1])[:160] if isinstance(v, list)
                      else json.dumps(dict(list(v.items())[:1])
                                      )[:160] if isinstance(v, dict)
                      else str(v)[:80])))
    doc = json.loads(s3.get_object(
        Bucket="justhodl-dashboard-live",
        Key="data/trend-reversal.json")["Body"].read())
    rows = doc.get("rows") or []
    seen, dups = set(), 0
    for x in rows:
        t = x.get("ticker")
        if t in seen:
            dups += 1
        seen.add(t)
    r.log("reversal artifact: %d rows, %d duplicate tickers"
          % (len(rows), dups))
    try:
        ev = logs.filter_log_events(
            logGroupName="/aws/lambda/justhodl-trend-reversal",
            startTime=int((time.time() - 3600) * 1000),
            filterPattern='"universe"')
        for e in (ev.get("events") or [])[-8:]:
            r.log("log: %s" % e["message"].strip()[:150])
    except Exception as e:
        r.warn("logs: %s" % str(e)[:80])
    if not isinstance(cm, dict):
        r.fail("matrix not a dict")
        sys.exit(1)
    r.ok("shape captured")
