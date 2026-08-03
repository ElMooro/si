"""ops_4317 -- the direction verdict inside Momentum & Trend itself:
hook div in the template, filled by the existing radar join with
TOP/BOTTOM FORMING (stage, score, weekly) when >=15, or an explicit
'trend intact' / 'outside universe'. Gate: edge markers + a live
radar row that would light it (any CONFIRMED direction >=15)."""
import json, sys, time, urllib.request
import boto3
from ops_report import report
s3 = boto3.client("s3", region_name="us-east-1")
fails = []
with report("4317_momo_verdict") as r:
    r.heading("ops 4317 -- momentum block speaks direction")
    d = json.loads(s3.get_object(
        Bucket="justhodl-dashboard-live",
        Key="data/trend-reversal.json")["Body"].read())
    lit = [x for x in d.get("rows") or []
           if x.get("direction") and (x.get("reversal_score") or 0)
           >= 15][:5]
    r.log("names that light the verdict today: %s"
          % [(x["ticker"], x["direction"], x["reversal_score"])
             for x in lit])
    if not lit:
        fails.append("no qualifying rows in radar (implausible)")
    body = ""
    for _ in range(13):
        try:
            body = urllib.request.urlopen(urllib.request.Request(
                "https://justhodl.ai/why.html",
                headers={"User-Agent": "ops/4317",
                         "Cache-Control": "no-cache"}),
                timeout=25).read().decode("utf-8", "ignore")
            if "jh-momo-reversal" in body:
                break
        except Exception:
            pass
        time.sleep(20)
    for mk in ("jh-momo-reversal", "BOTTOM FORMING",
               "trend intact per the", "outside universe"):
        if mk not in body:
            fails.append("edge missing %s" % mk)
    if "jh-momo-reversal" in body:
        r.ok("hook + fill logic LIVE (%d bytes)" % len(body))
    if fails:
        for f in fails:
            r.fail("  %s" % f)
        sys.exit(1)
    r.ok("OPS 4317 PASS -- the trend read now states its direction")
