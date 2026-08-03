"""ops_4318 -- why.html exponential: resilient fills (verdict pill now
survives async composer ordering), institution counts rendered as the
tracked-top-movers they are (with the 'selling 0' evidence printed),
and the Fleet Decision Board joining best-setups+backtest+industry-
boom+short-interest+congress per ticker."""
import json, sys, time, urllib.request
import boto3
from ops_report import report
s3 = boto3.client("s3", region_name="us-east-1")
fails = []
with report("4318_why_board") as r:
    r.heading("ops 4318 -- research page, fleet-grade")
    tf = json.loads(s3.get_object(
        Bucket="justhodl-dashboard-live",
        Key="data/13f-flows-by-ticker.json")["Body"].read())
    f = (tf.get("t") or {}).get("INTC") or {}
    r.log("13f INTC entry: $buy=%s $sell=%s top_buyers=%s "
          "top_sellers=%s funds=%s"
          % (f.get("b"), f.get("s"), f.get("fb"), f.get("fs"),
             f.get("nf")))
    r.ok("EVIDENCE: 'selling 0' was len(fs) of a TOP-MOVERS list, "
         "not a census -- $-selling field s=%s tells the truth"
         % f.get("s"))
    cg = json.loads(s3.get_object(
        Bucket="justhodl-dashboard-live",
        Key="data/congress-direct.json")["Body"].read())
    def find_rows(o, d=0):
        if d > 3 or o is None:
            return None
        if isinstance(o, list) and o and isinstance(o[0], dict) \
                and ("ticker" in o[0] or "symbol" in o[0]):
            return o
        if isinstance(o, dict):
            for v in o.values():
                r2 = find_rows(v, d + 1)
                if r2:
                    return r2
        return None
    rows = find_rows(cg) or []
    r.log("congress rows scannable: %d (sample keys %s)"
          % (len(rows), list(rows[0])[:7] if rows else None))
    if not rows:
        fails.append("congress rows not scannable")
    body = ""
    for _ in range(13):
        try:
            body = urllib.request.urlopen(urllib.request.Request(
                "https://justhodl.ai/why.html",
                headers={"User-Agent": "ops/4318",
                         "Cache-Control": "no-cache"}),
                timeout=25).read().decode("utf-8", "ignore")
            if "jh-fleetboard" in body:
                break
        except Exception:
            pass
        time.sleep(20)
    for mk in ("fillWhenReady", "fixInstCounts", "jh-fleetboard",
               "Fleet Decision Board", "tracked top movers",
               "verdict hist win", "CONFLICT"):
        if mk not in body:
            fails.append("edge missing %s" % mk)
    if "jh-fleetboard" in body:
        r.ok("page LIVE (%d bytes) -- all board markers" % len(body))
    if fails:
        for f2 in fails:
            r.fail("  %s" % f2)
        sys.exit(1)
    r.ok("OPS 4318 PASS -- one page, every engine's honest read")
