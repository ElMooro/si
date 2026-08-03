"""ops_4314 -- visual layer rebuilt product-grade: correct Bruls
squarified treemap (sector header bands, ghost +N tiles, perceptual
ramps, mode toggle, label halos), net-turn dial replacing the dueling
bars, gridded/area breadth history with value badges, filled
sparklines. Page-only push; gate = edge markers + artifact sanity."""
import json, sys, time, urllib.request
import boto3
from ops_report import report
s3 = boto3.client("s3", region_name="us-east-1")
fails = []
with report("4314_visuals") as r:
    r.heading("ops 4314 -- visuals, exponentially")
    doc = json.loads(s3.get_object(
        Bucket="justhodl-dashboard-live",
        Key="data/trend-reversal.json")["Body"].read())
    r.log("artifact: %s rows · v%s" % (doc.get("universe_n"),
                                       doc.get("version")))
    body = ""
    for _ in range(14):
        try:
            body = urllib.request.urlopen(urllib.request.Request(
                "https://justhodl.ai/trend-reversal.html",
                headers={"User-Agent": "ops/4314",
                         "Cache-Control": "no-cache"}),
                timeout=25).read().decode("utf-8", "ignore")
            if "net turn (bottoms" in body:
                break
        except Exception:
            pass
        time.sleep(20)
    marks = ["Bruls", "net turn (bottoms", "data-hm", "heatColor",
             "ghost", "breadth history — daily %", "paint-order",
             'id="ghist"', 'id="hmap"']
    miss = [m for m in marks if m not in body]
    if miss:
        fails.append("edge missing %s" % miss)
    else:
        r.ok("page v5 LIVE (%d bytes) -- all visual markers"
             % len(body))
    if fails:
        for f in fails:
            r.fail("  %s" % f)
        sys.exit(1)
    r.ok("OPS 4314 PASS -- the map finally looks like the market")
