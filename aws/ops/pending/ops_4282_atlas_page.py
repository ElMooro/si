"""ops_4282 -- Alpha Atlas page live on the edge, rendering the real
atlas (186 engines / 12 families / 60 dormant from ops 4281)."""
import sys, time, urllib.request
from ops_report import report
fails = []
with report("4282_atlas_page") as r:
    r.heading("ops 4282 -- alpha-atlas.html on edge")
    body = ""
    for i in range(10):
        try:
            req = urllib.request.Request(
                "https://justhodl.ai/alpha-atlas.html",
                headers={"User-Agent": "ops/4282",
                         "Cache-Control": "no-cache"})
            body = urllib.request.urlopen(req, timeout=25).read().decode(
                "utf-8", "ignore")
            if "Alpha Atlas" in body and "alpha-atlas.json" in body:
                break
        except Exception as e:
            r.log("wait %d: %s" % (i, str(e)[:70]))
        time.sleep(20)
    if "Alpha Atlas" in body and "alpha-atlas.json" in body:
        r.ok("page LIVE (%d bytes) -- fetches data/alpha-atlas.json "
             "client-side" % len(body))
    else:
        fails.append("page not on edge yet (%d bytes)" % len(body))
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4282 PASS")
if fails:
    sys.exit(1)
