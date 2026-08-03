"""ops_4303 -- live pages for the two new engines, verified on edge."""
import sys, time, urllib.request
from ops_report import report
fails = []
with report("4303_engine_pages") as r:
    r.heading("ops 4303 -- treasury-rehypo + trend-reversal pages")
    for path, mark in (("treasury-rehypo.html",
                        "Rehypothecation Stress"),
                       ("trend-reversal.html",
                        "Trend-Reversal Radar")):
        body = ""
        for i in range(12):
            try:
                req = urllib.request.Request(
                    "https://justhodl.ai/" + path,
                    headers={"User-Agent": "ops/4303",
                             "Cache-Control": "no-cache"})
                body = urllib.request.urlopen(
                    req, timeout=25).read().decode("utf-8", "ignore")
                if mark in body:
                    break
            except Exception:
                pass
            time.sleep(20)
        if mark in body:
            r.ok("%s LIVE (%d bytes)" % (path, len(body)))
        else:
            fails.append("%s not on edge" % path)
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4303 PASS")
if fails:
    sys.exit(1)
