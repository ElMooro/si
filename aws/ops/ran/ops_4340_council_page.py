"""ops_4340 -- alpha-council.html live on the edge, artifact-joined."""
import sys, time, urllib.request
from ops_report import report
fails = []
with report("4340_council_page") as r:
    r.heading("ops 4340 -- the council gets its chamber")
    body = ""
    for _ in range(14):
        try:
            body = urllib.request.urlopen(urllib.request.Request(
                "https://justhodl.ai/alpha-council.html",
                headers={"User-Agent": "ops/4340"}),
                timeout=25).read().decode("utf-8", "ignore")
            if "Alpha Council" in body:
                break
        except Exception:
            pass
        time.sleep(20)
    for mk in ("Alpha Council", "Wilson", "one-engine-one-vote",
               "Consensus board", "Honest zero", "Accountability",
               "eng:alpha-council", "Methodology"):
        if mk not in body:
            fails.append("edge missing %s" % mk)
    if fails:
        for f in fails:
            r.fail("  %s" % f)
        sys.exit(1)
    r.ok("PAGE LIVE: https://justhodl.ai/alpha-council.html "
         "(%d bytes) -- honest-zero state designed in" % len(body))
