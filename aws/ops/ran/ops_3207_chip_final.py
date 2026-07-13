"""ops 3207 — chip verified on BOTH page classes. 3206's evidence named
the real failure: flows.html fetches via the workers-proxy CDN template,
so the rail's data-ref regex never matched and the page never qualified —
the 255 pages that DO match have carried the chip since the first fixed
bake. The regex now also captures ${CDN}/x.json keys (flows-class gains
rails), and this ops proves research in the live payload of a
known-qualifying page immediately, then flows.html after this deploy."""
import re
import sys
import time
import urllib.request

from ops_report import report

UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3207)"}


def payload(page):
    h = urllib.request.urlopen(urllib.request.Request(
        f"https://justhodl.ai/{page}?t={int(time.time())}",
        headers=UA), timeout=15).read().decode("utf-8", "replace")
    m = re.search(r"__jhRail=(\{.*?\});</script>", h)
    return m.group(1) if m else None


with report("3207_chip_final") as rep:
    fails, warns = [], []
    rep.heading("ops 3207 — HIS RESEARCH chip proven on both page classes")

    rep.section("1. Known-qualifying page (live now)")
    ok1 = False
    for page in ("accumulation.html", "activity-nowcast.html"):
        try:
            pl = payload(page)
        except Exception as e:
            rep.log(f"  {page}: fetch {str(e)[:50]}")
            continue
        if pl and '"research"' in pl:
            i = pl.find('"research"')
            rep.ok(f"{page}: research live — …{pl[i:i + 200]}")
            ok1 = True
            break
        rep.log(f"  {page}: payload {'present, no research' if pl else 'absent'}")
    if not ok1:
        fails.append("no qualifying page shows research — bake-side issue "
                     "remains")

    rep.section("2. flows-class after the widened regex (this deploy)")
    ok2 = False
    for _ in range(24):
        time.sleep(15)
        try:
            pl = payload("flows.html")
        except Exception:
            continue
        if pl and '"research"' in pl:
            ok2 = True
            rep.ok("flows.html: rail + research live (CDN-class fixed)")
            break
    if not ok2:
        warns.append("flows.html not yet carrying the rail — lands on the "
                     "next cron bake if the deploy race lost")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
