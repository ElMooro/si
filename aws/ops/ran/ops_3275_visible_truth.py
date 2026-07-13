"""ops 3275 — remove the invisible-failure class. Watchlist block now
fetches SAME-ORIGIN (no cross-origin variables), renders its own
errors into the section (never silently empty), decouples list UI from
the chart lib, and shows 'N lists loaded'. Drawer FAVORITES states the
per-browser truth when empty. Verify served literals + same-origin
data reachability one more time."""
import json
import sys
import time
import urllib.request

from ops_report import report

UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3275)"}


def get(u):
    return urllib.request.urlopen(
        urllib.request.Request(u, headers=UA), timeout=20).read()\
        .decode("utf-8", "replace")


with report("3275_visible_truth") as rep:
    fails = []
    ok_p = ok_d = False
    for i in range(24):
        try:
            h = get("https://justhodl.ai/chart-pro.html?t="
                    f"{int(time.time())}")
            ok_p = ("ops 3275: SAME-ORIGIN" in h
                    and "'/data/tv-watchlists.json?v='" in h)
        except Exception:
            pass
        if ok_p:
            rep.ok(f"chart-pro hardened block live (~{(i+1)*15}s)")
            break
        time.sleep(15)
    try:
        js = get("https://justhodl.ai/jh-nav-drawer.js?t="
                 f"{int(time.time())}")
        ok_d = "No stars in THIS browser" in js
        if ok_d:
            rep.ok("drawer empty-favs truth live")
    except Exception:
        pass
    try:
        j = json.loads(get("https://justhodl.ai/data/"
                           "tv-watchlists.json?d=3275"))
        rep.kv(origin_lists=len(j.get("lists") or []))
    except Exception as e:
        fails.append(f"origin data: {str(e)[:60]}")
    if not ok_p:
        fails.append("hardened block not live")
    if not ok_d:
        fails.append("drawer hint not live")
    rep.kv(verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
