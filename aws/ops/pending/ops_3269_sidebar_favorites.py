"""ops 3269 — sidebar + favorites, root-caused and fixed for good.

FINDINGS: favorites were never deleted — jh_favs intact, sync is a
true union-merge. The manifest froze 2026-07-05 (hand-maintained,
362 pages) while the site grew to 377: every newer page (panels.html
included) was invisible in the drawer AND its star filtered from the
FAVORITES display. FIXES: (1) scripts/gen_nav_manifest.py regenerates
the manifest from the actual repo pages on EVERY deploy (pages.yml
step); (2) the drawer now renders every starred href with a fallback
title — a star can never silently vanish again.

VERIFY live: /nav-manifest.json carries /panels.html + n_pages>=377;
served jh-nav-drawer.js carries the ops-3269 literal.
"""
import json
import sys
import time
import urllib.request

from ops_report import report

UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3269)"}


def get(u):
    return urllib.request.urlopen(
        urllib.request.Request(u, headers=UA), timeout=15).read()\
        .decode("utf-8", "replace")


with report("3269_sidebar_favorites") as rep:
    fails = []
    ok_m = ok_d = False
    for i in range(24):
        try:
            m = json.loads(get("https://justhodl.ai/nav-manifest.json"
                               f"?t={int(time.time())}"))
            hrefs = [p["href"] for c in m.get("categories", [])
                     for p in c.get("pages", [])]
            if "/panels.html" in hrefs and m.get("n_pages", 0) >= 377:
                ok_m = True
                rep.ok(f"manifest live: {m['n_pages']} pages, "
                       "Research Panels present "
                       f"(~{(i + 1) * 15}s)")
        except Exception:
            pass
        if ok_m:
            break
        time.sleep(15)
    try:
        js = get("https://justhodl.ai/jh-nav-drawer.js"
                 f"?t={int(time.time())}")
        ok_d = "ops 3269" in js
        if ok_d:
            rep.ok("drawer never-lose-stars live")
    except Exception:
        pass
    if not ok_m:
        fails.append("manifest not fresh in window")
    if not ok_d:
        fails.append("drawer literal not live")
    rep.kv(manifest=ok_m, drawer=ok_d,
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
