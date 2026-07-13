"""ops 3272 — favorites/sidebar cache-bust. 3269's fixes were live at
the origin but the drawer JS and manifest are CDN/browser-cached with
no versioning — Khalid's browser kept serving the July-5 world.
Now: the drawer script src carries a content hash (reskin bake) and
the drawer fetches the manifest with an hourly buster. Verify on the
served homepage + served drawer."""
import sys
import time
import urllib.request

from ops_report import report

UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3272)"}


def get(u):
    return urllib.request.urlopen(
        urllib.request.Request(u, headers=UA), timeout=15).read()\
        .decode("utf-8", "replace")


with report("3272_fav_cachebust") as rep:
    fails = []
    ok_h = ok_d = False
    for i in range(24):
        try:
            h = get(f"https://justhodl.ai/?t={int(time.time())}")
            ok_h = "jh-nav-drawer.js?v=" in h
        except Exception:
            pass
        if ok_h:
            rep.ok(f"homepage serves versioned drawer src "
                   f"(~{(i + 1) * 15}s)")
            break
        time.sleep(15)
    try:
        js = get("https://justhodl.ai/jh-nav-drawer.js"
                 f"?t={int(time.time())}")
        ok_d = "nav-manifest.json?v=" in js
        if ok_d:
            rep.ok("drawer fetches manifest with hourly buster")
    except Exception:
        pass
    if not ok_h:
        fails.append("versioned src not in served homepage")
    if not ok_d:
        fails.append("busted manifest fetch not in served drawer")
    rep.kv(homepage=ok_h, drawer=ok_d,
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
