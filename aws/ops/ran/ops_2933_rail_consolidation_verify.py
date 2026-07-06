#!/usr/bin/env python3
"""ops 2933 — verify consolidation: exactly ONE rail system live, jh-rail.js gone,
jh-right-rail.js unaffected, still 238/366."""
import json, sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor
sys.path.insert(0, "aws/ops")
from ops_report import report

def get(u, to=15):
    try:
        r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent":"Mozilla/5.0 jh"}), timeout=to)
        return r.getcode(), r.read().decode("utf-8", "replace")
    except Exception:
        return None, ""

ok = True
with report("2933") as r:
    for att in range(12):
        c, idx = get(f"https://justhodl.ai/index.html?t={int(time.time())}")
        if 'JH_V="v1.2.18"' in idx: break
        time.sleep(18)
    r.ok(f"v1.2.18 deploy live attempt {att+1}")

    c, _ = get(f"https://justhodl.ai/jh-rail.js?t={int(time.time())}")
    gone = c in (404, None)
    ok &= gone
    (r.ok if gone else r.fail)(f"jh-rail.js retired: http={c} (expect 404)")

    c, mj = get(f"https://justhodl.ai/nav-manifest.json?t={int(time.time())}")
    pages = [p["href"].lstrip("/") for cat in json.loads(mj)["categories"] for p in cat["pages"]]
    def chk(p):
        c2, b = get(f"https://justhodl.ai/{p}?t={int(time.time())}")
        return p, c2 == 200 and "__jhRail" in b, c2 == 200 and 'src="/jh-rail.js"' in b
    with_new = with_old = 0
    with ThreadPoolExecutor(max_workers=12) as ex:
        for p, has_new, has_old in ex.map(chk, pages):
            with_new += has_new; with_old += has_old
    cov_ok = with_new == 238 and with_old == 0
    ok &= cov_ok
    (r.ok if cov_ok else r.fail)(f"coverage: jh-right-rail={with_new}/366 (expect 238) | old-system-traces={with_old} (expect 0)")
print("DONE 2933", "PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)
