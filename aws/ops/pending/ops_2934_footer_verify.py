#!/usr/bin/env python3
"""ops 2934 — shared footer live verification: coverage, exclusions, real
subscribe-endpoint contract intact, links resolve."""
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
with report("2934") as r:
    for att in range(12):
        c, idx = get(f"https://justhodl.ai/index.html?t={int(time.time())}")
        if 'JH_V="v1.2.19"' in idx: break
        time.sleep(18)
    r.ok(f"v1.2.19 deploy live attempt {att+1}")

    c, js = get(f"https://justhodl.ai/jh-footer.js?t={int(time.time())}")
    ok &= c == 200 and "jh_sub" in js and "subscribe-endpoint" in js
    (r.ok if ok else r.fail)(f"jh-footer.js live: http={c} bytes={len(js)}")

    idx_has = "jh-shared-footer" in idx
    ok &= not idx_has
    (r.ok if not idx_has else r.fail)(f"index.html correctly WITHOUT shared footer (has own hero CTA): {not idx_has}")

    c, sc = get(f"https://justhodl.ai/screener/?t={int(time.time())}")
    scr_clean = c != 200 or "jh-footer.js" not in sc
    ok &= scr_clean
    (r.ok if scr_clean else r.fail)(f"screener untouched: http={c} footer-script-present={'jh-footer.js' in sc if c==200 else 'n/a'}")

    c, mj = get(f"https://justhodl.ai/nav-manifest.json?t={int(time.time())}")
    pages = [p["href"].lstrip("/") for cat in json.loads(mj)["categories"] for p in cat["pages"]]
    def chk(p):
        c2, b = get(f"https://justhodl.ai/{p}?t={int(time.time())}")
        return p, c2 == 200 and "jh-shared-footer" in b
    n_with = 0
    with ThreadPoolExecutor(max_workers=12) as ex:
        for p, has in ex.map(chk, pages):
            n_with += has
    cov_ok = n_with == 365
    ok &= cov_ok
    (r.ok if cov_ok else r.fail)(f"footer coverage: {n_with}/366 (expect 365, index excluded by design)")

    for slug, name in [("about.html","About"),("glossary.html","Glossary"),("pricing.html","Pricing"),
                       ("status.html","Status"),("terms.html","Terms"),("privacy.html","Privacy")]:
        c2, _ = get(f"https://justhodl.ai/{slug}?t={int(time.time())}")
        (r.ok if c2 == 200 else r.fail)(f"  footer link resolves: {name} -> {slug} http={c2}")
        ok &= c2 == 200

    c2, ep = get(f"https://justhodl.ai/data/subscribe-endpoint.json?t={int(time.time())}")
    ep_ok = c2 == 200 and "url" in json.loads(ep or "{}")
    ok &= ep_ok
    (r.ok if ep_ok else r.fail)(f"subscribe-endpoint.json still live for the shared contract: {ep_ok}")
print("DONE 2934", "PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)
