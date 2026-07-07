#!/usr/bin/env python3
"""ops 2946 — Engine Directory live verification: page live, bake actually
ran (marker replaced with real data, not still literal __JH_ENGINE_DATA__),
counts sane, chrome present, nav+directory links resolve."""
import json, re, sys, time, urllib.request
sys.path.insert(0, "aws/ops")
from ops_report import report

def get(u):
    r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent":"Mozilla/5.0 jh"}), timeout=20)
    return r.getcode(), r.read().decode("utf-8", "replace")

ok = True
with report("2946") as r:
    for att in range(16):
        c, idx = get(f"https://justhodl.ai/index.html?t={int(time.time())}")
        if 'JH_V="v1.2.26"' in idx: break
        time.sleep(18)
    ok &= 'JH_V="v1.2.26"' in idx
    (r.ok if ok else r.fail)(f"v1.2.26 deploy live attempt {att+1}")

    c, eng = get(f"https://justhodl.ai/engines.html?t={int(time.time())}")
    baked = c == 200 and "__JH_ENGINE_DATA__" not in eng and "window.__jhEngineData" in eng
    ok &= baked
    (r.ok if baked else r.fail)(f"engines.html: http={c} bake marker replaced={baked}")

    m = re.search(r"window\.__jhEngineData\s*=\s*(\{.*?\});", eng, re.S)
    if m:
        data = json.loads(m.group(1))
        sane = data.get("total", 0) > 600 and "wired" in data.get("counts", {})
        ok &= sane
        (r.ok if sane else r.fail)(f"  total={data.get('total')} counts={data.get('counts')}")
    else:
        ok = False
        r.fail("  could not extract baked data at all")

    chrome_ok = all(x in eng for x in ("jh-nav-drawer.js", 'src="/jh-footer.js"', "/jh-chart-theme.js"))
    ok &= chrome_ok
    (r.ok if chrome_ok else r.fail)(f"chrome present: {chrome_ok}")

    c, mj = get(f"https://justhodl.ai/nav-manifest.json?t={int(time.time())}")
    man = json.loads(mj)
    all_hrefs = [p["href"].lstrip("/") for cat in man["categories"] for p in cat["pages"]]
    nav_ok = "engines.html" in all_hrefs and len(all_hrefs) == len(set(all_hrefs)) == 366
    ok &= nav_ok
    (r.ok if nav_ok else r.fail)(f"nav-manifest: engines.html present, {len(all_hrefs)} total, {len(set(all_hrefs))} unique (want 366/366)")

    c, dirp = get(f"https://justhodl.ai/directory.html?t={int(time.time())}")
    dir_ok = c == 200 and 'href="/engines.html"' in dirp and "344" not in dirp
    ok &= dir_ok
    (r.ok if dir_ok else r.fail)(f"directory.html links to engines.html + stale count fixed: {dir_ok}")
print("DONE 2946", "PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)
