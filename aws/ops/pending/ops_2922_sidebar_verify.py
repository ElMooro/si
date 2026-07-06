#!/usr/bin/env python3
"""ops 2922 — live sidebar v2 verify: structure, suppression order, manifest, favorites plumbing."""
import json, sys, time, urllib.request
sys.path.insert(0, "aws/ops")
from ops_report import report

def get(u, to=15):
    try:
        r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0 jh"}), timeout=to)
        return r.getcode(), r.read().decode("utf-8", "replace")
    except Exception:
        return None, ""

ok_all = True
out = {}
with report("2922") as r:
    for att in range(10):
        c, idx = get(f"https://justhodl.ai/index.html?t={int(time.time())}")
        if 'JH_V="v1.2.6"' in idx:
            break
        time.sleep(18)
    out["attempt"] = att + 1
    checks = {
        "v1.2.6": 'JH_V="v1.2.6"' in idx,
        "sidebar_container": 'id="jh-sidebar"' in idx,
        "mount": 'id="jh-side"' in idx,
        "search_input": 'id="jh-side-q"' in idx,
        "pin": 'id="jh-side-pin"' in idx,
        "favs_key": "jh_favs" in idx,
        "manifest_fetch": "/nav-manifest.json" in idx,
        "static_groups_gone": (">OVERVIEW<" not in idx and ">PLATFORM<" not in idx),
        "cmdk_slash": '(k==="/"&&!typing)' in idx,
        "no_object_leak": "[object Object]" not in idx.replace('indexOf("[object")', ""),
    }
    guard_i = idx.find("window.__jhNavDrawer=true")
    drawer_i = idx.find('src="/jh-nav-drawer.js"')
    checks["drawer_suppression_order"] = 0 < guard_i < drawer_i
    out["checks"] = checks
    ok_all &= all(checks.values())
    for k, v in checks.items():
        (r.ok if v else r.fail)(f"  {k}: {v}")

    c, b = get(f"https://justhodl.ai/nav-manifest.json?t={int(time.time())}")
    m = json.loads(b) if c == 200 else {}
    n = sum(len(cat.get("pages", [])) for cat in m.get("categories", []))
    out["manifest_pages"] = n
    ok_all &= (n == 366)
    (r.ok if n == 366 else r.fail)(f"manifest pages: {n}")

    c, b = get(f"https://justhodl.ai/jh-nav-drawer.js?t={int(time.time())}")
    drawer_ok = c == 200 and "jh_favs" in b and "slice(0, 80)" in b
    out["drawer_intact"] = drawer_ok
    ok_all &= drawer_ok
    (r.ok if drawer_ok else r.fail)(f"drawer intact on other 365 pages (favs key + cap-80 only): {drawer_ok}")

    json.dump(out, open("aws/ops/reports/2922.json", "w"), indent=2, default=str)
    r.ok("report -> 2922.json")
print("DONE 2922", "PASS" if ok_all else "FAIL")
sys.exit(0 if ok_all else 1)
