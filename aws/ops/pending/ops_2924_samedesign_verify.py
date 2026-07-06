#!/usr/bin/env python3
"""ops 2924 — sitewide same-design + sidebar-visibility verify."""
import json, re, sys, time, urllib.request
sys.path.insert(0, "aws/ops")
from ops_report import report
def get(u, to=15):
    try:
        r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent":"Mozilla/5.0 jh"}), timeout=to)
        return r.getcode(), r.read().decode("utf-8", "replace")
    except Exception:
        return None, ""
ok = True; out = {}
with report("2924") as r:
    for att in range(10):
        c, dj = get(f"https://justhodl.ai/jh-nav-drawer.js?t={int(time.time())}")
        if "jh-chrome" in dj and "if (false)" not in dj:
            break
        time.sleep(18)
    checks = {
        "drawer_chrome": "jh-chrome" in dj and "jhc-tape" in dj,
        "drawer_theme_alive": 'id = "jh-theme"' in dj.replace("'", '"') or '_jt.id = "jh-theme"' in dj,
        "no_dead_branch": "if (false)" not in dj,
        "drawer_name_sanitize": 'replace(/^[^A-Za-z0-9]+ */,"")' in dj,
        "screener_guard": '"/screener"' in dj,
    }
    c, idx = get(f"https://justhodl.ai/index.html?t={int(time.time())}")
    checks["sidebar_open_default"] = "dt.open=true" in idx
    checks["v1.2.11"] = 'JH_V="v1.2.11"' in idx
    checks["dashes_le_5"] = idx.count('">—</span>') <= 5
    c, mj = get(f"https://justhodl.ai/nav-manifest.json?t={int(time.time())}")
    m = json.loads(mj) if c == 200 else {}
    names = [cat["name"] for cat in m.get("categories", [])]
    checks["institutional_names"] = bool(names) and all(re.match(r"^[A-Za-z]", n) for n in names)
    c, th = get(f"https://justhodl.ai/jh-theme.css?t={int(time.time())}")
    checks["theme_v2_topbar"] = ".jh-topbar" in th
    c, pl = get(f"https://justhodl.ai/plumbing.html?t={int(time.time())}")
    checks["sample_page_has_shell"] = c == 200 and "jh-nav-drawer.js" in pl
    out["checks"] = checks; out["names"] = names
    ok = all(checks.values())
    for k, v in checks.items():
        (r.ok if v else r.fail)(f"  {k}: {v}")
    json.dump(out, open("aws/ops/reports/2924.json", "w"), indent=2)
print("DONE 2924", "PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)
