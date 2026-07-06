#!/usr/bin/env python3
"""ops 2940 — final pricing.html verification: precise checks this time."""
import sys, time, urllib.request
sys.path.insert(0, "aws/ops")
from ops_report import report

def get(u):
    r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent":"Mozilla/5.0 jh"}), timeout=15)
    return r.getcode(), r.read().decode("utf-8", "replace")

ok = True
with report("2940") as r:
    for att in range(14):
        c, idx = get(f"https://justhodl.ai/index.html?t={int(time.time())}")
        if 'JH_V="v1.2.23"' in idx: break
        time.sleep(18)
    ok &= 'JH_V="v1.2.23"' in idx
    (r.ok if ok else r.fail)(f"v1.2.23 deploy live attempt {att+1}")

    c, pr = get(f"https://justhodl.ai/pricing.html?t={int(time.time())}")
    checks = {
        "http 200": c == 200,
        "no rocket emoji": "🚀" not in pr,
        "lock-in banner phrase present": "lock in these prices" in pr.lower(),
        "zero gradients anywhere": pr.count("linear-gradient") == 0,
        "theme-color present + canonical": '<meta name="theme-color" content="#F0B429">' in pr,
        "full chrome present": all(x in pr for x in ("jh-nav-drawer.js", 'src="/jh-footer.js"', "/jh-chart-theme.js")),
    }
    for k, v in checks.items():
        ok &= v
        (r.ok if v else r.fail)(f"  {k}: {v}")
print("DONE 2940", "PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)
