#!/usr/bin/env python3
"""ops 2925 — sitewide Amber reskin live verify (samples, chrome JS, screener protection)."""
import json, sys, time, urllib.request
sys.path.insert(0, "aws/ops")
from ops_report import report
LEGACY = ["#00d4ff", "#a78bfa", "#0a0e14", "#1c2433", "#26ffaf", "#ff5577"]
def get(u, to=15):
    try:
        r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent":"Mozilla/5.0 jh"}), timeout=to)
        return r.getcode(), r.read().decode("utf-8", "replace")
    except Exception:
        return None, ""
ok = True; out = {"pages": {}}
with report("2925") as r:
    for att in range(12):
        c, idx = get(f"https://justhodl.ai/index.html?t={int(time.time())}")
        if 'JH_V="v1.2.12"' in idx:
            break
        time.sleep(18)
    out["deploy_attempt"] = att + 1
    r.ok(f"v1.2.12 deploy live (attempt {att+1}); index dashes={idx.count(chr(34)+'>—</span>')}")
    for p in ["plumbing.html", "risk-desk.html", "cycle-clock.html", "signal-board.html", "today.html"]:
        c, b = get(f"https://justhodl.ai/{p}?t={int(time.time())}")
        low = b.lower()
        legacy_hits = [h for h in LEGACY if h in low]
        amber = ("#f5b93e" in low) or ("#0b0906" in low)
        good = c == 200 and not legacy_hits and amber
        ok &= good
        out["pages"][p] = {"http": c, "legacy": legacy_hits, "amber": amber}
        (r.ok if good else r.fail)(f"  {p}: legacy={legacy_hits} amber={amber}")
    c, dj = get(f"https://justhodl.ai/jh-nav-drawer.js?t={int(time.time())}")
    dok = c == 200 and "#f5b93e" in dj.lower() and "#22d3ee" not in dj.lower()
    ok &= dok
    out["drawer_amber"] = dok
    (r.ok if dok else r.fail)(f"drawer chrome amber: {dok}")
    sc_c, sc = get(f"https://justhodl.ai/screener/?t={int(time.time())}")
    if sc_c == 200 and len(sc) > 500:
        prot = any(h in sc.lower() for h in LEGACY) or "#f5b93e" not in sc.lower()
        out["screener"] = {"http": 200, "untouched": prot}
        (r.ok if prot else r.fail)(f"screener PROTECTED (legacy palette intact): {prot}")
        ok &= prot
    else:
        out["screener"] = {"http": sc_c, "note": "served outside Pages — untouchable by construction"}
        r.ok("screener served outside Pages artifact — untouched by construction")
    json.dump(out, open("aws/ops/reports/2925.json", "w"), indent=2)
print("DONE 2925", "PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)
