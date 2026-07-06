#!/usr/bin/env python3
"""ops 2943 — mobile tape-collapse live verification."""
import sys, time, urllib.request
sys.path.insert(0, "aws/ops")
from ops_report import report

def get(u):
    r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent":"Mozilla/5.0 jh"}), timeout=15)
    return r.getcode(), r.read().decode("utf-8", "replace")

ok = True
with report("2943") as r:
    for att in range(14):
        c, idx = get(f"https://justhodl.ai/index.html?t={int(time.time())}")
        if 'JH_V="v1.2.25"' in idx: break
        time.sleep(18)
    ok &= 'JH_V="v1.2.25"' in idx
    (r.ok if ok else r.fail)(f"v1.2.25 deploy live attempt {att+1}")

    c, drawer = get(f"https://justhodl.ai/jh-nav-drawer.js?t={int(time.time())}")
    sym_ok = c == 200 and 'setAttribute("data-sym"' in drawer
    ok &= sym_ok
    (r.ok if sym_ok else r.fail)(f"jh-nav-drawer.js carries data-sym targeting: {sym_ok}")

    c, theme = get(f"https://justhodl.ai/jh-theme.css?t={int(time.time())}")
    css_ok = c == 200 and 'data-sym="SPX"' in theme and 'data-sym="BTC"' in theme and "max-width:640px" in theme
    ok &= css_ok
    (r.ok if css_ok else r.fail)(f"jh-theme.css carries the mobile collapse rule: {css_ok}")

    c, mt = get(f"https://justhodl.ai/data/market-tape.json?t={int(time.time())}")
    import json
    labels = [i.get("label") for i in json.loads(mt).get("items", [])]
    real_ok = set(labels) == {"SPX","NDX","BTC","GOLD","US10Y","VIX","DXY"}
    ok &= real_ok
    (r.ok if real_ok else r.fail)(f"live tape labels still match what the CSS targets: {labels}")
print("DONE 2943", "PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)
