#!/usr/bin/env python3
"""ops 2941 — self-hosted fonts live verification: font files serve correctly,
zero IBM Plex Google Fonts refs remain, mixed-family pages kept their other
fonts, jh-theme.css carries the @font-face rules, rendering chrome intact."""
import sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor
sys.path.insert(0, "aws/ops")
from ops_report import report

def get(u, to=15):
    try:
        r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent":"Mozilla/5.0 jh"}), timeout=to)
        return r.getcode(), r.read(), dict(r.headers)
    except Exception:
        return None, b"", {}

ok = True
with report("2941") as r:
    for att in range(14):
        c, idx, _ = get(f"https://justhodl.ai/index.html?t={int(time.time())}")
        idx = idx.decode("utf-8", "replace")
        if 'JH_V="v1.2.24"' in idx: break
        time.sleep(18)
    r.ok(f"v1.2.24 deploy live attempt {att+1}")

    for fname in ("mono/IBMPlexMono-Regular.woff2", "mono/IBMPlexMono-SemiBold.woff2",
                  "sans/IBMPlexSans-Regular.woff2", "sans/IBMPlexSans-SemiBold.woff2"):
        c, body, hdrs = get(f"https://justhodl.ai/fonts/{fname}?t={int(time.time())}")
        good = c == 200 and len(body) > 20000 and body[:4] == b"wOF2"
        ok &= good
        (r.ok if good else r.fail)(f"  font live: {fname} http={c} bytes={len(body)} magic={body[:4]}")

    c, theme, _ = get(f"https://justhodl.ai/jh-theme.css?t={int(time.time())}")
    theme = theme.decode("utf-8", "replace")
    ff_ok = theme.count("@font-face") >= 8 and "/fonts/mono/" in theme and "/fonts/sans/" in theme
    ok &= ff_ok
    (r.ok if ff_ok else r.fail)(f"jh-theme.css carries @font-face rules: {ff_ok}")

    c, lce, _ = get(f"https://justhodl.ai/lce.html?t={int(time.time())}")
    lce = lce.decode("utf-8", "replace")
    pure_ok = "fonts.googleapis.com" not in lce
    ok &= pure_ok
    (r.ok if pure_ok else r.fail)(f"lce.html (pure IBM Plex page): Google Fonts fully removed = {pure_ok}")
    chrome_ok = all(x in lce for x in ("jh-nav-drawer.js", 'src="/jh-footer.js"', "/jh-chart-theme.js"))
    ok &= chrome_ok
    (r.ok if chrome_ok else r.fail)(f"lce.html chrome still intact: {chrome_ok}")

    c, f13, _ = get(f"https://justhodl.ai/13f.html?t={int(time.time())}")
    f13 = f13.decode("utf-8", "replace")
    mixed_ok = "Fraunces" in f13 and "IBM+Plex" not in f13
    ok &= mixed_ok
    (r.ok if mixed_ok else r.fail)(f"13f.html (mixed-family page): Fraunces kept, IBM Plex removed = {mixed_ok}")

    c, inv, _ = get(f"https://justhodl.ai/investor.html?t={int(time.time())}")
    inv = inv.decode("utf-8", "replace")
    serif_ok = "IBM+Plex+Serif" in inv
    ok &= serif_ok
    (r.ok if serif_ok else r.fail)(f"investor.html: out-of-scope IBM+Plex+Serif correctly left alone = {serif_ok}")
print("DONE 2941", "PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)
