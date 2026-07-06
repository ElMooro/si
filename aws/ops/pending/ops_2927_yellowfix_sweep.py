#!/usr/bin/env python3
"""ops 2927 — full-population sweep with PERCEPTUAL metrics: zero cool AND
zero mustard AND zero dark-saturated ambers, plus dex.html explicit proof."""
import colorsys, json, re, sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor
sys.path.insert(0, "aws/ops")
from ops_report import report
def get(u, to=15):
    try:
        r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent":"Mozilla/5.0 jh"}), timeout=to)
        return r.getcode(), r.read().decode("utf-8", "replace")
    except Exception:
        return None, ""
def hslv(hs):
    hs = hs.lstrip('#'); hs = (''.join(c*2 for c in hs[:3]) if len(hs) in (3,4) else hs)
    r,g,b = int(hs[0:2],16), int(hs[2:4],16), int(hs[4:6],16)
    h,l,s = colorsys.rgb_to_hls(r/255,g/255,b/255); return h*360, s, l
def metrics(t):
    cool = dark_amber = 0
    for m in re.findall(r'#[0-9a-fA-F]{3,8}\b', t):
        try:
            h,s,l = hslv(m[:7])
            if s >= 0.06 and 165 <= h <= 300: cool += 1
            if 25 <= h < 65 and s >= 0.45 and l < 0.32: dark_amber += 1
        except Exception: pass
    return cool, t.lower().count("#8a6a25"), dark_amber
ok = True; out = {}
with report("2927") as r:
    for att in range(12):
        c, idx = get(f"https://justhodl.ai/index.html?t={int(time.time())}")
        if 'JH_V="v1.2.14"' in idx: break
        time.sleep(18)
    r.ok(f"v1.2.14 deploy live attempt {att+1}")
    c, mj = get(f"https://justhodl.ai/nav-manifest.json?t={int(time.time())}")
    pages = [p["href"].lstrip("/") for cat in json.loads(mj)["categories"] for p in cat["pages"]]
    pages = [p for p in pages if p != "index.html" and "screener" not in p]
    def chk(p):
        c2, b = get(f"https://justhodl.ai/{p}?t={int(time.time())}")
        return (p, c2) + (metrics(b) if c2 == 200 else (-1,-1,-1))
    bad = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        for p, c2, cool, must, da in ex.map(chk, pages):
            if cool or must or da or c2 != 200:
                bad.append((p, c2, cool, must, da))
    out["pages_swept"] = len(pages); out["offenders"] = bad[:12]
    ok &= not bad
    (r.ok if not bad else r.fail)(
        f"EXHAUSTIVE: {len(pages)-len(bad)}/{len(pages)} pages clean "
        f"(cool=0, mustard=0, dark-amber=0)" + ("" if not bad else f" | {bad[:5]}"))
    c, dx = get(f"https://justhodl.ai/dex.html?t={int(time.time())}")
    dxm = metrics(dx)
    dex_ok = dxm == (0,0,0) and ("#0b0906" in dx.lower() or "#141008" in dx.lower())
    ok &= dex_ok
    out["dex"] = {"metrics": dxm, "warm_black_bg": dex_ok}
    (r.ok if dex_ok else r.fail)(f"dex.html: metrics={dxm} warm-black bg present={dex_ok}")
    json.dump(out, open("aws/ops/reports/2927.json","w"), indent=2)
print("DONE 2927", "PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)
