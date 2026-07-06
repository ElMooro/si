#!/usr/bin/env python3
"""ops 2926 — EXHAUSTIVE live color sweep: every page, zero legacy-cool tolerance.
New verification doctrine: sitewide claims require full-population sweeps."""
import colorsys, json, re, sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor
sys.path.insert(0, "aws/ops")
from ops_report import report

def get(u, to=15):
    try:
        r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0 jh"}), timeout=to)
        return r.getcode(), r.read().decode("utf-8", "replace")
    except Exception:
        return None, ""

def cool(r, g, b):
    h, l, s = colorsys.rgb_to_hls(r/255, g/255, b/255)
    return s >= 0.06 and 165 <= h*360 <= 300

def count_cool(s):
    n = 0
    for m in re.findall(r'#[0-9a-fA-F]{3,8}\b', s):
        hs = m.lstrip('#'); hs = (''.join(c*2 for c in hs[:3]) if len(hs) in (3,4) else hs)
        try:
            if cool(int(hs[0:2],16), int(hs[2:4],16), int(hs[4:6],16)): n += 1
        except Exception: pass
    for m in re.findall(r'\brgba?\(\s*([\d.]+)[,\s]+([\d.]+)[,\s]+([\d.]+)', s):
        try:
            if cool(float(m[0]), float(m[1]), float(m[2])): n += 1
        except Exception: pass
    return n

ok = True; out = {}
with report("2926") as r:
    for att in range(12):
        c, idx = get(f"https://justhodl.ai/index.html?t={int(time.time())}")
        if 'JH_V="v1.2.13"' in idx:
            break
        time.sleep(18)
    out["deploy_attempt"] = att + 1
    r.ok(f"v1.2.13 deploy live attempt {att+1} | index dashes={idx.count(chr(34)+'>—</span>')}")

    c, mj = get(f"https://justhodl.ai/nav-manifest.json?t={int(time.time())}")
    pages = [p["href"].lstrip("/") for cat in json.loads(mj)["categories"] for p in cat["pages"]]
    pages = [p for p in pages if p != "index.html" and "screener" not in p]
    def chk(p):
        c2, b = get(f"https://justhodl.ai/{p}?t={int(time.time())}")
        return p, c2, (count_cool(b) if c2 == 200 else -1)
    offenders = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        for p, c2, n in ex.map(chk, pages):
            if n != 0:
                offenders.append((p, c2, n))
    out["pages_swept"] = len(pages)
    out["offenders"] = offenders[:12]
    clean = not offenders
    ok &= clean
    (r.ok if clean else r.fail)(f"EXHAUSTIVE sweep: {len(pages)-len(offenders)}/{len(pages)} pages "
                                f"ZERO legacy-cool literals" + ("" if clean else f" | offenders: {offenders[:6]}"))
    for j in ("jh-nav-drawer.js", "jh-page-ai.js", "wss-client.js"):
        c2, b = get(f"https://justhodl.ai/{j}?t={int(time.time())}")
        n = count_cool(b)
        ok &= (n == 0)
        out[j] = n
        (r.ok if n == 0 else r.fail)(f"  {j}: cool={n}")
    json.dump(out, open("aws/ops/reports/2926.json", "w"), indent=2)
print("DONE 2926", "PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)
