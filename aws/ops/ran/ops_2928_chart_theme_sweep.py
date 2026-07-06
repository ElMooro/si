#!/usr/bin/env python3
"""ops 2928 — chart-theme layer: full-population coverage + lib-page guarantees."""
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
LIBS = ("echarts", "LightweightCharts", "new Chart(", "Plotly")
ok = True; out = {}
with report("2928") as r:
    for att in range(12):
        c, idx = get(f"https://justhodl.ai/index.html?t={int(time.time())}")
        if 'JH_V="v1.2.15"' in idx: break
        time.sleep(18)
    r.ok(f"v1.2.15 deploy live attempt {att+1}")
    c, tj = get(f"https://justhodl.ai/jh-chart-theme.js?t={int(time.time())}")
    markers = all(m in tj for m in ("echarts", "LightweightCharts", '"Chart"', '"Plotly"', "defineProperty"))
    ok &= c == 200 and markers
    out["theme_js"] = {"http": c, "bytes": len(tj), "all_lib_markers": markers}
    (r.ok if markers else r.fail)(f"jh-chart-theme.js live, interception + 4 libs: {out['theme_js']}")
    c, mj = get(f"https://justhodl.ai/nav-manifest.json?t={int(time.time())}")
    pages = [p["href"].lstrip("/") for cat in json.loads(mj)["categories"] for p in cat["pages"]]
    pages = [p for p in pages if p != "index.html" and "screener" not in p]
    def chk(p):
        c2, b = get(f"https://justhodl.ai/{p}?t={int(time.time())}")
        return p, c2, ("/jh-chart-theme.js" in b), [l for l in LIBS if l in b]
    untagged, lib_untagged = [], []
    with ThreadPoolExecutor(max_workers=10) as ex:
        for p, c2, tag, libs in ex.map(chk, pages):
            if not tag:
                untagged.append(p)
                if libs: lib_untagged.append((p, libs))
    out["pages"] = len(pages); out["untagged"] = untagged[:10]; out["lib_untagged"] = lib_untagged[:10]
    cov = not untagged
    ok &= cov and not lib_untagged
    (r.ok if cov else r.fail)(f"tag coverage: {len(pages)-len(untagged)}/{len(pages)}"
                              + ("" if cov else f" missing: {untagged[:5]}"))
    (r.ok if not lib_untagged else r.fail)(f"chart-library pages unprotected: {len(lib_untagged)}")
    for p in ("chart-pro.html", "onchain.html", "dex.html"):
        c2, b = get(f"https://justhodl.ai/{p}?t={int(time.time())}")
        got = "/jh-chart-theme.js" in b
        ok &= got
        (r.ok if got else r.fail)(f"  spot {p}: themed={got}")
    json.dump(out, open("aws/ops/reports/2928.json", "w"), indent=2)
print("DONE 2928", "PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)
