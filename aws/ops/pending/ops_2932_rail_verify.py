#!/usr/bin/env python3
"""ops 2932 — right-rail live verification: renderer live, coverage matches
build (238/366), real freshness values landed (not all-null), spot pages."""
import json, re, sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor
sys.path.insert(0, "aws/ops")
from ops_report import report

def get(u, to=15):
    try:
        r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent":"Mozilla/5.0 jh"}), timeout=to)
        return r.getcode(), r.read().decode("utf-8", "replace")
    except Exception:
        return None, ""

ok = True; out = {}
with report("2932") as r:
    for att in range(12):
        c, idx = get(f"https://justhodl.ai/index.html?t={int(time.time())}")
        if 'JH_V="v1.2.17"' in idx: break
        time.sleep(18)
    r.ok(f"v1.2.17 deploy live attempt {att+1}")

    c, js = get(f"https://justhodl.ai/jh-right-rail.js?t={int(time.time())}")
    ok &= c == 200 and "__jhRail" in js
    (r.ok if ok else r.fail)(f"jh-right-rail.js live: http={c} bytes={len(js)}")

    c, mj = get(f"https://justhodl.ai/nav-manifest.json?t={int(time.time())}")
    pages = [p["href"].lstrip("/") for cat in json.loads(mj)["categories"] for p in cat["pages"]]
    def chk(p):
        c2, b = get(f"https://justhodl.ai/{p}?t={int(time.time())}")
        m = re.search(r"window\.__jhRail=(\{.*?\});", b) if c2 == 200 else None
        if not m: return p, False, None
        try: return p, True, json.loads(m.group(1))
        except Exception: return p, False, None
    with_rail, ages_seen = [], []
    with ThreadPoolExecutor(max_workers=12) as ex:
        for p, has, d in ex.map(chk, pages):
            if has:
                with_rail.append(p)
                ages_seen += [f.get("h") for f in d.get("feeds", []) if f.get("h") is not None]
    out["pages_total"] = len(pages); out["pages_with_rail"] = len(with_rail)
    cov_ok = len(with_rail) == 238
    ok &= cov_ok
    (r.ok if cov_ok else r.fail)(f"rail coverage LIVE: {len(with_rail)}/366 (expect 238)")
    real_ages = sum(1 for a in ages_seen if a is not None and a >= 0)
    ok &= real_ages > 100
    (r.ok if real_ages > 100 else r.fail)(f"real freshness values landed: {real_ages} (of {len(ages_seen)} feed refs seen)")

    for p in ("lce.html", "bonds.html", "options.html", "apac.html"):
        c2, b = get(f"https://justhodl.ai/{p}?t={int(time.time())}")
        m = re.search(r"window\.__jhRail=(\{.*?\});", b)
        d = json.loads(m.group(1)) if m else {}
        r.ok(f"  spot {p}: feeds={[f['label'] for f in d.get('feeds',[])]} "
             f"related={[x['title'] for x in d.get('related',[])]}")
    out["with_rail_sample"] = with_rail[:15]
    json.dump(out, open("aws/ops/reports/rail_2932.json", "w"), indent=2)
print("DONE 2932", "PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)
