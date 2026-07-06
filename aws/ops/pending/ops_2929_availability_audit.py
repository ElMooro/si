#!/usr/bin/env python3
"""ops 2929 — FULL AVAILABILITY AUDIT: pages structural + repaired-JS live proof
+ feeds referenced-vs-live with engine classification + freshness buckets."""
import json, re, subprocess, sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor
from email.utils import parsedate_to_datetime
sys.path.insert(0, "aws/ops")
from ops_report import report

def get(u, to=15):
    try:
        r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent":"Mozilla/5.0 jh"}), timeout=to)
        return r.getcode(), r.read().decode("utf-8","replace"), dict(r.headers)
    except Exception:
        return None, "", {}

ok = True; out = {}
with report("2929") as r:
    for att in range(12):
        c, idx, _ = get(f"https://justhodl.ai/benzinga.html?t={int(time.time())}")
        if c == 200 and "\n<table><tr><th>Date" not in idx: break
        time.sleep(18)
    r.ok(f"repair deploy live attempt {att+1}")

    # ── A: pages structural, full population ──
    c, mj, _ = get(f"https://justhodl.ai/nav-manifest.json?t={int(time.time())}")
    pages = [p["href"].lstrip("/") for cat in json.loads(mj)["categories"] for p in cat["pages"]]
    def pchk(p):
        c2, b, _ = get(f"https://justhodl.ai/{p}?t={int(time.time())}")
        okp = (c2 == 200 and len(b) > 2000 and "</html>" in b
               and ("screener" in p or p == "index.html" or
                    ("/jh-chart-theme.js" in b and "jh-nav-drawer.js" in b)))
        return p, okp, c2, len(b)
    bad = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        for p, okp, c2, ln in ex.map(pchk, pages):
            if not okp: bad.append((p, c2, ln))
    out["pages_total"] = len(pages); out["pages_bad"] = bad[:10]
    ok &= not bad
    (r.ok if not bad else r.fail)(f"A pages structural: {len(pages)-len(bad)}/{len(pages)}"
                                  + ("" if not bad else f" bad={bad[:5]}"))

    # ── A2: the 9 repaired blocks, node-checked from LIVE ──
    REPAIRED = {"apac.html":0,"ath.html":0,"benzinga.html":0,"desk-v2.html":0,"desk.html":1,
                "fmp.html":0,"ml-predictions.html":0,"supply-chain.html":0,"trading-signals.html":0}
    live_js_ok = 0
    for f, i in REPAIRED.items():
        c2, b, _ = get(f"https://justhodl.ai/{f}?t={int(time.time())}")
        blks = re.findall(r"<script(?![^>]*\bsrc=)[^>]*>(.*?)</script>", b, re.S|re.I)
        open("/tmp/c.js","w").write(blks[i] if i < len(blks) else "]")
        live_js_ok += subprocess.run(["node","--check","/tmp/c.js"],capture_output=True).returncode == 0
    out["repaired_live_parse"] = live_js_ok
    ok &= live_js_ok == 9
    (r.ok if live_js_ok == 9 else r.fail)(f"A2 repaired blocks parse LIVE: {live_js_ok}/9")

    # ── C: feeds referenced vs live, classified ──
    refs = json.load(open("aws/ops/reports/feeds_ref_2929.json"))
    reg = json.load(open("data/engine-registry.json"))
    engines = set()
    for e in (reg.get("engines") or reg if isinstance(reg, list) else reg.get("engines", {})):
        engines.add((e.get("name") if isinstance(e, dict) else str(e)).lower())
    def fchk(ref):
        u = "https://justhodl.ai/" + (ref if ref.startswith("data/") else "data/"+ref)
        c2, _, h = get(u); lm = h.get("Last-Modified","")
        age = None
        if lm:
            try: age = (time.time() - parsedate_to_datetime(lm).timestamp())/3600
            except Exception: pass
        return ref, c2, age
    live, missing, ages = [], [], []
    with ThreadPoolExecutor(max_workers=10) as ex:
        for ref, c2, age in ex.map(fchk, refs):
            (live if c2 == 200 else missing).append(ref)
            if c2 == 200 and age is not None: ages.append((ref, age))
    def has_engine(ref):
        stem = re.sub(r"[^a-z]+","-", ref.split("/")[-1].replace(".json","").lower())
        toks = [t for t in stem.split("-") if len(t) > 2]
        return any(all(t in e for t in toks[:2]) for e in engines) if toks else False
    m_eng = sorted(x for x in missing if has_engine(x))
    m_no  = sorted(x for x in missing if x not in m_eng)
    fresh = sum(1 for _, a in ages if a < 24); mid = sum(1 for _, a in ages if 24 <= a < 48)
    stale = sorted([x for x in ages if x[1] >= 48], key=lambda t:-t[1])
    out["feeds"] = {"referenced": len(refs), "live": len(live), "missing_engine_exists": m_eng,
                    "missing_no_engine": m_no, "fresh_24h": fresh, "mid_24_48": mid,
                    "stale_48h": [(f, round(a)) for f, a in stale[:10]]}
    r.ok(f"C feeds: {len(live)}/{len(refs)} live | missing w/engine={len(m_eng)} no-engine={len(m_no)} | fresh<24h={fresh} 24-48h={mid} stale>48h={len(stale)}")
    out["engines_registry"] = len(engines)
    r.ok(f"D engines in registry: {len(engines)}")
    json.dump(out, open("aws/ops/reports/availability_2929.json","w"), indent=2)
print("DONE 2929", "PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)
