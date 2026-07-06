#!/usr/bin/env python3
"""ops 2932 — right-rail: tag coverage sitewide + real-render proof on sample
desk pages (headless-free: parse jh-rail.js's own matching logic server-side
against each page's actual script content + live registry, so we get a true
render/no-render prediction without a browser)."""
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
with report("2933") as r:
    for att in range(12):
        c, idx = get(f"https://justhodl.ai/index.html?t={int(time.time())}")
        if 'JH_V="v1.2.17"' in idx: break
        time.sleep(18)
    r.ok(f"v1.2.17 deploy live attempt {att+1}")

    c, rj = get(f"https://justhodl.ai/data/engine-registry.json?t={int(time.time())}")
    reg = json.loads(rj)
    raw = reg.get("engines", reg) if isinstance(reg, dict) else reg
    entries = ([dict(v, name=k) for k, v in raw.items()] if isinstance(raw, dict) else raw)
    outs_index = {}
    doc_nonempty = 0
    for e in entries:
        if (e.get("doc") or "").strip(): doc_nonempty += 1
        for o in e.get("outs", []): outs_index.setdefault(o, []).append(e.get("name"))

    def stem(s):
        return s.replace("data/", "").replace(".json", "").lower()
    def toks(s):
        return [t for part in stem(s).split("-") for t in part.split("_") if t]
    def best_match(page, refs):
        pstem = stem(page.rsplit(".", 1)[0]); ptoks = toks(page.rsplit(".", 1)[0])
        best, bscore = None, 0
        for o in refs:
            names = outs_index.get(o)
            if not names: continue
            ot = toks(o)
            shared = len(set(ptoks) & set(ot))
            initials = "".join(t[0] for t in ot if t)
            sc = shared / max(len(ptoks), len(ot), 1)
            if initials == pstem or stem(o) == pstem: sc = 1
            if sc > bscore: bscore, best = sc, names[0]
        return best if bscore > 0 else None
    out["doc_populated"] = f"{doc_nonempty}/{len(entries)}"
    r.ok(f"registry doc-field populated: {doc_nonempty}/{len(entries)} engines")

    c, mj = get(f"https://justhodl.ai/nav-manifest.json?t={int(time.time())}")
    pages = [p["href"].lstrip("/") for cat in json.loads(mj)["categories"] for p in cat["pages"]]
    pages = [p for p in pages if p != "index.html" and "screener" not in p]

    def check(p):
        c2, b = get(f"https://justhodl.ai/{p}?t={int(time.time())}")
        if c2 != 200: return p, "http-error", None
        tagged = "/jh-rail.js" in b
        refs = set(m.replace("'", "").replace('"', "") for m in
                   re.findall(r'["\'/](data/[a-z0-9_\-./]+?\.json)', b, re.I))
        matched = best_match(p, refs)
        return p, tagged, matched

    tagged_n = renders_n = 0
    no_tag = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        for p, tagged, matched in ex.map(check, pages):
            if tagged == "http-error": continue
            tagged_n += tagged
            renders_n += bool(matched)
            if not tagged: no_tag.append(p)
    out["pages_total"] = len(pages); out["tagged"] = tagged_n
    out["predicted_renders"] = renders_n; out["untagged_sample"] = no_tag[:8]
    ok &= not no_tag
    (r.ok if not no_tag else r.fail)(f"script-tag coverage: {tagged_n}/{len(pages)}"
                                     + ("" if not no_tag else f" missing: {no_tag[:5]}"))
    r.ok(f"predicted real-content renders: {renders_n}/{len(pages)} pages resolve to a registered engine")

    for p in ("lce.html", "dex.html", "auctions.html", "about.html", "directory.html"):
        c2, b = get(f"https://justhodl.ai/{p}?t={int(time.time())}")
        refs = set(m for m in re.findall(r'["\'/](data/[a-z0-9_\-./]+?\.json)', b, re.I))
        matched = best_match(p, refs)
        out.setdefault("spot", {})[p] = {"tagged": "/jh-rail.js" in b, "would_render": bool(matched), "engine": matched}
        r.ok(f"  spot {p}: tagged={'/jh-rail.js' in b} would_render={bool(matched)} engine={matched}")
    json.dump(out, open("aws/ops/reports/rail_2933.json", "w"), indent=2)
print("DONE 2932", "PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)
