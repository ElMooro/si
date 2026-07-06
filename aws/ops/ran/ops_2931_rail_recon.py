#!/usr/bin/env python3
"""ops 2931 — recon for the right-rail template: registry shape, manifest shape,
page->engine correlation, sample desk-page <head>/structure."""
import json, sys, time, urllib.request
sys.path.insert(0, "aws/ops")
from ops_report import report

def get(u, to=15):
    try:
        r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent":"Mozilla/5.0 jh"}), timeout=to)
        return r.getcode(), r.read().decode("utf-8", "replace")
    except Exception as ex:
        return None, str(ex)

out = {}
with report("2931") as r:
    _, rj = get(f"https://justhodl.ai/data/engine-registry.json?t={int(time.time())}")
    reg = json.loads(rj)
    raw = reg.get("engines", reg) if isinstance(reg, dict) else reg
    if isinstance(raw, dict):
        sample = [dict(v, name=k) for k, v in list(raw.items())[:4]]
    else:
        sample = raw[:4]
    out["registry_sample"] = sample
    out["registry_top_keys"] = sorted(reg.keys()) if isinstance(reg, dict) else "list"
    r.ok(f"registry: {len(raw)} entries, sample captured")

    _, mj = get(f"https://justhodl.ai/nav-manifest.json?t={int(time.time())}")
    man = json.loads(mj)
    out["manifest_categories"] = [{"name": c.get("name") or c.get("category"),
                                   "count": len(c.get("pages", [])),
                                   "sample_pages": [p.get("href") for p in c.get("pages", [])[:4]]}
                                  for c in man["categories"]]
    r.ok(f"manifest: {len(man['categories'])} categories")

    for p in ("lce.html", "dex.html", "auctions.html", "about.html", "directory.html"):
        c, b = get(f"https://justhodl.ai/{p}?t={int(time.time())}")
        head = b[:b.lower().find("</head>")] if "</head>" in b.lower() else b[:1200]
        out.setdefault("page_heads", {})[p] = {
            "http": c, "bytes": len(b),
            "has_right_rail_markup": "rail" in b.lower() and ("interpretation" in b.lower() or "feeds into" in b.lower()),
            "title": (b.split("<title>")[1].split("</title>")[0] if "<title>" in b else None),
            "og_desc": (b.split('property="og:description" content="')[1].split('"')[0]
                       if 'property="og:description" content="' in b else None),
            "has_drawer": "jh-nav-drawer" in b, "has_chart_theme": "jh-chart-theme" in b,
        }
    r.ok(f"sample pages inspected: {list(out['page_heads'].keys())}")
    json.dump(out, open("aws/ops/reports/rail_recon_2931.json","w"), indent=2, default=str)
print("DONE 2931 PASS"); sys.exit(0)
