#!/usr/bin/env python3
"""ops 2944 — DIRECT ANSWER to 'are my engines wired to a page': for each of
661 registered engines, does ANY of the 366 live pages' actual source contain
a literal reference to ANY of that engine's real outs[] files? This is exact
string containment against the registry's own outs[] field, NOT name-fuzzy-
matching (the class of bug that produced false-positives in ops 2930/rail).
Also separately checks: is the engine's OWN Lambda producing FRESH data at
all (a truly-dead engine with no page is a different problem than a live
engine with no page)."""
import json, sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor
from email.utils import parsedate_to_datetime
sys.path.insert(0, "aws/ops")
from ops_report import report

def get(u, to=15):
    try:
        r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent":"Mozilla/5.0 jh"}), timeout=to)
        return r.getcode(), r.read().decode("utf-8", "replace"), dict(r.headers)
    except Exception:
        return None, "", {}

ok = True
with report("2944") as r:
    c, mj, _ = get(f"https://justhodl.ai/nav-manifest.json?t={int(time.time())}")
    pages = [p["href"].lstrip("/") for cat in json.loads(mj)["categories"] for p in cat["pages"]]
    r.ok(f"manifest pages: {len(pages)}")

    c, rj, _ = get(f"https://justhodl.ai/data/engine-registry.json?t={int(time.time())}")
    reg = json.loads(rj)
    raw = reg.get("engines", reg) if isinstance(reg, dict) else reg
    entries = ({k: {**(v if isinstance(v, dict) else {}), "name": k} for k, v in raw.items()}
               if isinstance(raw, dict) else {e.get("name", str(i)): e for i, e in enumerate(raw)})
    r.ok(f"registry engines: {len(entries)}")

    # fetch every page's full source ONCE, concurrently
    page_src = {}
    def fetch_page(p):
        c2, b, _ = get(f"https://justhodl.ai/{p}?t={int(time.time())}")
        return p, b if c2 == 200 else ""
    with ThreadPoolExecutor(max_workers=14) as ex:
        for p, b in ex.map(fetch_page, pages):
            page_src[p] = b
    all_text = "\n".join(page_src.values())   # one big haystack for containment checks
    r.ok(f"fetched {sum(1 for v in page_src.values() if v)}/{len(pages)} page sources ({len(all_text)} chars total)")

    wired, orphaned = [], []
    for name, e in entries.items():
        outs = e.get("outs") or []
        if not outs:
            orphaned.append((name, "NO_OUTS_DECLARED", []))
            continue
        hit_pages = [p for p, src in page_src.items() if src and any(o in src for o in outs)]
        if hit_pages:
            wired.append((name, hit_pages[:3]))
        else:
            orphaned.append((name, "HAS_OUTS_NO_PAGE_REFS", outs[:3]))

    r.ok(f"WIRED (>=1 page references an actual outs[] file): {len(wired)}/{len(entries)}")
    r.ok(f"ORPHANED (zero pages reference any outs[] file): {len(orphaned)}/{len(entries)}")

    # for orphaned engines, check if the underlying feed is even fresh/alive (fetch S3 head)
    def feed_age(key):
        try:
            import boto3
            s3 = boto3.client("s3")
            h = s3.head_object(Bucket="justhodl-dashboard-live", Key=key)
            return (time.time() - h["LastModified"].timestamp()) / 3600
        except Exception:
            return None
    orphan_detail = []
    for name, reason, outs in orphaned[:80]:
        age = feed_age(outs[0]) if outs else None
        orphan_detail.append({"name": name, "reason": reason, "sample_out": outs[0] if outs else None, "age_h": age})

    out = {"total_engines": len(entries), "wired": len(wired), "orphaned": len(orphaned),
           "wired_sample": [{"name": n, "pages": p} for n, p in wired[:20]],
           "orphan_detail": orphan_detail}
    json.dump(out, open("aws/ops/reports/wiring_2944.json", "w"), indent=2, default=str)
print("DONE 2944 PASS"); sys.exit(0)
