#!/usr/bin/env python3
"""scripts/bake_engine_directory.py — computes the REAL, live status of all
661 registered engines and bakes it into engines.html at deploy time (same
mechanism as bake_right_rail.py: public HTTPS only, no AWS creds needed in
this workflow — Last-Modified header from data/*.json IS the freshness
source, exactly like the rail's own feed-freshness checks).

For each engine: is ANY of its declared outs[] referenced by ANY live page's
actual source (exact string containment, not name-fuzzy-matching)? If yes,
which page(s). If no, is the underlying feed still fresh (engine running,
just invisible) or stale/dead? This directly answers 'is my engine wired to
a page' for the whole fleet in one place, replacing guesswork.
"""
import glob, json, re, sys, time
from concurrent.futures import ThreadPoolExecutor
from email.utils import parsedate_to_datetime

BUCKET = "https://justhodl.ai"


def get(url, to=12):
    import urllib.request
    try:
        r = urllib.request.urlopen(urllib.request.Request(
            url, headers={"User-Agent": "Mozilla/5.0 jh"}), timeout=to)
        return r.getcode(), r.read(), dict(r.headers)
    except Exception:
        return None, b"", {}


def main(build_dir="."):
    c, rb, _ = get(f"{BUCKET}/data/engine-registry.json?t={int(time.time())}")
    reg = json.loads(rb)
    raw = reg.get("engines", reg) if isinstance(reg, dict) else reg
    entries = ({k: {**(v if isinstance(v, dict) else {}), "name": k} for k, v in raw.items()}
               if isinstance(raw, dict) else {e.get("name", str(i)): e for i, e in enumerate(raw)})

    # read every page's ALREADY-ASSEMBLED source in _site (no network needed for this part)
    page_src = {}
    for f in glob.glob(f"{build_dir}/*.html"):
        name = f.split("/")[-1]
        if name in ("engines.html",):
            continue
        page_src[name] = open(f, encoding="utf-8", errors="replace").read()

    unique_feeds = set()
    for e in entries.values():
        unique_feeds.update(e.get("outs") or [])

    def age_of(key):
        c2, _, hdrs = get(f"{BUCKET}/{key}?t={int(time.time())}")
        lm = hdrs.get("Last-Modified") if c2 == 200 else None
        if not lm:
            return None
        try:
            return round((time.time() - parsedate_to_datetime(lm).timestamp()) / 3600, 1)
        except Exception:
            return None

    ages = {}
    with ThreadPoolExecutor(max_workers=16) as ex:
        for k, a in zip(unique_feeds, ex.map(age_of, unique_feeds)):
            ages[k] = a

    rows = []
    for name, e in sorted(entries.items()):
        outs = e.get("outs") or []
        pages = sorted({p for p, src in page_src.items()
                        if any(o in src for o in outs)}) if outs else []
        best_age = min((ages[o] for o in outs if ages.get(o) is not None), default=None)
        if not outs:
            status = "no-outs"
        elif pages:
            status = "wired"
        elif best_age is not None and best_age < 24:
            status = "orphan-fresh"
        elif best_age is not None:
            status = "orphan-stale"
        else:
            status = "orphan-dead"
        rows.append({"name": name, "outs": outs[:3], "pages": pages[:3],
                      "age_h": best_age, "status": status})

    counts = {}
    for r in rows:
        counts[r["status"]] = counts.get(r["status"], 0) + 1

    data = {"generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "total": len(rows), "counts": counts, "rows": rows}

    path = f"{build_dir}/engines.html"
    s = open(path, encoding="utf-8").read()
    marker = "__JH_ENGINE_DATA__"
    if marker not in s:
        print("engines.html: bake marker not found, skipping")
        return
    s = s.replace(marker, json.dumps(data, separators=(",", ":")))
    open(path, "w", encoding="utf-8").write(s)
    print(f"engine directory baked: {len(rows)} engines, counts={counts}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else ".")
