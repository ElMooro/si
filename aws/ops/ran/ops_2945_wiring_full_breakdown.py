#!/usr/bin/env python3
"""ops 2945 — full breakdown (not a 40-item sample) of the 193 orphaned
engines: separate genuine real-data-no-page gaps from no-outs-declared
infra/feeder engines, with fresh-vs-stale split on the real gap."""
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

with report("2945") as r:
    c, mj = get(f"https://justhodl.ai/nav-manifest.json?t={int(time.time())}")
    pages = [p["href"].lstrip("/") for cat in json.loads(mj)["categories"] for p in cat["pages"]]
    c, rj = get(f"https://justhodl.ai/data/engine-registry.json?t={int(time.time())}")
    reg = json.loads(rj)
    raw = reg.get("engines", reg) if isinstance(reg, dict) else reg
    entries = ({k: {**(v if isinstance(v, dict) else {}), "name": k} for k, v in raw.items()}
               if isinstance(raw, dict) else {e.get("name", str(i)): e for i, e in enumerate(raw)})

    page_src = {}
    def fetch_page(p):
        c2, b = get(f"https://justhodl.ai/{p}?t={int(time.time())}")
        return p, b if c2 == 200 else ""
    with ThreadPoolExecutor(max_workers=14) as ex:
        for p, b in ex.map(fetch_page, pages):
            page_src[p] = b

    import boto3
    s3 = boto3.client("s3")
    def feed_age(key):
        try:
            h = s3.head_object(Bucket="justhodl-dashboard-live", Key=key)
            return round((time.time() - h["LastModified"].timestamp()) / 3600, 1)
        except Exception:
            return None

    real_gap, no_outs = [], []
    for name, e in entries.items():
        outs = e.get("outs") or []
        if not outs:
            no_outs.append(name)
            continue
        if not any(any(o in src for o in outs) for src in page_src.values() if src):
            real_gap.append((name, outs[0]))

    with_ages = [(n, o, feed_age(o)) for n, o in real_gap]
    fresh = sorted([x for x in with_ages if x[2] is not None and x[2] < 24], key=lambda t: t[2])
    stale = sorted([x for x in with_ages if x[2] is not None and x[2] >= 24], key=lambda t: -t[2])
    dead = [x for x in with_ages if x[2] is None]

    r.ok(f"real_gap (has real outs[], zero page anywhere): {len(real_gap)}")
    r.ok(f"  of these: FRESH<24h={len(fresh)}  STALE>=24h={len(stale)}  feed-missing/dead={len(dead)}")
    r.ok(f"no_outs_declared (registry metadata gap or non-display infra): {len(no_outs)}")

    out = {"real_gap_fresh": fresh, "real_gap_stale": stale, "real_gap_dead": dead, "no_outs": no_outs}
    json.dump(out, open("aws/ops/reports/wiring_full_2945.json", "w"), indent=2, default=str)
print("DONE 2945 PASS"); sys.exit(0)
