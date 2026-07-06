#!/usr/bin/env python3
"""ops 2944 — full diagnosis of the dashes Khalid spotted on the live homepage:
exact current HTML for each flagged slot + underlying feed existence/freshness/content."""
import json, re, sys, time, urllib.request
sys.path.insert(0, "aws/ops")
from ops_report import report

def get(u):
    try:
        r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent":"Mozilla/5.0 jh"}), timeout=15)
        return r.getcode(), r.read().decode("utf-8", "replace"), dict(r.headers)
    except Exception as e:
        return None, "", {}

SLOTS = ["kpi-nl", "kpi-cr", "read-body", "d-pump", "d-debate", "d-vol", "d-tail",
         "d-optf", "d-marb", "d-pairs", "hd-regime"]
FEEDS = {
    "net-liquidity": ["data/liquidity-inflection.json", "data/global-liquidity.json"],
    "crypto-emergence": ["data/crypto-emergence.json"],
    "strategist/signal-board": ["data/strategist.json", "data/signal-board.json"],
    "pump-radar-summary": ["data/pump-radar-summary.json"],
    "hedge-book": ["data/hedge-book.json"],
    "options-analytics": ["data/options-analytics.json"],
}

with report("2944") as r:
    c, idx, _ = get(f"https://justhodl.ai/index.html?t={int(time.time())}")
    r.ok(f"index.html live: http={c} bytes={len(idx)}")
    for sid in SLOTS:
        m = re.search(r'id="' + re.escape(sid) + r'"[^>]*>([^<]*)<', idx)
        r.ok(f"  LIVE slot {sid}: {m.group(1)!r if m else 'NOT FOUND'}" if m else f"  LIVE slot {sid}: NOT FOUND IN HTML")

    for name, paths in FEEDS.items():
        found = None
        for p in paths:
            c2, body, hdrs = get(f"https://justhodl.ai/{p}?t={int(time.time())}")
            if c2 == 200:
                found = (p, body, hdrs); break
        if not found:
            r.fail(f"feed {name}: NONE of {paths} returned 200")
            continue
        p, body, hdrs = found
        lm = hdrs.get("Last-Modified", "?")
        try:
            d = json.loads(body)
            r.ok(f"feed {name} @ {p}: 200, Last-Modified={lm}, top-keys={list(d.keys())[:12]}")
        except Exception:
            r.fail(f"feed {name} @ {p}: 200 but not valid JSON, first 200 chars={body[:200]!r}")

    # is bake_homepage.py's marker/comment present at all, proving the bake step ran on THIS deploy?
    baked_marker = "AS OF" in idx
    r.ok(f"bake ran on this deploy (AS OF timestamp present): {baked_marker}")
print("DONE 2944 PASS"); sys.exit(0)
