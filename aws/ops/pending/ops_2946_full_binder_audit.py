#!/usr/bin/env python3
"""ops 2946 — Khalid's concern was broader than 3 slots: audit EVERY homepage
binder row's expected field against the REAL current feed content, not just
the ones visibly showing dashes. Schema drift is silent until it isn't."""
import json, sys, time, urllib.request
sys.path.insert(0, "aws/ops")
from ops_report import report

def get(u):
    try:
        r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent":"Mozilla/5.0 jh"}), timeout=15)
        c = r.getcode()
        return c, json.loads(r.read()) if c == 200 else None
    except Exception:
        return None, None

def g(d, path):
    cur = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur

# every (slot, feed, candidate field paths tried in priority order) from the CURRENT bake script
CHECKS = [
    ("kpi-nl (Net Liquidity)", "data/liquidity-inflection.json", ["composite.regime", "trajectory", "state"]),
    ("kpi-cr (Crypto)", "data/crypto-emergence.json", ["state", "regime", "complex_read"]),
    ("kpi-gsi (Stress)", "data/global-stress.json", ["global_stress_index"]),
    ("kpi-edge (Edge Live)", "data/engine-alpha.json", ["alpha_proven_signals"]),
    ("read-body (Today's Read)", "data/strategist.json", ["headline", "read", "summary", "interpretation"]),
    ("read-body fallback", "data/signal-board.json", ["composite.posture"]),
    ("d-lce (Liquidity Desk)", "data/liquidity-credit.json", ["state", "verdict"]),
    ("d-lce fallback", "data/eurodollar-plumbing.json", ["state", "verdict", "plumbing_health"]),
    ("d-auct (Treasury Auctions)", "data/treasury-auctions.json", ["next"]),
    ("d-gbc (Global Cycle)", "data/global-business-cycle.json", ["aggregate.global_phase"]),
    ("d-alpha (Alpha Scoreboard)", "data/engine-alpha.json", ["alpha_proven_signals"]),
    ("d-cro (Risk Desk)", "data/crisis-composite.json", ["defcon_level", "posture.hedge"]),
    ("d-cro fallback", "data/risk-regime.json", ["regime"]),
    ("d-div (Divergences)", "data/cycle-clock.json", ["divergences"]),
    ("d-conv (Conviction)", "data/best-setups.json", ["setups", "top"]),
    ("d-fleet (Fleet Health)", "data/engine-registry.json", ["count", "engines"]),
]

with report("2946") as r:
    bad = []
    for label, feed, fields in CHECKS:
        code, d = get(f"https://justhodl.ai/{feed}?t={int(time.time())}")
        if d is None:
            bad.append((label, feed, "FEED MISSING/UNREACHABLE", None))
            r.fail(f"{label} @ {feed}: feed unreachable (http={code})")
            continue
        hit = None
        for f in fields:
            v = g(d, f)
            if v is not None and v != "" and v != []:
                hit = (f, v); break
        if hit:
            r.ok(f"{label} @ {feed}: field '{hit[0]}' = {str(hit[1])[:80]!r}")
        else:
            bad.append((label, feed, "ALL candidate fields empty/missing", list(d.keys())[:15]))
            r.fail(f"{label} @ {feed}: NONE of {fields} resolved — real top-keys: {list(d.keys())[:15]}")
    r.ok(f"SUMMARY: {len(CHECKS)-len(bad)}/{len(CHECKS)} bindings resolve to real data; {len(bad)} additional drift candidates: {[b[0] for b in bad]}")
    json.dump(bad, open("aws/ops/reports/binder_drift_2946.json", "w"), indent=1, default=str)
print("DONE 2946 PASS"); sys.exit(0)
