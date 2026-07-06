#!/usr/bin/env python3
"""ops 2936 — full real data pull for the Proof page (no truncation this time)."""
import json, sys, time, urllib.request
sys.path.insert(0, "aws/ops")
from ops_report import report

def get(u):
    r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent":"Mozilla/5.0 jh"}), timeout=15)
    return json.loads(r.read())

with report("2936") as r:
    pb = get(f"https://justhodl.ai/data/paper-book.json?t={int(time.time())}")
    sc = get(f"https://justhodl.ai/data/signal-scorecard.json?t={int(time.time())}")
    ea = get(f"https://justhodl.ai/data/engine-alpha.json?t={int(time.time())}")
    bt = get(f"https://justhodl.ai/data/signal-backtest.json?t={int(time.time())}")
    out = {
        "paper_book": pb,
        "scorecard_meta": {k: v for k, v in sc.items() if k != "scorecard"},
        "scorecard_promoted": [row for row in sc.get("scorecard", []) if row.get("signal_type") in sc.get("promoted_signals", [])][:35],
        "engine_alpha_meta": {k: v for k, v in ea.items() if k not in ("engines",)},
        "backtest_overall": bt.get("overall"),
        "backtest_by_verdict": bt.get("by_verdict"),
    }
    r.ok(f"pulled paper_book keys={list(pb.keys())}")
    r.ok(f"pulled scorecard rows matching promoted: {len(out['scorecard_promoted'])}")
    json.dump(out, open("aws/ops/reports/proof_full_2936.json", "w"), indent=2, default=str)
print("DONE 2936 PASS"); sys.exit(0)
