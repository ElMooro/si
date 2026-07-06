#!/usr/bin/env python3
"""ops 2935 — recon for the Proof page: real current values from paper-book,
signal-scorecard, engine-alpha, signal-backtest. No page built yet — evidence first."""
import json, sys, time, urllib.request
sys.path.insert(0, "aws/ops")
from ops_report import report

def get(u):
    try:
        r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent":"Mozilla/5.0 jh"}), timeout=15)
        return r.getcode(), r.read().decode("utf-8", "replace")
    except Exception as e:
        return None, str(e)

CANDIDATES = {
    "paper_book": ["data/paper-book.json", "data/paper-portfolio.json", "data/pm-decision.json"],
    "signal_scorecard": ["data/signal-scorecard.json"],
    "engine_alpha": ["data/engine-alpha.json"],
    "signal_backtest": ["data/signal-backtest.json"],
    "audit": ["data/history-index.json"],
}
out = {}
with report("2935") as r:
    for label, cands in CANDIDATES.items():
        found = None
        for c in cands:
            code, body = get(f"https://justhodl.ai/{c}?t={int(time.time())}")
            if code == 200:
                found = (c, body); break
        if found:
            key, body = found
            try:
                d = json.loads(body)
                out[label] = {"key": key, "sample": json.dumps(d)[:900]}
                r.ok(f"{label}: FOUND at {key} ({len(body)}B)")
            except Exception:
                out[label] = {"key": key, "sample": body[:300]}
                r.ok(f"{label}: FOUND at {key} but not JSON")
        else:
            out[label] = None
            r.fail(f"{label}: NOT FOUND at any of {cands}")
    # also grab the homepage hero markup + nav-manifest PLATFORM category for wiring context
    c, idx = get(f"https://justhodl.ai/index.html?t={int(time.time())}")
    out["hero_snippet"] = idx[idx.find('id="jh-cta"')-200: idx.find('id="jh-cta"')+400] if 'id="jh-cta"' in idx else None
    c, mj = get(f"https://justhodl.ai/nav-manifest.json?t={int(time.time())}")
    man = json.loads(mj)
    plat = next((cat for cat in man["categories"] if "platform" in (cat.get("name","")+cat.get("category","")).lower()), None)
    out["platform_category"] = plat
    r.ok(f"platform category found: {plat.get('name') if plat else None} with {len(plat.get('pages',[])) if plat else 0} pages")
    json.dump(out, open("aws/ops/reports/proof_recon_2935.json", "w"), indent=2, default=str)
print("DONE 2935 PASS"); sys.exit(0)
