#!/usr/bin/env python3
"""ops 2945 — full field-level recon for the 3 genuinely-broken homepage slots:
pump-radar-summary, hedge-book, options-analytics. bake_homepage.py's field
lambdas reference fields these schemas no longer have."""
import json, sys, time, urllib.request
sys.path.insert(0, "aws/ops")
from ops_report import report

def get(u):
    r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent":"Mozilla/5.0 jh"}), timeout=15)
    return json.loads(r.read())

with report("2945") as r:
    pr = get(f"https://justhodl.ai/data/pump-radar-summary.json?t={int(time.time())}")
    r.ok(f"pump-radar: conviction={pr.get('conviction')!r} temperature={pr.get('temperature')!r} "
         f"exec_summary={str(pr.get('executive_summary'))[:150]!r}")
    hb = get(f"https://justhodl.ai/data/hedge-book.json?t={int(time.time())}")
    r.ok(f"hedge-book: scenario_class={hb.get('scenario_class')!r} last_action={hb.get('last_action')!r} "
         f"target_budget_pct={hb.get('target_budget_pct')!r}")
    oa = get(f"https://justhodl.ai/data/options-analytics.json?t={int(time.time())}")
    dist = oa.get("distribution")
    r.ok(f"options-analytics: thesis={str(oa.get('thesis'))[:150]!r} distribution_type={type(dist).__name__} "
         f"distribution={json.dumps(dist)[:200] if dist else None}")
    json.dump({"pump": pr, "hedge": hb, "options": oa}, open("aws/ops/reports/dash_fields_2945.json","w"), indent=1, default=str)
print("DONE 2945 PASS"); sys.exit(0)
