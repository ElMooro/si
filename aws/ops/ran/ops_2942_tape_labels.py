#!/usr/bin/env python3
"""ops 2942 — recon: real label strings in market-tape.json before building
mobile-collapse CSS that targets them by exact match."""
import json, sys, time, urllib.request
sys.path.insert(0, "aws/ops")
from ops_report import report

def get(u):
    r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent":"Mozilla/5.0 jh"}), timeout=15)
    return json.loads(r.read())

with report("2942") as r:
    d = get(f"https://justhodl.ai/data/market-tape.json?t={int(time.time())}")
    items = d.get("items", [])
    r.ok(f"labels: {[it.get('label') for it in items]}")
    import json as j
    open("aws/ops/reports/tape_labels_2942.json", "w").write(j.dumps(items, indent=1))
print("DONE 2942 PASS"); sys.exit(0)
