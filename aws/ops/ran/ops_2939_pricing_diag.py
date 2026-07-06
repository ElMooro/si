#!/usr/bin/env python3
"""ops 2939 — pinpoint exactly which pricing.html check failed."""
import sys, time, urllib.request
sys.path.insert(0, "aws/ops")
from ops_report import report

def get(u):
    r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent":"Mozilla/5.0 jh"}), timeout=15)
    return r.getcode(), r.read().decode("utf-8", "replace")

with report("2939") as r:
    c, pr = get(f"https://justhodl.ai/pricing.html?t={int(time.time())}")
    r.ok(f"http={c} bytes={len(pr)}")
    r.ok(f"rocket emoji present: {'🚀' in pr}")
    r.ok(f"'lock in' present: {'lock in' in pr.lower()}")
    r.ok(f"'linear-gradient' present: {'linear-gradient' in pr}")
    idx = pr.lower().find("lock")
    r.ok(f"context around 'lock': {pr[max(0,idx-60):idx+80]!r}" if idx >= 0 else "no 'lock' substring anywhere")
print("DONE 2939 PASS"); sys.exit(0)
