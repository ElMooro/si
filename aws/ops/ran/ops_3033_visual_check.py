#!/usr/bin/env python3
"""ops 3033 -- exact-match visual live check (markup-only push 8e0243f)."""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report

AWS_DIR = Path(__file__).resolve().parents[2]

with report("3033_visual_check") as rep:
    fails = []
    req = urllib.request.Request(
        "https://justhodl.ai/canaries.html?v=%d" % time.time(),
        headers={"User-Agent": "Mozilla/5.0 ops-3033"})
    page = urllib.request.urlopen(req, timeout=25).read().decode(
        "utf-8", "replace")
    marks = {"header": "EARLY-WARNING WAR ROOM",
             "chip": "CANARY MECHANISMS",
             "wedges": "url(#ndl)",
             "hub": 'radialGradient id="hub"',
             "legend": "RISK LEVEL LEGEND",
             "methodology": "HOW THE BAROMETER WAS BUILT",
             "diamond": "rotate(45deg)"}
    res = {k: (v in page) for k, v in marks.items()}
    rep.kv(**res)
    fails = [k for k, ok in res.items() if not ok]
    payload = {"ops": 3033, "fails": fails, "warns": [],
               "verdict": "FAIL" if fails else "PASS",
               "ts": datetime.now(timezone.utc).isoformat()}
    (AWS_DIR / "ops" / "reports" / "3033.json").write_text(
        json.dumps(payload, indent=1))
    rep.kv(verdict=payload["verdict"])
    if fails:
        rep.log("FAIL: missing %s" % fails)
        sys.exit(1)
    rep.log("PASS -- all mock markers live")
sys.exit(0)
