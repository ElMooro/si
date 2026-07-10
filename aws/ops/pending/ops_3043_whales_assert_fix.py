#!/usr/bin/env python3
"""ops 3043 -- whales assert fix (verify-only). 3041/3042 failed on MY
expectation, not the data: the roster comment's 'Citadel 12,508' counts
raw 13F ROWS (incl option lots); the engine counts DISTINCT equity
symbols -- ~5,970 is correct. Cap-130 rerun produced identical numbers,
proving pagination was never binding. Assert corrected to a
distinct-symbol band; reads the already-fresh whales.json."""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
from ops_report import report

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]

with report("3043_whales_assert_fix") as rep:
    fails, warns = [], []
    d = json.loads(S3.get_object(Bucket=BUCKET,
                                 Key="data/whales.json")["Body"].read())
    whales = d.get("whales") or []
    boards = d.get("boards") or {}
    cit = next((w for w in whales if "Citadel" in w["name"]), None)
    berk = next((w for w in whales if "Berkshire" in w["name"]), None)
    infl = boards.get("whale_inflow_leaders") or []
    outf = boards.get("whale_outflow_leaders") or []
    max_flow = max((abs(r["conviction_flow_usd"])
                    for r in infl[:1] + outf[:1]), default=0)
    rep.kv(quarter=d.get("quarter"), whales_ok=d.get("n_whales_ok"),
           citadel_distinct_symbols=(cit or {}).get("n_positions"),
           berkshire_book=(berk or {}).get("total_value_usd"),
           max_single_flow=max_flow,
           top_inflow=json.dumps([{r["symbol"]: r["conviction_flow_usd"]}
                                  for r in infl[:5]]),
           top_outflow=json.dumps([{r["symbol"]:
                                    r["conviction_flow_usd"]}
                                   for r in outf[:5]]),
           fresh=json.dumps([r["symbol"] for r in
                             (boards.get("fresh_accumulation")
                              or [])[:8]]),
           exits=json.dumps([r["symbol"] for r in
                             (boards.get("full_distribution")
                              or [])[:8]]))
    if not cit or not (3000 <= cit["n_positions"] <= 20000):
        fails.append("Citadel distinct symbols=%s outside sane band"
                     % (cit or {}).get("n_positions"))
    if not berk or not (1.5e11 <= berk["total_value_usd"] <= 5e11):
        fails.append("Berkshire book implausible")
    if max_flow > 30_000_000_000 or max_flow < 100_000_000:
        fails.append("flow magnitude implausible: %s" % max_flow)
    if (d.get("n_whales_ok") or 0) < 30:
        fails.append("whales_ok=%s" % d.get("n_whales_ok"))
    try:
        req = urllib.request.Request(
            "https://justhodl.ai/whales.html?v=%d" % time.time(),
            headers={"User-Agent": "Mozilla/5.0 ops-3043"})
        page = urllib.request.urlopen(req, timeout=25).read().decode(
            "utf-8", "replace")
        rep.kv(page_live="WHALES ARE HOLDING" in page)
        if "WHALES ARE HOLDING" not in page:
            warns.append("page not live")
    except Exception as e:
        warns.append("page: %s" % str(e)[:90])
    payload = {"ops": 3043, "fails": fails, "warns": warns,
               "verdict": "FAIL" if fails else "PASS",
               "quarter": d.get("quarter"),
               "ts": datetime.now(timezone.utc).isoformat()}
    (AWS_DIR / "ops" / "reports" / "3043.json").write_text(
        json.dumps(payload, indent=1))
    rep.kv(verdict=payload["verdict"])
    if fails:
        for f in fails:
            rep.log("FAIL: %s" % f)
        sys.exit(1)
    rep.log("PASS")
sys.exit(0)
