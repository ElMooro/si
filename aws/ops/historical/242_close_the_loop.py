#!/usr/bin/env python3
"""Step 242 — final close-the-loop verifier after IG_BBB_OAS + ESTR fix.

Expectation:
  funding_credit_signals: 6/6 populated (IG_BBB_OAS now via BAMLC0A4CBBB)
  xcc_basis_proxy:        4/4 populated (rate_diff_eur_3m now via ESTR)
"""
import json
import time
from ops_report import report
import boto3
from botocore.config import Config

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=300))
s3 = boto3.client("s3", region_name=REGION)

with report("phase9_close_the_loop") as r:
    r.heading("Phase 9 close-the-loop — IG_BBB + ESTR EUR")

    cfg = lam.get_function_configuration(FunctionName="justhodl-crisis-plumbing")
    r.log(f"  CodeSha256: {cfg['CodeSha256']}  modified: {cfg['LastModified']}")
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-crisis-plumbing", InvocationType="RequestResponse")
    payload = json.loads(resp["Payload"].read())
    dur = round(time.time() - t0, 1)
    r.log(f"  invoke OK ({dur}s)")

    body = s3.get_object(Bucket=BUCKET, Key="data/crisis-plumbing.json")["Body"].read()
    d = json.loads(body)
    fcs = d.get("funding_credit_signals", {})
    xcc = d.get("xcc_basis_proxy", {})

    r.section("Funding & Credit Signals (target 6/6)")
    n_fc = 0
    for key in ("SOFR_IORB_SPREAD", "HY_OAS", "IG_BBB_OAS", "T10YIE", "DFII10", "SLOOS_TIGHTEN"):
        v = fcs.get(key, {})
        if v.get("available"):
            n_fc += 1
            sig = v.get("signal", "?")
            if key == "SOFR_IORB_SPREAD":
                disp = f"{v.get('spread_bps')}bps"
            elif key in ("HY_OAS", "IG_BBB_OAS"):
                lv = v.get("latest_value", 0)
                disp = f"{lv:.2f}% = {round(lv * 100)}bps"
            else:
                disp = f"{v.get('latest_value')}{v.get('unit','')}"
            r.log(f"  ✅ {key:20s}  {disp:25s}  signal={sig}  z={v.get('z_score_1y')}")
        else:
            r.log(f"  ✗ {key:20s}  unavailable")

    r.section("Cross-Currency Signals (target 4/4)")
    n_xcc = 0
    for key in ("rate_diff_jpy_3m", "rate_diff_eur_3m", "broad_dollar_index", "obfr_iorb_spread"):
        v = xcc.get(key, {})
        if v.get("available"):
            n_xcc += 1
            sig = v.get("signal", "?")
            if key.startswith("rate_diff"):
                disp = f"{v.get('current_pct')}%  z={v.get('z_score_1y')}"
            elif key == "broad_dollar_index":
                disp = f"level={v.get('level')}  z={v.get('z_score_1y')}"
            else:
                disp = f"{v.get('spread_bps')}bps"
            r.log(f"  ✅ {key:25s}  {disp:35s}  signal={sig}")
        else:
            reason = v.get("reason", "?")
            r.log(f"  ✗ {key:25s}  unavailable ({reason})")

    r.section("FINAL")
    r.log(f"  funding+credit: {n_fc}/6")
    r.log(f"  cross-currency: {n_xcc}/4")
    if n_fc >= 6 and n_xcc >= 4:
        r.log("")
        r.log("  🟢 PHASE 9 — ALL SIGNALS POPULATED, FULLY GREEN")
    elif n_fc >= 5 and n_xcc >= 3:
        r.log("")
        r.log("  🟡 minor gap remaining — see above")
    else:
        r.log("")
        r.log("  ✗ regression — investigate")
    r.log("Done")
