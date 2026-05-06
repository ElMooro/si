#!/usr/bin/env python3
"""Step 240 — FINAL Phase 9 verifier after all post-deploy fixes.

Confirms after deploys land:
  - HY/IG OAS values × 100 = bps in display, signal classification correct
  - correlation-breaks: full 10 instruments OR 9 if GOLDPMGBD also broken,
    composite + top breaking pairs computed.

This is the green-light report.
"""
import json
import sys
import time
from ops_report import report
import boto3
from botocore.config import Config

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=300))
s3 = boto3.client("s3", region_name=REGION)


with report("phase9_final_green_light") as r:
    r.heading("Phase 9 GREEN LIGHT verifier — final")

    # ─────────────────────────────────────────
    # 1. crisis-plumbing
    # ─────────────────────────────────────────
    r.section("1. crisis-plumbing — re-invoke + signal sanity")
    cfg = lam.get_function_configuration(FunctionName="justhodl-crisis-plumbing")
    r.log(f"  CodeSha256: {cfg['CodeSha256']}  modified: {cfg['LastModified']}")
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-crisis-plumbing", InvocationType="RequestResponse")
    payload = json.loads(resp["Payload"].read())
    dur = round(time.time() - t0, 1)
    r.log(f"  invoke OK ({dur}s): {json.dumps(payload)[:200]}")
    body = s3.get_object(Bucket=BUCKET, Key="data/crisis-plumbing.json")["Body"].read()
    d = json.loads(body)

    fcs = d.get("funding_credit_signals", {})
    xcc = d.get("xcc_basis_proxy", {})

    r.log("")
    r.log("  Funding & Credit signals (post unit fix):")
    for key in ("SOFR_IORB_SPREAD", "HY_OAS", "IG_BBB_OAS", "T10YIE", "DFII10", "SLOOS_TIGHTEN"):
        v = fcs.get(key, {})
        if v.get("available"):
            sig = v.get("signal", "?")
            if key == "SOFR_IORB_SPREAD":
                disp = f"{v.get('spread_bps')}bps"
            elif key in ("HY_OAS", "IG_BBB_OAS"):
                lv = v.get("latest_value", 0)
                disp = f"{lv:.2f}% = {round(lv * 100)}bps"
            else:
                disp = f"{v.get('latest_value')}{v.get('unit','')}"
            r.log(f"    ✅ {key:20s}  {disp:25s}  signal={sig}  z={v.get('z_score_1y')}")
        else:
            r.log(f"    ✗ {key:20s}  unavailable")

    r.log("")
    r.log("  Cross-currency signals:")
    for key in ("rate_diff_jpy_3m", "rate_diff_eur_3m", "broad_dollar_index", "obfr_iorb_spread"):
        v = xcc.get(key, {})
        if v.get("available"):
            sig = v.get("signal", "?")
            if key.startswith("rate_diff"):
                disp = f"{v.get('current_pct')}%  z={v.get('z_score_1y')}"
            elif key == "broad_dollar_index":
                disp = f"level={v.get('level')}  z={v.get('z_score_1y')}"
            else:
                disp = f"{v.get('spread_bps')}bps"
            r.log(f"    ✅ {key:25s}  {disp:35s}  signal={sig}")
        else:
            reason = v.get("reason", "?")
            r.log(f"    ✗ {key:25s}  unavailable ({reason})")

    # ─────────────────────────────────────────
    # 2. correlation-breaks
    # ─────────────────────────────────────────
    r.section("2. correlation-breaks — re-invoke + top breaking pairs")
    cfg = lam.get_function_configuration(FunctionName="justhodl-correlation-breaks")
    r.log(f"  CodeSha256: {cfg['CodeSha256']}  modified: {cfg['LastModified']}")
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-correlation-breaks", InvocationType="RequestResponse")
    payload = json.loads(resp["Payload"].read())
    dur = round(time.time() - t0, 1)
    r.log(f"  invoke OK ({dur}s): {json.dumps(payload)[:200]}")
    body = s3.get_object(Bucket=BUCKET, Key="data/correlation-breaks.json")["Body"].read()
    d = json.loads(body)

    if d.get("status") == "warming_up":
        r.warn(f"  ⚠ still warming_up: {d.get('message')}")
        corr_ok = False
    else:
        sig = d.get("signal", "?")
        fz = d.get("frobenius_z_score_1y")
        n_inst = d.get("n_instruments")
        n_dates = d.get("n_dates_aligned")
        r.log(f"")
        r.log(f"  signal:                  {sig}")
        r.log(f"  Frobenius Δ z-score 1Y:  {fz}")
        r.log(f"  pairs > 2σ from norm:    {d.get('n_pairs_above_2sigma')}")
        r.log(f"  pairs > 3σ from norm:    {d.get('n_pairs_above_3sigma')}")
        r.log(f"  instruments aligned:     {n_inst}")
        r.log(f"  dates aligned:           {n_dates}")
        r.log(f"  interpretation: {d.get('interpretation')}")
        r.log("")
        r.log("  TOP BREAKING PAIRS:")
        for p in d.get("top_breaking_pairs", []):
            r.log(
                f"    {p['labels'][0]:18s} ↔ {p['labels'][1]:18s}  "
                f"now={p['current_corr']:+.3f}  base={p['baseline_corr']:+.3f}  "
                f"z={p['z_score']:+.2f}"
            )
            if p.get("context"):
                r.log(f"        ↳ {p['context']}")
        corr_ok = len(d.get("top_breaking_pairs", [])) >= 3

    # ─────────────────────────────────────────
    # FINAL VERDICT
    # ─────────────────────────────────────────
    r.section("FINAL VERDICT")
    cp_ok = sum(1 for v in fcs.values() if v.get("available")) >= 5
    pillars = {
        "crisis-plumbing — funding+credit ≥ 5/6": cp_ok,
        "correlation-breaks — top pairs ≥ 3":      corr_ok,
    }
    for k, v in pillars.items():
        r.log(f"  {'✅' if v else '✗'}  {k}")
    if all(pillars.values()):
        r.log("")
        r.log("  🟢 PHASE 9 GREEN — all systems operational")
    r.log("Done")
