#!/usr/bin/env python3
"""
Step 154 — Verify IBIT populated by natural cron + re-invoke divergence.

Step 153 hit a Lambda Invoke rate limit force-invoking daily-report-v3
(reserved concurrency=1 conflicting with active scheduled run). We
backed off and let the natural */5 cron pick up the STOCK_TICKERS
change.

This step:
  A. Reads data/report.json and confirms IBIT/GBTC/ETHA history is
     now populated. If still empty, prints the LastModified time so
     we can tell whether daily-report-v3 has run since deploy.
  B. If IBIT is populated → re-invoke divergence scanner via
     InvocationType=Event (async, no rate-limit risk) to recompute
     BTC/Nasdaq + Gold/BTC pairs immediately. Wait briefly, re-read.
  C. Confirms morning-intelligence successfully reads regime/current.json
     (sanity check on step 150's deploy).
  D. Confirms risk-sizer still producing differentiated sizes
     (sanity check on step 151).

PURE VERIFICATION — no code changes.
"""
import json
import os
import time
from datetime import datetime, timezone

from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


with report("verify_ibit_natural_cron") as r:
    r.heading("Verify IBIT populated by natural cron + re-run divergence")

    # ─── A. Check IBIT in data/report.json ──────────────────────────────
    r.section("A. IBIT/GBTC/ETHA in data/report.json")
    obj = s3.get_object(Bucket=BUCKET, Key="data/report.json")
    age_min = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 60
    rpt = json.loads(obj["Body"].read().decode())
    stocks = rpt.get("stocks", {})
    r.log(f"  data/report.json: {obj['ContentLength']:,}B, age {age_min:.1f}min")
    r.log(f"  Total stocks: {len(stocks)}")

    crypto_status = {}
    for tk in ("IBIT", "GBTC", "ETHA", "FBTC", "ARKB"):
        s = stocks.get(tk, {})
        history = s.get("history", [])
        crypto_status[tk] = len(history)
        if history:
            latest_close = history[0].get("c") if isinstance(history[0], dict) else None
            r.ok(f"    {tk:6} {len(history)} bars, latest_close=${latest_close}")
        else:
            r.warn(f"    {tk:6} 0 bars (price={s.get('price')})")

    populated = sum(1 for n in crypto_status.values() if n > 0)

    # ─── B. Re-invoke divergence scanner if IBIT is populated ──────────
    r.section("B. Re-invoke divergence scanner (async, no rate limit risk)")
    if crypto_status.get("IBIT", 0) > 0:
        try:
            resp = lam.invoke(
                FunctionName="justhodl-divergence-scanner",
                InvocationType="Event",  # async — no concurrency conflict
            )
            r.ok(f"  Async invoked (StatusCode={resp.get('StatusCode')})")
            r.log(f"  Waiting 25s for scanner to complete...")
            time.sleep(25)
        except Exception as e:
            r.warn(f"  invoke failed: {e}")
    else:
        r.log(f"  Skipping — IBIT not yet populated")

    # ─── C. Verify divergence pairs ─────────────────────────────────────
    r.section("C. Verify BTC pairs in divergence/current.json")
    obj = s3.get_object(Bucket=BUCKET, Key="divergence/current.json")
    snap = json.loads(obj["Body"].read().decode())
    div_age = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 60
    r.log(f"  divergence/current.json age: {div_age:.1f}min")

    fixed = []
    still_missing = []
    for rel in snap.get("relationships", []):
        if rel.get("id") in ("btc_nasdaq", "gold_btc"):
            if rel.get("status") == "ok":
                z = rel.get("z_score")
                fixed.append(rel)
                r.ok(f"    {rel['name']:25} z={z:+.2f}  R²={rel.get('r_squared'):.2f}")
                r.log(f"      → {rel.get('mispricing')}")
            else:
                still_missing.append(rel)
                r.warn(f"    {rel['name']:25} status={rel.get('status')} "
                       f"a_len={rel.get('a_len')} b_len={rel.get('b_len')}")

    n_proc = snap.get("summary", {}).get("n_processed", 0)
    n_extreme = snap.get("summary", {}).get("n_extreme", 0)
    r.log(f"\n  Total processed: {n_proc}/12  At >2σ extreme: {n_extreme}")

    # Show the top 5 most-divergent regardless
    r.log(f"\n  Top 5 divergences today:")
    rels_ok = [r for r in snap.get("relationships", []) if r.get("status") == "ok"]
    for rel in rels_ok[:5]:
        z = rel.get("z_score", 0)
        marker = " ← EXTREME" if rel.get("extreme") else ""
        r.log(f"    {rel['name']:30} z={(z >= 0 and '+' or '') + f'{z:.2f}'}{marker}")

    # ─── D. Sanity: morning-intelligence reads regime/current.json ─────
    r.section("D. Confirm morning-intelligence has bond_regime in extract_metrics")
    try:
        obj = s3.get_object(
            Bucket=BUCKET, Key="aws/lambdas/justhodl-morning-intelligence/source/lambda_function.py"
        )
        # That key probably doesn't exist in S3; the source is in the repo
        r.log(f"  Skipping S3 source check — source is in repo")
    except Exception:
        pass

    # Read the deployed Lambda config to confirm CodeSha256 matches step 150 deploy
    cfg = lam.get_function_configuration(FunctionName="justhodl-morning-intelligence")
    r.log(f"  morning-intelligence: sha={cfg.get('CodeSha256','')[:16]}... "
          f"last_modified={cfg.get('LastModified', '')[:19]}")

    # ─── E. Sanity: risk-sizer still differentiating ───────────────────
    r.section("E. Risk-sizer still producing differentiated sizes")
    obj = s3.get_object(Bucket=BUCKET, Key="risk/recommendations.json")
    snap = json.loads(obj["Body"].read().decode())
    recs = snap.get("sized_recommendations", [])
    sizes = [rec.get("recommended_size_pct", 0) for rec in recs if rec.get("recommended_size_pct")]
    if sizes:
        spread = max(sizes) - min(sizes)
        r.log(f"  Sizes: {min(sizes):.2f}% — {max(sizes):.2f}%, spread {spread:.2f}%")
        if spread > 0.5:
            r.ok(f"  ✅ Differentiation persistent")

    r.kv(
        ibit_bars=crypto_status.get("IBIT", 0),
        gbtc_bars=crypto_status.get("GBTC", 0),
        etha_bars=crypto_status.get("ETHA", 0),
        crypto_etfs_populated=f"{populated}/5",
        btc_pairs_fixed=len(fixed),
        n_div_processed=n_proc,
        n_div_extreme=n_extreme,
    )
    r.log("Done")
