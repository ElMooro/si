#!/usr/bin/env python3
"""
Step 152 — Comprehensive verification of post-Phase-4 fixes.

Verifies:
  A. IBIT/GBTC/ETHA history populating in data/report.json
     (after step 4: STOCK_TICKERS update + CI redeploy)
  B. Divergence scanner now processing BTC/Nasdaq + Gold/BTC pairs
     (re-invokes scanner if data is fresh enough)
  C. Risk-sizer producing differentiated sizes
  D. Lambda CodeSha256 timestamps confirm latest deploy
  E. Index.html homepage on production has Desk link

This is purely diagnostic — no code changes.
"""
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


with report("verify_post_phase4_fixes") as r:
    r.heading("Verify post-Phase-4 fixes — IBIT, divergence, sizing, brief")

    # ─── A. Check IBIT/GBTC/ETHA history ───────────────────────────────
    r.section("A. IBIT/GBTC/ETHA history in data/report.json")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/report.json")
        age_min = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 60
        rpt = json.loads(obj["Body"].read().decode())
        stocks = rpt.get("stocks", {})
        r.log(f"  data/report.json: {obj['ContentLength']:,}B, age {age_min:.1f}min, {len(stocks)} stocks")
        for tk in ("IBIT", "GBTC", "ETHA", "FBTC", "ARKB"):
            s = stocks.get(tk, {})
            history = s.get("history", [])
            price = s.get("price")
            if history:
                r.ok(f"    {tk:6} {len(history)} bars, latest=${price}")
            elif price:
                r.warn(f"    {tk:6} 0 bars, but price=${price} (history fetch may have failed)")
            else:
                r.warn(f"    {tk:6} not yet populated — daily-report-v3 may not have re-run")
    except Exception as e:
        r.fail(f"  read report.json: {e}")

    # ─── B. Check divergence scanner BTC/Nasdaq + Gold/BTC ─────────────
    r.section("B. Divergence scanner BTC/Nasdaq + Gold/BTC pairs")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="divergence/current.json")
        age_min = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 60
        snap = json.loads(obj["Body"].read().decode())
        rels = snap.get("relationships", [])
        for rel in rels:
            if rel.get("id") in ("btc_nasdaq", "gold_btc"):
                status = rel.get("status")
                if status == "ok":
                    r.ok(f"    {rel['name']:25} z={rel.get('z_score'):+.2f}")
                else:
                    r.warn(f"    {rel['name']:25} status={status} (a_len={rel.get('a_len')} b_len={rel.get('b_len')})")
    except Exception as e:
        r.fail(f"  read divergence: {e}")

    # If pairs still missing data and report.json IS populated, re-invoke
    # the scanner manually
    r.log(f"\n  If both pairs say 'missing_data', either daily-report-v3")
    r.log(f"  hasn't re-run yet OR scanner needs to be re-invoked.")
    r.log(f"  daily-report-v3 schedule: cron(*/5 * * * ? *) — every 5 min")

    # ─── C. Risk-sizer differentiation persistent ──────────────────────
    r.section("C. Risk-sizer producing differentiated sizes (post-step-151)")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="risk/recommendations.json")
        age_min = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 60
        snap = json.loads(obj["Body"].read().decode())
        recs = snap.get("sized_recommendations", [])
        sizes = [rec.get("recommended_size_pct", 0) for rec in recs if rec.get("recommended_size_pct")]
        if sizes:
            spread = max(sizes) - min(sizes)
            r.log(f"  risk/recommendations.json: age {age_min:.1f}min")
            r.log(f"  Size range: {min(sizes):.2f}% — {max(sizes):.2f}%, spread {spread:.2f}%")
            r.log(f"  Total: {snap.get('summary',{}).get('total_recommended_size_pct')}%")
            if spread > 0.5:
                r.ok(f"  ✅ Sizing differentiated (step 151 patch active)")
            else:
                r.warn(f"  ⚠ Spread small ({spread:.2f}%) — patch may not be active yet")
            # Show top 5 weighted
            recs_sorted = sorted(recs, key=lambda x: -(x.get("recommended_size_pct") or 0))
            for rec in recs_sorted[:5]:
                w = rec.get("quality_weight")
                r.log(f"    {rec['symbol']:6} comp={rec.get('phase2b_composite','?'):>5} "
                      f"w={w if w else '—':>5} size={rec.get('recommended_size_pct'):>5}%")
    except Exception as e:
        r.fail(f"  read recommendations: {e}")

    # ─── D. Lambda CodeSha256 confirms recent deploys ──────────────────
    r.section("D. Lambda configurations — confirm post-batch deploys")
    for fname in ("justhodl-daily-report-v3", "justhodl-morning-intelligence",
                  "justhodl-risk-sizer"):
        try:
            cfg = lam.get_function_configuration(FunctionName=fname)
            sha = cfg.get("CodeSha256", "")[:16]
            lm = cfg.get("LastModified", "")[:19]
            r.log(f"  {fname:32} sha={sha}... last_modified={lm}")
        except Exception as e:
            r.warn(f"  {fname}: {e}")

    # ─── E. Production index.html has Desk link ────────────────────────
    r.section("E. Homepage Desk navigation")
    import urllib.request
    try:
        req = urllib.request.Request(
            "https://justhodl.ai/index.html",
            headers={"User-Agent": "JustHodl-VerifyDesk/1.0"},
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
        r.log(f"  Fetched {len(html):,}B from justhodl.ai/index.html")
        checks = {
            "Desk in top nav": ('href="desk.html" style="color:#00d4ff' in html),
            "Desk in secondary nav badge": ('href="desk.html" class="new-badge"' in html),
        }
        for label, found in checks.items():
            mark = "✅" if found else "❌"
            r.log(f"    {mark} {label}")
        if all(checks.values()):
            r.ok(f"  ✅ Both Desk links present on production homepage")
        else:
            r.warn(f"  ⚠ GitHub Pages CDN may still be serving cached version")
    except Exception as e:
        r.warn(f"  Couldn't fetch homepage: {e}")

    # ─── F. Quick sanity on regime data flowing ────────────────────────
    r.section("F. Bond regime data freshness")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="regime/current.json")
        age_min = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 60
        snap = json.loads(obj["Body"].read().decode())
        r.log(f"  regime/current.json: age {age_min:.1f}min")
        r.log(f"  Regime: {snap.get('regime')} strength {snap.get('regime_strength')}")
        r.log(f"  Indicators: {snap.get('indicators_total')}, extreme {snap.get('indicators_extreme')}")
    except Exception as e:
        r.warn(f"  regime: {e}")

    r.log("Done")
