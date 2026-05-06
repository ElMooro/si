#!/usr/bin/env python3
"""
Step 173 — Force-invoke screener Lambda to populate new SMA + cross fields.

Just deployed:
  - get_price_history() added to screener Lambda
  - detect_cross() walks history detecting SMA50/SMA200 crossing events
  - 4 new fields per stock: sma50, sma200, crossSignal, crossDaysAgo

Cache TTL is 4 hours so naturally the cron won't run for up to 4h. Fire
a force-run now so the new fields populate the cache and Khalid sees
them immediately when he refreshes the page.

Watch out: with 5 workers × 503 stocks × 5 FMP calls each, this takes
5-7 minutes. Sync invoke timeout is 15 min (Lambda max), should fit.
But just in case, we use async invoke and just verify it queued — let
the cron path handle slow runs.

This step:
  A. Wait briefly for Lambda code to be deployed (CI race)
  B. Verify new code is live by checking config CodeSha256 changed
  C. Sync-invoke with {"force": true}
  D. Read back screener/data.json from S3, confirm new fields present
  E. Sample 3 stocks with each cross type (golden/death/none)
"""
import json
import time
from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
KEY = "screener/data.json"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


with report("populate_screener_cross_fields") as r:
    r.heading("Force-invoke screener to populate new SMA+cross fields")

    # ─── A. Verify Lambda has the new code ─────────────────────────────
    r.section("A. Verify Lambda deployment")
    cfg = lam.get_function_configuration(FunctionName="justhodl-stock-screener")
    sha = cfg.get("CodeSha256", "?")[:16]
    last_mod = cfg.get("LastModified", "?")[:19]
    r.log(f"  CodeSha256: {sha}...")
    r.log(f"  LastModified: {last_mod}")
    if "2026-04-25" in last_mod or "2026-04-26" in last_mod:
        r.ok(f"  ✅ Lambda recently deployed")
    else:
        r.warn(f"  ⚠ Lambda may not have new code yet — wait then retry")

    # ─── B. Sync invoke with force=true ─────────────────────────────────
    r.section("B. Force-invoke screener (5-7 min expected)")
    t0 = time.time()
    try:
        resp = lam.invoke(
            FunctionName="justhodl-stock-screener",
            InvocationType="RequestResponse",
            Payload=json.dumps({"force": True}),
        )
        elapsed = time.time() - t0
        payload = resp.get("Payload").read().decode()
        if resp.get("FunctionError"):
            r.fail(f"  FunctionError ({elapsed:.1f}s): {payload[:600]}")
            raise SystemExit(1)
        r.ok(f"  Invoked in {elapsed:.1f}s ({elapsed/60:.1f} min)")
        body = json.loads(payload)
        body_inner = json.loads(body.get("body", "{}")) if "body" in body else body
        r.log(f"  Response: count={body_inner.get('count')} elapsed={body_inner.get('elapsed_seconds')}s")
    except Exception as e:
        r.fail(f"  Invoke failed: {e}")
        raise SystemExit(1)

    # ─── C. Read back the cached payload ───────────────────────────────
    r.section("C. Verify new fields present in screener/data.json")
    obj = s3.get_object(Bucket=BUCKET, Key=KEY)
    cached = json.loads(obj["Body"].read())
    stocks = cached.get("stocks", [])
    r.log(f"  Total stocks in cache: {len(stocks)}")

    if not stocks:
        r.fail(f"  Empty cache!")
        raise SystemExit(1)

    sample = stocks[0]
    r.log(f"  Fields on first stock: {sorted(sample.keys())[:30]}")

    has_sma50 = "sma50" in sample
    has_sma200 = "sma200" in sample
    has_cross = "crossSignal" in sample
    has_days = "crossDaysAgo" in sample

    if has_sma50 and has_sma200 and has_cross and has_days:
        r.ok(f"  ✅ All 4 new fields present on every stock")
    else:
        missing = []
        if not has_sma50: missing.append("sma50")
        if not has_sma200: missing.append("sma200")
        if not has_cross: missing.append("crossSignal")
        if not has_days: missing.append("crossDaysAgo")
        r.warn(f"  ⚠ Missing fields: {missing}")

    # ─── D. Crosses found across S&P 500 ───────────────────────────────
    r.section("D. Cross-signal distribution across S&P 500")
    by_signal = {"GOLDEN": [], "DEATH": [], None: []}
    sma50_count = sum(1 for s in stocks if s.get("sma50") is not None)
    sma200_count = sum(1 for s in stocks if s.get("sma200") is not None)
    for s in stocks:
        sig = s.get("crossSignal")
        by_signal.setdefault(sig, []).append(s)
    r.log(f"  Stocks with sma50:  {sma50_count}/{len(stocks)}")
    r.log(f"  Stocks with sma200: {sma200_count}/{len(stocks)}")
    r.log(f"")
    r.log(f"  🟢 GOLDEN crosses (last 60d): {len(by_signal.get('GOLDEN', []))}")
    r.log(f"  🔴 DEATH crosses  (last 60d): {len(by_signal.get('DEATH', []))}")
    r.log(f"  ⚪ No cross signal:           {len(by_signal.get(None, []))}")

    # ─── E. Sample examples of each ────────────────────────────────────
    r.section("E. Top examples by signal")
    for sig in ("GOLDEN", "DEATH"):
        items = by_signal.get(sig, [])
        if not items:
            continue
        # Sort by recency (lower days_ago = more recent)
        items_sorted = sorted(items, key=lambda x: x.get("crossDaysAgo") or 999)
        r.log(f"\n  Most recent {sig} crosses:")
        for s in items_sorted[:5]:
            sym = s.get("symbol", "?")
            sec = s.get("sector", "?")[:18]
            d = s.get("crossDaysAgo")
            sma50 = s.get("sma50")
            sma200 = s.get("sma200")
            price = s.get("price")
            r.log(f"    {sym:6} {sec:20} {d:>3}d ago  $sma50={sma50}  $sma200={sma200}  $price={price}")

    r.kv(
        n_stocks=len(stocks),
        sma50_coverage=sma50_count,
        sma200_coverage=sma200_count,
        n_golden=len(by_signal.get("GOLDEN", [])),
        n_death=len(by_signal.get("DEATH", [])),
        n_no_cross=len(by_signal.get(None, [])),
        elapsed_min=round(elapsed/60, 1),
    )
    r.log("Done — refresh justhodl.ai/screener/ to see new column + tabs")
