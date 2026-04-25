#!/usr/bin/env python3
"""
Step 153 — Force daily-report-v3 invoke to populate IBIT history NOW.

Step 152 verified the STOCK_TICKERS update deployed but the next
*/5 cron hadn't fired yet. Rather than wait, force-invoke and verify.

Then re-invoke divergence scanner so BTC/Nasdaq + Gold/BTC pairs
process for today.
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


with report("force_ibit_population") as r:
    r.heading("Force daily-report-v3 invoke + verify IBIT history")

    # ─── 1. Sync invoke daily-report-v3 ─────────────────────────────────
    r.section("1. Force-invoke daily-report-v3")
    invoke_start = time.time()
    resp = lam.invoke(
        FunctionName="justhodl-daily-report-v3",
        InvocationType="RequestResponse",
    )
    elapsed = time.time() - invoke_start
    payload = resp.get("Payload").read().decode()
    if resp.get("FunctionError"):
        r.fail(f"  FunctionError ({elapsed:.1f}s): {payload[:600]}")
        # Don't bail — try to verify anyway
    else:
        r.ok(f"  Invoked in {elapsed:.1f}s")

    # ─── 2. Verify IBIT history now populated ───────────────────────────
    r.section("2. Verify IBIT/GBTC/ETHA in data/report.json")
    time.sleep(2)
    obj = s3.get_object(Bucket=BUCKET, Key="data/report.json")
    age_min = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 60
    rpt = json.loads(obj["Body"].read().decode())
    stocks = rpt.get("stocks", {})
    r.log(f"  data/report.json: {obj['ContentLength']:,}B, age {age_min:.1f}min")

    crypto_etfs_status = {}
    for tk in ("IBIT", "GBTC", "ETHA", "FBTC", "ARKB"):
        s = stocks.get(tk, {})
        history = s.get("history", [])
        crypto_etfs_status[tk] = len(history)
        if history:
            r.ok(f"    {tk:6} {len(history)} bars, latest=${s.get('price')}")
        else:
            r.warn(f"    {tk:6} still 0 bars (price={s.get('price')})")

    populated_count = sum(1 for n in crypto_etfs_status.values() if n > 0)

    # ─── 3. Re-invoke divergence scanner ────────────────────────────────
    r.section("3. Re-invoke divergence scanner with new IBIT data")
    invoke_start = time.time()
    resp = lam.invoke(
        FunctionName="justhodl-divergence-scanner",
        InvocationType="RequestResponse",
    )
    elapsed = time.time() - invoke_start
    payload = resp.get("Payload").read().decode()
    if resp.get("FunctionError"):
        r.fail(f"  FunctionError ({elapsed:.1f}s): {payload[:300]}")
    else:
        r.ok(f"  Invoked in {elapsed:.1f}s")
        try:
            outer = json.loads(payload)
            body = json.loads(outer.get("body", "{}"))
            r.log(f"  Response: {body}")
        except Exception:
            pass

    # ─── 4. Verify divergence pairs ─────────────────────────────────────
    r.section("4. Verify BTC/Nasdaq + Gold/BTC processed")
    obj = s3.get_object(Bucket=BUCKET, Key="divergence/current.json")
    snap = json.loads(obj["Body"].read().decode())
    fixed_count = 0
    still_missing = 0
    for rel in snap.get("relationships", []):
        if rel.get("id") in ("btc_nasdaq", "gold_btc"):
            status = rel.get("status")
            if status == "ok":
                r.ok(f"    {rel['name']:25} z={rel.get('z_score'):+.2f} ({rel.get('mispricing')[:60]})")
                fixed_count += 1
            else:
                r.warn(f"    {rel['name']:25} status={status} a_len={rel.get('a_len')} b_len={rel.get('b_len')}")
                still_missing += 1

    n_proc = snap.get("summary", {}).get("n_processed", 0)
    r.log(f"\n  Total processed: {n_proc}/12")
    r.log(f"  BTC pairs fixed: {fixed_count}/2 (was 0/2)")

    r.kv(
        ibit_bars=crypto_etfs_status.get("IBIT", 0),
        gbtc_bars=crypto_etfs_status.get("GBTC", 0),
        etha_bars=crypto_etfs_status.get("ETHA", 0),
        crypto_etfs_populated=f"{populated_count}/5",
        btc_pairs_fixed=fixed_count,
        n_divergence_processed=n_proc,
    )
    r.log("Done")
