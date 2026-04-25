#!/usr/bin/env python3
"""
Step 181 — Trigger screener async + check Altman Z after wait.

Both Lambdas redeployed. Screener now has scores fetch for Altman Z.
Run two separate things:
  A. Async-invoke screener
  B. Wait 8 min (longer than 5-9 min expected)
  C. Check S3 cache Altman Z coverage

Async invoke = no boto3 timeout fights. Single flush at the end.
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


with report("trigger_screener_check_altman") as r:
    r.heading("Trigger screener async + verify Altman Z")

    # ─── A. Pre-state ───────────────────────────────────────────────────
    r.section("A. Current cache state")
    pre_head = s3.head_object(Bucket=BUCKET, Key=KEY)
    pre_mtime = pre_head["LastModified"]
    pre_obj = s3.get_object(Bucket=BUCKET, Key=KEY)
    pre_cache = json.loads(pre_obj["Body"].read())
    pre_altman = sum(1 for s in pre_cache.get("stocks", []) if s.get("altmanZ") is not None)
    r.log(f"  Pre-mtime: {pre_mtime}")
    r.log(f"  Pre-altmanZ populated: {pre_altman}/{len(pre_cache.get('stocks', []))}")

    # ─── B. Async invoke ────────────────────────────────────────────────
    r.section("B. Async-invoke screener with force=true")
    resp = lam.invoke(
        FunctionName="justhodl-stock-screener",
        InvocationType="Event",
        Payload=json.dumps({"force": True}),
    )
    r.ok(f"  Queued (StatusCode={resp.get('StatusCode')})")

    # ─── C. Single sleep, single check ─────────────────────────────────
    r.section("C. Wait 9 minutes (single block, no polling)")
    t0 = time.time()
    time.sleep(9 * 60)
    r.log(f"  Slept {time.time()-t0:.0f}s")

    # ─── D. Final state ─────────────────────────────────────────────────
    r.section("D. Post state")
    head2 = s3.head_object(Bucket=BUCKET, Key=KEY)
    post_mtime = head2["LastModified"]
    r.log(f"  Post-mtime: {post_mtime}")
    cache_updated = post_mtime > pre_mtime
    r.log(f"  Cache updated: {cache_updated}")

    obj = s3.get_object(Bucket=BUCKET, Key=KEY)
    cached = json.loads(obj["Body"].read())
    stocks = cached.get("stocks", [])
    n = len(stocks)
    altman_n = sum(1 for s in stocks if s.get("altmanZ") is not None)
    sma50_n = sum(1 for s in stocks if s.get("sma50") is not None)
    name_n = sum(1 for s in stocks if s.get("name"))
    pe_n = sum(1 for s in stocks if s.get("peRatio") is not None)

    altman_pct = round(100 * altman_n / max(n, 1), 1)
    sma50_pct = round(100 * sma50_n / max(n, 1), 1)
    name_pct = round(100 * name_n / max(n, 1), 1)
    pe_pct = round(100 * pe_n / max(n, 1), 1)

    r.log(f"")
    r.log(f"  Total stocks: {n}")
    r.log(f"  name:    {name_n}/{n} ({name_pct}%)")
    r.log(f"  peRatio: {pe_n}/{n} ({pe_pct}%)")
    r.log(f"  sma50:   {sma50_n}/{n} ({sma50_pct}%)")
    r.log(f"  altmanZ: {altman_n}/{n} ({altman_pct}%)  ← was 0 before fix")

    if altman_n == 0:
        r.fail(f"\n  ❌ Altman Z still null — fix didn't take")
    elif altman_pct >= 80:
        r.ok(f"\n  ✅ Altman Z populated for {altman_pct}% of stocks")
    else:
        r.warn(f"\n  ⚠ Altman Z partial: {altman_pct}%")

    # Sample
    if altman_n > 0:
        with_altman = [s for s in stocks if s.get("altmanZ") is not None]
        with_altman.sort(key=lambda s: s.get("altmanZ", 0), reverse=True)
        r.log(f"\n  Top 5 highest Altman Z (safest):")
        for s in with_altman[:5]:
            sym = s.get("symbol", "?")
            az = s.get("altmanZ")
            sec = (s.get("sector") or "?")[:18]
            r.log(f"    {sym:6} {sec:20} altmanZ={az:.2f}  Safe")
        r.log(f"\n  Bottom 5 (potential distress):")
        for s in with_altman[-5:]:
            sym = s.get("symbol", "?")
            az = s.get("altmanZ")
            sec = (s.get("sector") or "?")[:18]
            cls = "Safe" if az > 3 else "Grey" if az > 1.81 else "Distress"
            r.log(f"    {sym:6} {sec:20} altmanZ={az:.2f}  {cls}")

    r.kv(
        cache_updated=cache_updated,
        n_stocks=n,
        altman_n=altman_n,
        altman_pct=altman_pct,
        sma50_pct=sma50_pct,
    )
    r.log("Done")
