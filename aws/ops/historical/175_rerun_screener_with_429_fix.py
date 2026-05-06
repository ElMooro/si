#!/usr/bin/env python3
"""
Step 175 — Bump Lambda timeout, then force-rerun screener with fixed code.

Step 174 confirmed: 70% of all 5 FMP endpoints were getting HTTP 429
because the screener was issuing ~750 req/sec into a ~12 req/sec FMP
budget. Just patched fmp() to retry with backoff (1s, 3s, 9s) on 429,
and reduced workers 5→2 to smooth out the request rate.

Now:
  A. Wait for CI to deploy the new code
  B. Bump Lambda timeout to 900s (15 min Lambda max) — at 2 workers
     with retries, runtime could go 7-12 min
  C. Force-invoke and verify SMA coverage gets back to ~95%+

Math check:
  503 stocks × 5 endpoints = 2515 calls
  At 12 req/sec FMP budget = 210s = 3.5 min for first pass
  + retry overhead from inevitable 429 spikes
  → realistic 5-9 min total
"""
import json
import time
from ops_report import report
import boto3

REGION = "us-east-1"
LAMBDA = "justhodl-stock-screener"
BUCKET = "justhodl-dashboard-live"
KEY = "screener/data.json"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


with report("rerun_screener_with_429_fix") as r:
    r.heading("Force-rerun screener with retry+backoff + 2 workers")

    # ─── A. Verify Lambda has the new code ─────────────────────────────
    r.section("A. Verify Lambda deployment")
    cfg = lam.get_function_configuration(FunctionName=LAMBDA)
    sha = cfg.get("CodeSha256", "?")[:16]
    last_mod = cfg.get("LastModified", "?")[:19]
    cur_timeout = cfg.get("Timeout", 0)
    cur_mem = cfg.get("MemorySize", 0)
    r.log(f"  CodeSha256: {sha}...")
    r.log(f"  LastModified: {last_mod}")
    r.log(f"  Current timeout: {cur_timeout}s, memory: {cur_mem}MB")

    # ─── B. Bump timeout to 900s if not already ─────────────────────────
    r.section("B. Bump timeout to 900s if needed")
    if cur_timeout < 900:
        r.log(f"  Bumping timeout {cur_timeout}s → 900s")
        lam.update_function_configuration(
            FunctionName=LAMBDA,
            Timeout=900,
        )
        # Wait for update to complete
        for _ in range(30):
            cfg2 = lam.get_function_configuration(FunctionName=LAMBDA)
            if cfg2.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(1)
        r.ok(f"  Timeout updated to 900s")
    else:
        r.log(f"  Already at {cur_timeout}s, skip")

    # ─── C. Sync invoke with force=true ─────────────────────────────────
    r.section("C. Force-invoke screener (5-9 min expected)")
    t0 = time.time()
    try:
        resp = lam.invoke(
            FunctionName=LAMBDA,
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
        elapsed = time.time() - t0
        r.fail(f"  Invoke failed after {elapsed:.1f}s: {e}")
        raise SystemExit(1)

    # ─── D. Verify SMA coverage improved ───────────────────────────────
    r.section("D. SMA coverage check")
    obj = s3.get_object(Bucket=BUCKET, Key=KEY)
    cached = json.loads(obj["Body"].read())
    stocks = cached.get("stocks", [])
    r.log(f"  Total stocks: {len(stocks)}")

    sma50_n = sum(1 for s in stocks if s.get("sma50") is not None)
    sma200_n = sum(1 for s in stocks if s.get("sma200") is not None)
    name_n = sum(1 for s in stocks if s.get("name"))
    pe_n = sum(1 for s in stocks if s.get("peRatio") is not None)

    sma50_pct = round(100 * sma50_n / max(len(stocks), 1), 1)
    sma200_pct = round(100 * sma200_n / max(len(stocks), 1), 1)
    name_pct = round(100 * name_n / max(len(stocks), 1), 1)
    pe_pct = round(100 * pe_n / max(len(stocks), 1), 1)

    r.log(f"  Field coverage:")
    r.log(f"    name:    {name_n}/{len(stocks)} ({name_pct}%)")
    r.log(f"    peRatio: {pe_n}/{len(stocks)} ({pe_pct}%)")
    r.log(f"    sma50:   {sma50_n}/{len(stocks)} ({sma50_pct}%)")
    r.log(f"    sma200:  {sma200_n}/{len(stocks)} ({sma200_pct}%)")

    if sma50_pct >= 90:
        r.ok(f"\n  ✅ SMA coverage at {sma50_pct}% — fix worked")
    elif sma50_pct >= 70:
        r.warn(f"\n  ⚠ SMA coverage at {sma50_pct}% — improvement but still gaps")
    else:
        r.fail(f"\n  ❌ SMA coverage at {sma50_pct}% — fix didn't help enough")

    # ─── E. Cross signals ──────────────────────────────────────────────
    golden = [s for s in stocks if s.get("crossSignal") == "GOLDEN"]
    death = [s for s in stocks if s.get("crossSignal") == "DEATH"]

    r.section("E. Cross signal distribution")
    r.log(f"  🟢 GOLDEN: {len(golden)}")
    r.log(f"  🔴 DEATH:  {len(death)}")
    r.log(f"")
    r.log(f"  Most recent GOLDEN crosses:")
    for s in sorted(golden, key=lambda x: x.get("crossDaysAgo") or 999)[:5]:
        sym = s.get("symbol", "?")
        d = s.get("crossDaysAgo")
        sec = (s.get("sector") or "?")[:18]
        r.log(f"    {sym:6} {sec:20} {d:>3}d ago")
    r.log(f"")
    r.log(f"  Most recent DEATH crosses:")
    for s in sorted(death, key=lambda x: x.get("crossDaysAgo") or 999)[:5]:
        sym = s.get("symbol", "?")
        d = s.get("crossDaysAgo")
        sec = (s.get("sector") or "?")[:18]
        r.log(f"    {sym:6} {sec:20} {d:>3}d ago")

    r.kv(
        elapsed_min=round(elapsed/60, 1),
        sma50_pct=sma50_pct,
        sma200_pct=sma200_pct,
        n_golden=len(golden),
        n_death=len(death),
    )
    r.log("Done")
