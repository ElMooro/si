#!/usr/bin/env python3
"""
Step 186 — Force-run screener with corrected Altman endpoint, verify.

The FMP endpoint that ACTUALLY returns Altman Z (confirmed in step 185):
  /stable/financial-scores → altmanZScore field

For AAPL: altmanZScore=10.56 (safe).

Just deployed with the fix. Now:
  A. Wait for code deploy (CodeSha256 changed)
  B. Async-invoke screener with force=true
  C. Sleep 9 min for full run
  D. Check S3 cache — altmanZ should now be populated for ~95%+ stocks
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


with report("verify_altman_with_fixed_endpoint") as r:
    r.heading("Verify Altman Z populated after endpoint fix")

    # ─── A. Wait for deploy ──────────────────────────────────────────────
    r.section("A. Confirm Lambda deployed")
    for i in range(40):
        cfg = lam.get_function_configuration(FunctionName=LAMBDA)
        last = cfg.get("LastModified", "")[:19]
        if "2026-04-26" in last or last >= "2026-04-25T23:55":
            r.ok(f"  Deployed: {last}")
            break
        time.sleep(5)
    else:
        r.warn(f"  Couldn't confirm deploy — using current state")

    # ─── B. Async invoke ────────────────────────────────────────────────
    r.section("B. Async-invoke screener")
    pre_head = s3.head_object(Bucket=BUCKET, Key=KEY)
    pre_mtime = pre_head["LastModified"]
    r.log(f"  Pre-mtime: {pre_mtime}")

    resp = lam.invoke(
        FunctionName=LAMBDA, InvocationType="Event",
        Payload=json.dumps({"force": True}),
    )
    r.ok(f"  Queued (StatusCode={resp.get('StatusCode')})")

    # ─── C. Wait 9 min ──────────────────────────────────────────────────
    r.section("C. Wait 9 min for screener to complete")
    time.sleep(9 * 60)

    # ─── D. Verify ───────────────────────────────────────────────────────
    r.section("D. Coverage check")
    head2 = s3.head_object(Bucket=BUCKET, Key=KEY)
    post_mtime = head2["LastModified"]
    r.log(f"  Post-mtime: {post_mtime}")
    r.log(f"  Cache updated: {post_mtime > pre_mtime}")

    obj = s3.get_object(Bucket=BUCKET, Key=KEY)
    cached = json.loads(obj["Body"].read())
    stocks = cached.get("stocks", [])
    n = len(stocks)
    altman_n = sum(1 for s in stocks if s.get("altmanZ") is not None)
    sma50_n = sum(1 for s in stocks if s.get("sma50") is not None)
    pe_n = sum(1 for s in stocks if s.get("peRatio") is not None)

    altman_pct = round(100 * altman_n / max(n, 1), 1)
    sma50_pct = round(100 * sma50_n / max(n, 1), 1)
    pe_pct = round(100 * pe_n / max(n, 1), 1)

    r.log(f"")
    r.log(f"  Total stocks: {n}")
    r.log(f"  peRatio: {pe_n}/{n} ({pe_pct}%)")
    r.log(f"  sma50:   {sma50_n}/{n} ({sma50_pct}%)")
    r.log(f"  altmanZ: {altman_n}/{n} ({altman_pct}%)  ← was 0")

    if altman_pct >= 90:
        r.ok(f"\n  ✅ Altman Z fix worked — {altman_pct}% coverage")
    elif altman_pct >= 50:
        r.warn(f"\n  ⚠ Partial: {altman_pct}%")
    else:
        r.fail(f"\n  ❌ Still broken: {altman_pct}%")

    # Sample
    if altman_n > 0:
        with_altman = sorted(
            [s for s in stocks if s.get("altmanZ") is not None],
            key=lambda x: x.get("altmanZ", 0),
            reverse=True,
        )
        r.log(f"\n  Top 5 safest:")
        for s in with_altman[:5]:
            r.log(f"    {s['symbol']:6} {(s.get('sector') or '?')[:18]:20} Z={s['altmanZ']:>6.2f}")
        r.log(f"\n  Bottom 5 (potential distress):")
        for s in with_altman[-5:]:
            cls = "Safe" if s["altmanZ"] > 3 else "Grey" if s["altmanZ"] > 1.81 else "Distress"
            r.log(f"    {s['symbol']:6} {(s.get('sector') or '?')[:18]:20} Z={s['altmanZ']:>6.2f}  {cls}")

    r.kv(altman_pct=altman_pct, altman_n=altman_n, n_stocks=n)
    r.log("Done")
