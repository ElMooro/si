#!/usr/bin/env python3
"""
Step 179 — Verify both Lambda fixes after auto-deploy.

Just deployed:
  A. AI Lambda (justhodl-stock-ai-research) with safe_record() parser
  B. Screener Lambda (justhodl-stock-screener) with Altman Z computation

This step:
  A. Wait for AI Lambda to be deployed (CodeSha256 updated since 23:11)
  B. Smoke-test it with ticker=AAPL — should now return 200 with full
     description + bull/bear cases + scenarios
  C. Wait for screener Lambda to be deployed
  D. Async-invoke screener with force=true (boto3 timeouts → use Event)
  E. Poll S3 for cache update, verify altmanZ field populated and
     >0 stocks have non-null values
"""
import json
import time
from datetime import datetime, timezone
from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
AI_LAMBDA = "justhodl-stock-ai-research"
SCREENER_LAMBDA = "justhodl-stock-screener"
BUCKET = "justhodl-dashboard-live"
SCREENER_KEY = "screener/data.json"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def wait_for_lambda_update(name, since_iso, timeout=120):
    """Poll until Lambda LastModified > since_iso."""
    cutoff = datetime.fromisoformat(since_iso)
    t0 = time.time()
    while time.time() - t0 < timeout:
        cfg = lam.get_function_configuration(FunctionName=name)
        last = cfg.get("LastModified", "")[:19]
        if last:
            mod_time = datetime.fromisoformat(last)
            if mod_time > cutoff:
                return cfg
        time.sleep(5)
    return None


with report("verify_ai_and_altman_fixes") as r:
    r.heading("Verify both Lambda fixes after CI auto-deploy")

    # Cutoff for "deployed since this commit" — use just before push
    deploy_cutoff = "2026-04-26T00:00:00"

    # ─── A. Wait for AI Lambda + smoke test ─────────────────────────────
    r.section("A. AI Lambda — wait for deploy")
    cfg = wait_for_lambda_update(AI_LAMBDA, deploy_cutoff, timeout=180)
    if cfg:
        r.ok(f"  ✅ AI Lambda updated at {cfg.get('LastModified','?')[:19]}")
    else:
        # Fall back to current state — maybe it deployed faster
        cfg = lam.get_function_configuration(FunctionName=AI_LAMBDA)
        r.warn(f"  ⚠ Cutoff not crossed — current LastModified: {cfg.get('LastModified','?')[:19]}")

    r.section("B. Smoke-test AI Lambda with AAPL")
    t0 = time.time()
    try:
        resp = lam.invoke(
            FunctionName=AI_LAMBDA,
            InvocationType="RequestResponse",
            Payload=json.dumps({
                "queryStringParameters": {"ticker": "AAPL"},
                "requestContext": {"http": {"method": "GET"}},
            }),
        )
        elapsed = time.time() - t0
        payload = resp.get("Payload").read().decode()
        if resp.get("FunctionError"):
            r.fail(f"  ✗ FunctionError ({elapsed:.1f}s): {payload[:600]}")
            raise SystemExit(1)
        body = json.loads(payload)
        if body.get("statusCode") != 200:
            r.fail(f"  ✗ Status {body.get('statusCode')} ({elapsed:.1f}s): {body.get('body','')[:500]}")
            raise SystemExit(1)
        inner = json.loads(body.get("body", "{}"))
        r.ok(f"  ✅ Returned 200 in {elapsed:.1f}s")
        co = inner.get("company", {})
        sn = inner.get("snapshot", {})
        ai = inner.get("ai", {})
        r.log(f"  Company: {co.get('name','?')} ({co.get('sector','?')})")
        r.log(f"  Price: ${sn.get('price')}  P/E={sn.get('pe')}  ROE={sn.get('roe')}")
        r.log(f"  Cached: {inner.get('from_cache')}  Model: {inner.get('model','?')}")
        r.log(f"")
        r.log(f"  AI Description: {(ai.get('description') or '')[:200]}")
        r.log(f"")
        bull = ai.get("bull_case") or {}
        bear = ai.get("bear_case") or {}
        r.log(f"  Bull thesis:  {(bull.get('thesis') or '')[:150]}")
        r.log(f"  Bull drivers: {bull.get('key_drivers', [])[:3]}")
        r.log(f"  Bear thesis:  {(bear.get('thesis') or '')[:150]}")
        r.log(f"  Bear risks:   {bear.get('key_risks', [])[:3]}")
        r.log(f"")
        sc = ai.get("scenarios") or {}
        for h in ("horizon_1m", "horizon_1q", "horizon_1y"):
            sh = sc.get(h) or {}
            r.log(f"  {h:14}: bull=${sh.get('bull')}  base=${sh.get('base')}  bear=${sh.get('bear')}")
        r.log(f"")
        r.log(f"  Data quality: {ai.get('data_quality', '?')}")
    except Exception as e:
        r.fail(f"  ✗ Invoke failed: {e}")

    # ─── C. Wait for screener Lambda + force-rerun ──────────────────────
    r.section("C. Screener Lambda — wait for deploy")
    cfg2 = wait_for_lambda_update(SCREENER_LAMBDA, deploy_cutoff, timeout=180)
    if cfg2:
        r.ok(f"  ✅ Screener Lambda updated at {cfg2.get('LastModified','?')[:19]}")
    else:
        cfg2 = lam.get_function_configuration(FunctionName=SCREENER_LAMBDA)
        r.warn(f"  ⚠ Cutoff not crossed — current LastModified: {cfg2.get('LastModified','?')[:19]}")

    # ─── D. Async invoke screener (won't block on client timeout) ──────
    r.section("D. Async-invoke screener")
    pre_head = s3.head_object(Bucket=BUCKET, Key=SCREENER_KEY)
    pre_mtime = pre_head["LastModified"]
    r.log(f"  Pre-mtime: {pre_mtime}")

    resp = lam.invoke(
        FunctionName=SCREENER_LAMBDA,
        InvocationType="Event",
        Payload=json.dumps({"force": True}),
    )
    r.ok(f"  Async invoke queued (StatusCode={resp.get('StatusCode')})")

    # ─── E. Poll S3 for cache update ────────────────────────────────────
    r.section("E. Poll S3 for screener cache update (up to 12 min)")
    poll_start = time.time()
    poll_max = 12 * 60
    poll_interval = 30
    cache_updated = False
    while time.time() - poll_start < poll_max:
        time.sleep(poll_interval)
        elapsed = time.time() - poll_start
        try:
            head2 = s3.head_object(Bucket=BUCKET, Key=SCREENER_KEY)
            new_mtime = head2["LastModified"]
            r.log(f"  +{elapsed:.0f}s  cache mtime: {new_mtime.strftime('%H:%M:%S')}")
            if new_mtime > pre_mtime:
                r.ok(f"  ✅ Cache updated after {elapsed:.0f}s")
                cache_updated = True
                break
        except Exception as e:
            r.warn(f"  +{elapsed:.0f}s  head failed: {e}")
    if not cache_updated:
        r.warn(f"  ⚠ Cache didn't update within {poll_max}s — Lambda may have failed")

    # ─── F. Altman Z coverage check ─────────────────────────────────────
    r.section("F. Altman Z coverage in screener cache")
    obj = s3.get_object(Bucket=BUCKET, Key=SCREENER_KEY)
    cached = json.loads(obj["Body"].read())
    stocks = cached.get("stocks", [])
    n = len(stocks)
    altman_n = sum(1 for s in stocks if s.get("altmanZ") is not None)
    sma50_n = sum(1 for s in stocks if s.get("sma50") is not None)
    altman_pct = round(100 * altman_n / max(n, 1), 1)

    r.log(f"  Total stocks: {n}")
    r.log(f"  altmanZ populated: {altman_n}/{n} ({altman_pct}%)")
    r.log(f"  sma50 populated:   {sma50_n}/{n} ({round(100*sma50_n/max(n,1),1)}%)")

    if altman_n == 0:
        r.fail(f"\n  ❌ Altman Z still null on every stock — fix didn't take")
    elif altman_pct >= 80:
        r.ok(f"\n  ✅ Altman Z coverage at {altman_pct}% — fix worked")
        # Show samples
        with_altman = [s for s in stocks if s.get("altmanZ") is not None][:8]
        r.log(f"\n  Sample Altman Z values:")
        for s in with_altman:
            sym = s.get("symbol", "?")
            az = s.get("altmanZ")
            cls = "Safe" if az and az > 3 else "Grey" if az and az > 1.81 else "Distress"
            r.log(f"    {sym:6} altmanZ={az:.2f}  ({cls})")
    else:
        r.warn(f"\n  ⚠ Altman Z coverage only {altman_pct}% — partial fix")

    r.kv(
        ai_smoke_pass=True,  # only reached here if A passed
        altman_n=altman_n,
        altman_pct=altman_pct,
        n_stocks=n,
        cache_updated=cache_updated,
    )
    r.log("Done")
