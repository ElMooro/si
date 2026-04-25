#!/usr/bin/env python3
"""
Step 176 — Check screener S3 cache + async re-invoke if stale.

Step 175's force-invoke timed out at 305s on the boto3 client side, but
the Lambda itself has 900s timeout. Two scenarios:
  A. Lambda finished AFTER boto3 gave up → cache is fresh, just verify
  B. Lambda still running → wait for cache update OR re-invoke async

Async invoke returns immediately after queuing — no client timeout
issue. Use that pattern going forward for long screener runs.

This step:
  A. Check screener/data.json LastModified — if recent (since 23:11),
     Lambda finished. Read coverage stats.
  B. If stale, async-invoke. Then poll S3 for up to 12 min watching for
     the cache to update.
  C. Final coverage verdict.
"""
import json
import time
from datetime import datetime, timezone, timedelta
from ops_report import report
import boto3

REGION = "us-east-1"
LAMBDA = "justhodl-stock-screener"
BUCKET = "justhodl-dashboard-live"
KEY = "screener/data.json"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def coverage_stats(stocks):
    sma50 = sum(1 for s in stocks if s.get("sma50") is not None)
    sma200 = sum(1 for s in stocks if s.get("sma200") is not None)
    name = sum(1 for s in stocks if s.get("name"))
    pe = sum(1 for s in stocks if s.get("peRatio") is not None)
    golden = sum(1 for s in stocks if s.get("crossSignal") == "GOLDEN")
    death = sum(1 for s in stocks if s.get("crossSignal") == "DEATH")
    return dict(
        n=len(stocks),
        sma50=sma50, sma200=sma200, name=name, pe=pe,
        golden=golden, death=death,
    )


with report("verify_screener_or_async_rerun") as r:
    r.heading("Verify screener cache or async-rerun if stale")

    # ─── A. Read current S3 cache mtime ─────────────────────────────────
    r.section("A. Current screener/data.json mtime")
    head = s3.head_object(Bucket=BUCKET, Key=KEY)
    cache_mtime = head["LastModified"]
    age = datetime.now(timezone.utc) - cache_mtime
    r.log(f"  LastModified: {cache_mtime.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    r.log(f"  Age: {age.total_seconds()/60:.1f} min")

    # Cutoff: anything since 23:11 UTC (when the rerun started)
    cutoff = datetime(2026, 4, 25, 23, 11, 0, tzinfo=timezone.utc)
    is_fresh = cache_mtime >= cutoff

    if is_fresh:
        r.ok(f"  ✅ Cache updated since rerun started (Lambda completed despite client timeout)")
    else:
        r.warn(f"  ⚠ Cache is stale — Lambda may have failed or still running")

    # ─── B. Read cache + check coverage ─────────────────────────────────
    r.section("B. Coverage check (current cache)")
    obj = s3.get_object(Bucket=BUCKET, Key=KEY)
    cached = json.loads(obj["Body"].read())
    stocks = cached.get("stocks", [])
    s_now = coverage_stats(stocks)

    pct = lambda x: round(100*x/max(s_now['n'],1),1)
    r.log(f"  Total stocks: {s_now['n']}")
    r.log(f"  name:    {s_now['name']:>3}/{s_now['n']} ({pct(s_now['name'])}%)")
    r.log(f"  peRatio: {s_now['pe']:>3}/{s_now['n']} ({pct(s_now['pe'])}%)")
    r.log(f"  sma50:   {s_now['sma50']:>3}/{s_now['n']} ({pct(s_now['sma50'])}%)")
    r.log(f"  sma200:  {s_now['sma200']:>3}/{s_now['n']} ({pct(s_now['sma200'])}%)")
    r.log(f"  GOLDEN:  {s_now['golden']}")
    r.log(f"  DEATH:   {s_now['death']}")

    sma50_pct = pct(s_now['sma50'])
    needs_rerun = sma50_pct < 80

    # ─── C. If still bad coverage, async-invoke + poll ─────────────────
    if not needs_rerun:
        r.ok(f"\n  ✅ SMA coverage at {sma50_pct}% — fix worked, no rerun needed")
        r.kv(
            sma50_pct=sma50_pct,
            sma200_pct=pct(s_now['sma200']),
            n_golden=s_now['golden'],
            n_death=s_now['death'],
            ran_async=False,
        )
        r.log("Done")
        raise SystemExit(0)

    r.section("C. Async-invoke screener (won't block on client timeout)")
    pre_mtime = cache_mtime
    resp = lam.invoke(
        FunctionName=LAMBDA,
        InvocationType="Event",  # async, returns immediately
        Payload=json.dumps({"force": True}),
    )
    status = resp.get("StatusCode")
    r.ok(f"  Async invoke queued (StatusCode={status})")

    # ─── D. Poll for cache update ───────────────────────────────────────
    r.section("D. Poll S3 for cache update (up to 12 min)")
    poll_start = time.time()
    poll_max = 12 * 60
    poll_interval = 30

    while time.time() - poll_start < poll_max:
        time.sleep(poll_interval)
        elapsed = time.time() - poll_start
        try:
            head2 = s3.head_object(Bucket=BUCKET, Key=KEY)
            new_mtime = head2["LastModified"]
            r.log(f"  +{elapsed:.0f}s  cache mtime: {new_mtime.strftime('%H:%M:%S')}")
            if new_mtime > pre_mtime:
                r.ok(f"  ✅ Cache updated after {elapsed:.0f}s")
                break
        except Exception as e:
            r.warn(f"  +{elapsed:.0f}s  head failed: {e}")
    else:
        r.warn(f"  ⚠ Cache didn't update within {poll_max}s — Lambda may have failed")

    # ─── E. Final coverage check ────────────────────────────────────────
    r.section("E. Final coverage check")
    obj = s3.get_object(Bucket=BUCKET, Key=KEY)
    cached2 = json.loads(obj["Body"].read())
    stocks2 = cached2.get("stocks", [])
    s_final = coverage_stats(stocks2)
    pct2 = lambda x: round(100*x/max(s_final['n'],1),1)
    sma50_pct = pct2(s_final['sma50'])

    r.log(f"  Total stocks: {s_final['n']}")
    r.log(f"  sma50:   {s_final['sma50']:>3}/{s_final['n']} ({sma50_pct}%)")
    r.log(f"  sma200:  {s_final['sma200']:>3}/{s_final['n']} ({pct2(s_final['sma200'])}%)")
    r.log(f"  GOLDEN:  {s_final['golden']}")
    r.log(f"  DEATH:   {s_final['death']}")

    if sma50_pct >= 90:
        r.ok(f"\n  ✅ SMA coverage at {sma50_pct}% — fix fully successful")
    elif sma50_pct >= 70:
        r.warn(f"\n  ⚠ SMA coverage at {sma50_pct}% — better but still gaps; may need WORKERS=1")
    else:
        r.fail(f"\n  ❌ SMA coverage at {sma50_pct}% — fix didn't help; need stronger throttle")

    r.kv(
        sma50_pct=sma50_pct,
        sma200_pct=pct2(s_final['sma200']),
        n_golden=s_final['golden'],
        n_death=s_final['death'],
        ran_async=True,
    )
    r.log("Done")
