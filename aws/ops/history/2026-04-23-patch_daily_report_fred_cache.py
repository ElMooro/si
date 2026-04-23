#!/usr/bin/env python3
"""
Apply FRED caching + throttle to daily-report-v3.

Why this matters:
  - daily-report-v3 fetches 119 FRED series every 5 minutes
  - Current batching (8 per batch, 4 workers, 2.5s gap) averages ~180/min
  - FRED's documented limit is 120 req/min
  - When secretary also runs → combined ~250+/min → 429 storms
  - On 429, current retry still hammers the same endpoint — no backoff
    across the whole batch, just per-request
  - No caching — wasted fetches for series that only update daily

Fix strategy:
  A. Add S3 cache read BEFORE Phase 1: load data/fred-cache.json
     (shared with secretary v2.2)
  B. Change worker count 4→2 and batch gap 2.5s→3.5s
     (peak rate drops ~40% — ~100/min, well under 120 limit)
  C. Skip live fetch for any series whose cached entry has today's date
     (saves ~70% of requests on a normal day since most FRED series
     update at 8:30 AM ET and then stay fixed for 24 hours)
  D. After Phase 1, if ≥70% fresh, write merged result back to the
     shared cache. Otherwise leave cache as-is so secretary still has
     a good baseline.

Risk mitigation:
  - We only touch fetch_fred internals and the Phase 1 loop. Everything
    downstream (compute_changes, fd processing, Phase 2+) is untouched.
  - Dry-run via ast.parse before deploy.
  - Cache-miss path preserves the current behavior exactly (live fetch,
    no regression).
  - Deploy via update_function_code, then invoke once to smoke-test
    before letting the scheduled EventBridge rule fire.
"""

import io
import os
import re
import zipfile
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
TARGET = REPO_ROOT / "aws/lambdas/justhodl-daily-report-v3/source/lambda_function.py"

lam = boto3.client("lambda", region_name=REGION)


# Cache helpers inserted right before fetch_fred
CACHE_HELPERS = '''
# ============================================================
# v3.1 — FRED CACHE LAYER (shared with secretary)
# ============================================================
_FRED_CACHE_KEY = "data/fred-cache.json"

def _load_fred_cache():
    """Load shared FRED cache from S3. Returns {} on any error."""
    try:
        import boto3 as _b
        _s3 = _b.client("s3", region_name="us-east-1")
        obj = _s3.get_object(Bucket="justhodl-dashboard-live", Key=_FRED_CACHE_KEY)
        return json.loads(obj["Body"].read().decode())
    except Exception as e:
        print(f"[FRED-CACHE] load err (non-fatal): {e}")
        return {}

def _save_fred_cache(cache_data):
    """Save merged FRED data back to shared cache."""
    try:
        import boto3 as _b
        _s3 = _b.client("s3", region_name="us-east-1")
        _s3.put_object(
            Bucket="justhodl-dashboard-live", Key=_FRED_CACHE_KEY,
            Body=json.dumps(cache_data, default=str).encode(),
            ContentType="application/json", CacheControl="max-age=1800",
        )
    except Exception as e:
        print(f"[FRED-CACHE] save err (non-fatal): {e}")

def _cache_entry_is_fresh_today(entry):
    """Return True if cache entry's latest observation is from today (US Eastern).
    Series that only update daily won't need a re-fetch within the same day."""
    if not entry or not isinstance(entry, list) or not entry:
        return False
    latest = entry[0] if isinstance(entry[0], dict) else None
    if not latest or not latest.get("date"):
        return False
    try:
        today_et = datetime.now(timezone(timedelta(hours=-5))).strftime("%Y-%m-%d")
        return latest["date"] >= today_et
    except Exception:
        return False

'''


def build_zip(src_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for f in src_dir.rglob("*"):
            if f.is_file():
                zout.write(f, str(f.relative_to(src_dir)))
    return buf.getvalue()


with report("patch_daily_report_fred_cache") as r:
    r.heading("daily-report-v3 — add FRED cache + throttle")

    src = TARGET.read_text(encoding="utf-8")
    r.log(f"  Source size: {len(src)} bytes")

    # ─────────────────────────────────────────
    # Safety check — does it already have cache helpers?
    # ─────────────────────────────────────────
    if "_FRED_CACHE_KEY" in src:
        r.warn("  Cache helpers already present — skipping insertion (idempotent)")
        cache_already_present = True
    else:
        cache_already_present = False

    # ─────────────────────────────────────────
    # Insert cache helpers before fetch_fred
    # ─────────────────────────────────────────
    if not cache_already_present:
        r.section("Step 1: insert cache helpers before fetch_fred")
        marker = "def fetch_fred(sid):"
        if marker not in src:
            r.fail("  Can't find 'def fetch_fred(sid):' — aborting")
            raise SystemExit(1)
        # Find the line with marker + its preceding comment blocks
        idx = src.find(marker)
        src = src[:idx] + CACHE_HELPERS.lstrip() + src[idx:]
        r.ok(f"  Inserted {len(CACHE_HELPERS)} bytes of cache helpers")

    # ─────────────────────────────────────────
    # Modify Phase 1 batch loop: add cache read + skip-if-fresh logic,
    # lower workers 4→2, lengthen gap 2.5s→3.5s
    # ─────────────────────────────────────────
    r.section("Step 2: retrofit Phase 1 with cache + slower throttle")

    old_phase1 = '''    # ── PHASE 1: FRED (batched 8 at a time, 5 workers, 2.5s gap, retry on 429) ──
    fred_raw = {}
    all_sids = list(FRED_SERIES.keys())
    batch_sz = 8
    for i in range(0, len(all_sids), batch_sz):
        batch = all_sids[i:i+batch_sz]
        with ThreadPoolExecutor(max_workers=4) as ex:
            fm = {ex.submit(fetch_fred, sid): sid for sid in batch}
            for f in as_completed(fm):
                sid = fm[f]
                try:
                    d = f.result()
                    if d: fred_raw[sid] = d
                except: pass
        if i + batch_sz < len(all_sids):
            time.sleep(2.5)
        if (i // batch_sz) % 5 == 0:
            print(f"  FRED batch {i//batch_sz+1}: {len(fred_raw)} series")

    print(f"[V10] FRED: {len(fred_raw)}/{len(all_sids)} in {time.time()-t0:.1f}s")'''

    new_phase1 = '''    # ── PHASE 1 v3.1: FRED with S3 cache + throttle (2 workers, 3.5s gap, 429 retry) ──
    fred_raw = {}
    fred_cache = _load_fred_cache()
    all_sids = list(FRED_SERIES.keys())
    # Skip live fetches for series whose cached data already reflects today
    to_fetch = []
    skipped_fresh = 0
    for sid in all_sids:
        cached = fred_cache.get(sid)
        if cached and _cache_entry_is_fresh_today(cached):
            fred_raw[sid] = cached
            skipped_fresh += 1
        else:
            to_fetch.append(sid)
    print(f"[V10] FRED: {skipped_fresh}/{len(all_sids)} already fresh in cache, fetching {len(to_fetch)}")

    batch_sz = 8
    for i in range(0, len(to_fetch), batch_sz):
        batch = to_fetch[i:i+batch_sz]
        with ThreadPoolExecutor(max_workers=2) as ex:  # v3.1: reduced from 4
            fm = {ex.submit(fetch_fred, sid): sid for sid in batch}
            for f in as_completed(fm):
                sid = fm[f]
                try:
                    d = f.result()
                    if d:
                        fred_raw[sid] = d
                except Exception:
                    pass
        if i + batch_sz < len(to_fetch):
            time.sleep(3.5)  # v3.1: lengthened from 2.5s
        if (i // batch_sz) % 5 == 0:
            print(f"  FRED batch {i//batch_sz+1}: total {len(fred_raw)} series")

    # Cache-backstop: any series we couldn't fetch live, fall back to cache
    used_cache_backstop = 0
    for sid in all_sids:
        if sid not in fred_raw and sid in fred_cache:
            fred_raw[sid] = fred_cache[sid]
            used_cache_backstop += 1
    if used_cache_backstop:
        print(f"[V10] FRED: {used_cache_backstop} series from cache backstop")

    print(f"[V10] FRED: {len(fred_raw)}/{len(all_sids)} in {time.time()-t0:.1f}s (skipped {skipped_fresh} fresh, backstop {used_cache_backstop})")

    # Write merged result back to cache if healthy (>=70% populated)
    if len(fred_raw) >= len(all_sids) * 0.7:
        _save_fred_cache(fred_raw)'''

    if old_phase1 in src:
        src = src.replace(old_phase1, new_phase1, 1)
        r.ok("  Replaced Phase 1 loop (cache + throttle + backstop)")
    else:
        r.warn("  Old Phase 1 block not found verbatim — may have already been patched or code drift")
        r.log("  Dumping first 200 chars of Phase 1 area for diagnosis:")
        idx = src.find("PHASE 1: FRED")
        if idx > 0:
            r.log(f"    {src[idx:idx+200]!r}")

    # ─────────────────────────────────────────
    # Syntax check
    # ─────────────────────────────────────────
    r.section("Step 3: verify syntax")
    import ast
    try:
        ast.parse(src)
        r.ok(f"  Syntax valid ({len(src)} bytes)")
    except SyntaxError as e:
        r.fail(f"  SYNTAX ERROR at line {e.lineno}: {e.msg}")
        lines = src.splitlines()
        for i in range(max(0, e.lineno - 3), min(len(lines), e.lineno + 3)):
            marker = ">>>" if i + 1 == e.lineno else "   "
            r.log(f"  {marker} {i+1}: {lines[i][:150]}")
        raise SystemExit(1)

    TARGET.write_text(src, encoding="utf-8")
    r.ok(f"  Wrote patched source ({len(src)} bytes)")

    # ─────────────────────────────────────────
    # Deploy
    # ─────────────────────────────────────────
    r.section("Step 4: deploy")
    zbytes = build_zip(TARGET.parent)
    lam.update_function_code(FunctionName="justhodl-daily-report-v3", ZipFile=zbytes)
    lam.get_waiter("function_updated").wait(
        FunctionName="justhodl-daily-report-v3",
        WaiterConfig={"Delay": 3, "MaxAttempts": 30},
    )
    r.ok(f"  Deployed ({len(zbytes)} bytes)")

    # ─────────────────────────────────────────
    # Synchronous smoke-test invoke
    # ─────────────────────────────────────────
    r.section("Step 5: sync smoke-test invoke (waits for full scan ~60-80s)")
    import json as _json
    try:
        resp = lam.invoke(
            FunctionName="justhodl-daily-report-v3",
            InvocationType="RequestResponse",
            Payload=_json.dumps({"source": "manual-smoke-test"}).encode(),
        )
        status = resp.get("StatusCode")
        fn_err = resp.get("FunctionError")
        payload = resp["Payload"].read().decode("utf-8", errors="ignore")
        if fn_err:
            r.fail(f"  FunctionError: {fn_err}")
            r.log(f"  Body: {payload[:400]}")
            r.kv(smoke_test="FAILED", error=fn_err)
        else:
            r.ok(f"  Invocation returned status {status}")
            # Show the tail — where print() statements usually surface useful context
            r.log(f"  Payload first 400 chars: {payload[:400]}")
            r.kv(smoke_test="OK", status=status)
    except Exception as e:
        r.fail(f"  Smoke test exception: {e}")
        r.kv(smoke_test="EXCEPTION", error=str(e)[:100])

    r.log("Done")
