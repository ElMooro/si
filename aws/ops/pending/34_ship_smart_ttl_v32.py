#!/usr/bin/env python3
"""
FRED cache v3.2 — smart TTL based on per-series publishing cadence.

v3.1 problem: cache-hit check was "does latest observation have today's
date?". Most FRED series publish weekly (WALCL every Thursday) or
monthly (CPI once per month), so the latest date is almost never
'today' — cache optimization virtually never triggered.

v3.2 fix: infer publishing cadence from the cache entry itself.
  - Compare the last 2 observation dates
  - Classify: DAILY (1-2 days gap), WEEKLY (5-8), MONTHLY (25-35),
    QUARTERLY (80-100), ANNUAL (350-380)
  - Next expected publication = latest_obs + cadence
  - Skip fetch if now < next_expected + 2 hours buffer
    (FRED publishes at various times, so buffer avoids missing it)

Additionally: track a per-series `_fetched_at` UTC timestamp. If fetched
in the last hour and we're not expecting a new publication soon, skip
regardless of cadence. This handles the case where the 5-min Lambda
cycle re-runs within the same publishing window.

We apply this to BOTH:
  - justhodl-financial-secretary (26 series)
  - justhodl-daily-report-v3 (233 series)

Expected saving: ~90% of FRED fetches on a steady-state day. Scan time
should drop from ~240s → ~30s when cache is warm. 429s effectively
eliminated.

Approach:
  1. Write a single helper function _should_skip_fetch(sid, cache_entry)
     that implements the smart logic
  2. Patch both Lambdas to call it instead of the current same-day check
  3. Also add _fetched_at to cache entries when a fresh fetch completes
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
SEC = REPO_ROOT / "aws/lambdas/justhodl-financial-secretary/source/lambda_function.py"
DR = REPO_ROOT / "aws/lambdas/justhodl-daily-report-v3/source/lambda_function.py"

lam = boto3.client("lambda", region_name=REGION)


# ───────────────────────────────────────────────────────
# Smart TTL helper — drops into both Lambdas
# ───────────────────────────────────────────────────────
SMART_TTL_HELPER = '''def _infer_fred_cadence_days(obs_list):
    """Given a list of FRED observations (dicts with 'date'), infer the
    typical publishing cadence in days. Returns None if not enough data."""
    if not obs_list or len(obs_list) < 2:
        return None
    # Observations are newest-first after reverse-sort in fetch_fred
    try:
        d0 = datetime.strptime(obs_list[0]["date"], "%Y-%m-%d")
        d1 = datetime.strptime(obs_list[1]["date"], "%Y-%m-%d")
        gap = (d0 - d1).days
    except Exception:
        return None
    if gap <= 0:
        return None
    # Look at up to 5 most recent gaps and take median to resist outliers
    gaps = [gap]
    for i in range(1, min(5, len(obs_list) - 1)):
        try:
            a = datetime.strptime(obs_list[i]["date"], "%Y-%m-%d")
            b = datetime.strptime(obs_list[i + 1]["date"], "%Y-%m-%d")
            g = (a - b).days
            if g > 0:
                gaps.append(g)
        except Exception:
            pass
    gaps.sort()
    return gaps[len(gaps) // 2]  # median


def _classify_cadence(days):
    """Round a gap (days) to its canonical cadence."""
    if days is None:
        return "unknown", 1  # treat as daily; we'll re-fetch
    if days <= 3:
        return "daily", 1
    if days <= 10:
        return "weekly", 7
    if days <= 45:
        return "monthly", 30
    if days <= 120:
        return "quarterly", 90
    if days <= 400:
        return "annual", 365
    return "annual+", 365


def _should_skip_fetch(cache_entry):
    """Return (skip_bool, reason_str). Implements smart TTL.

    Three conditions to skip:
      A. We fetched this series < 60 min ago (race-condition shield when
         multiple Lambdas run close together)
      B. Next expected FRED publication is still in the future
         (computed from latest_obs + inferred_cadence)
      C. Latest observation is from today (catch-all for daily series
         that just published)
    """
    if not cache_entry or not isinstance(cache_entry, list) or not cache_entry:
        return False, "no-cache"

    now_utc = datetime.now(timezone.utc)

    # Condition A: very recent fetch
    meta = cache_entry[0].get("_meta") if isinstance(cache_entry[0], dict) else None
    if isinstance(meta, dict):
        fetched_at = meta.get("fetched_at")
        if fetched_at:
            try:
                fa = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
                if (now_utc - fa).total_seconds() < 3600:
                    return True, "recent-fetch"
            except Exception:
                pass

    # Condition B: next publication still in future
    latest = cache_entry[0]
    latest_date_str = latest.get("date") if isinstance(latest, dict) else None
    if not latest_date_str:
        return False, "no-date"
    try:
        latest_date = datetime.strptime(latest_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return False, "bad-date"

    cadence_days = _infer_fred_cadence_days(cache_entry)
    label, canonical = _classify_cadence(cadence_days)
    # Next expected publication (+ 1 day buffer since FRED publishes at various hours)
    next_pub = latest_date + timedelta(days=canonical + 1)
    if now_utc < next_pub:
        return True, f"{label}-not-due-until-{next_pub.date()}"

    # Condition C: same-day catch
    today_et = datetime.now(timezone(timedelta(hours=-5))).strftime("%Y-%m-%d")
    if latest_date_str >= today_et:
        return True, "today"

    return False, "due"

'''


def build_zip(src_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for f in src_dir.rglob("*"):
            if f.is_file():
                zout.write(f, str(f.relative_to(src_dir)))
    return buf.getvalue()


def patch_secretary(r, src):
    """Replace secretary's same-day check with smart TTL."""
    # Insert helper before fetch_fred (or before LIQUIDITY section fallback)
    if "_should_skip_fetch" in src:
        r.log("  helper already present in secretary")
        return src
    # Insert right before FRED_SERIES declaration
    marker = "# ═══ FRED — 26 series ═══"
    if marker not in src:
        r.warn("  secretary: FRED section marker not found")
        return src
    src = src.replace(marker, SMART_TTL_HELPER + "\n\n" + marker, 1)
    r.ok("  secretary: helper inserted")

    # Secretary doesn't use a date-based skip check today — it has no
    # cache-first branch. The cache is only a fallback on 70%-fail.
    # Add a cache-first skip pass inside fetch_fred so secretary also
    # benefits from smart TTL.
    old_loop = '''    # Throttled: 3 workers caps burst ~3 req/s = 180/min — sits near limit.
    # Use 2 for extra headroom when daily-report-v3 is also running.
    with ThreadPoolExecutor(max_workers=2) as ex:
        futs = {ex.submit(_get, sid, nm): sid for sid, nm in FRED_SERIES.items()}
        for f in as_completed(futs):
            f.result()'''
    new_loop = '''    # v3.2 — cache-first skip pass via smart TTL
    try:
        _cache_obj = s3.get_object(Bucket=BUCKET, Key="data/fred-cache.json")
        _fred_cache = json.loads(_cache_obj["Body"].read().decode())
    except Exception as _e:
        _fred_cache = {}
    _to_fetch = []
    _skipped = {"daily": 0, "weekly": 0, "monthly": 0, "quarterly": 0, "annual": 0, "annual+": 0, "recent-fetch": 0, "today": 0, "unknown": 0}
    for sid, nm in FRED_SERIES.items():
        cached = _fred_cache.get(sid)
        if cached:
            # Secretary cache entries are dict-shaped, not list-shaped. Convert
            # list-of-obs → dict-of-summary only applies here, but for the skip
            # check we need the list of observations. Store both when possible.
            obs_list = cached.get("history") if isinstance(cached, dict) else cached
            if obs_list and isinstance(obs_list, list) and obs_list and isinstance(obs_list[0], (int, float)):
                # secretary history is list of floats, no dates — skip check won't work
                obs_list = None
            elif isinstance(cached, list):
                obs_list = cached
            if obs_list:
                skip, reason = _should_skip_fetch(obs_list)
                if skip:
                    # Translate daily-report list format → secretary dict format
                    if isinstance(cached, list) and cached and isinstance(cached[0], dict):
                        latest = cached[0]
                        prev = cached[1] if len(cached) > 1 else latest
                        prev_1m = cached[min(22, len(cached) - 1)] if len(cached) > 22 else latest
                        results[sid] = {
                            "name": nm, "value": latest.get("value"),
                            "prev": prev.get("value"),
                            "chg_1d": round(latest.get("value", 0) - prev.get("value", 0), 4),
                            "chg_1m": round(latest.get("value", 0) - prev_1m.get("value", 0), 4),
                            "date": latest.get("date"),
                            "history": [o.get("value") for o in cached[:30]],
                            "_from_cache": True,
                        }
                    else:
                        results[sid] = dict(cached, _from_cache=True) if isinstance(cached, dict) else cached
                    # Tally reason (short-label: take part before '-')
                    _skipped[reason.split("-")[0] if "-" in reason else reason] = _skipped.get(reason.split("-")[0] if "-" in reason else reason, 0) + 1
                    continue
        _to_fetch.append((sid, nm))
    print(f"[FRED-v3.2] skipped {sum(_skipped.values())} via smart TTL ({_skipped}), fetching {len(_to_fetch)}")

    with ThreadPoolExecutor(max_workers=2) as ex:
        futs = {ex.submit(_get, sid, nm): sid for sid, nm in _to_fetch}
        for f in as_completed(futs):
            f.result()'''
    if old_loop not in src:
        r.warn("  secretary: loop pattern not found (v2.2 signature changed?)")
        return src
    src = src.replace(old_loop, new_loop, 1)
    r.ok("  secretary: smart-TTL skip pass added")
    return src


def patch_daily_report(r, src):
    """Replace daily-report-v3's same-day check with smart TTL."""
    if "_should_skip_fetch" in src:
        r.log("  helper already present in daily-report")
        return src

    # Insert helper before fetch_fred def
    marker = "def fetch_fred(sid):"
    if marker not in src:
        r.fail("  daily-report: fetch_fred marker not found")
        return src
    # Find the exact position
    idx = src.find(marker)
    src = src[:idx] + SMART_TTL_HELPER + "\n\n" + src[idx:]
    r.ok("  daily-report: helper inserted")

    # Replace the old fresh-check with smart TTL.
    # Current code (v3.1):
    old_check = '''    # Skip live fetches for series whose cached data already reflects today
    to_fetch = []
    skipped_fresh = 0
    for sid in all_sids:
        cached = fred_cache.get(sid)
        if cached and _cache_entry_is_fresh_today(cached):
            fred_raw[sid] = cached
            skipped_fresh += 1
        else:
            to_fetch.append(sid)
    print(f"[V10] FRED: {skipped_fresh}/{len(all_sids)} already fresh in cache, fetching {len(to_fetch)}")'''
    new_check = '''    # v3.2 — smart TTL based on inferred publishing cadence
    to_fetch = []
    skip_reasons = {}
    for sid in all_sids:
        cached = fred_cache.get(sid)
        if cached:
            should_skip, reason = _should_skip_fetch(cached)
            if should_skip:
                fred_raw[sid] = cached
                bucket = reason.split("-")[0] if "-" in reason else reason
                skip_reasons[bucket] = skip_reasons.get(bucket, 0) + 1
                continue
        to_fetch.append(sid)
    skipped_fresh = sum(skip_reasons.values())
    print(f"[V10] FRED v3.2: skipped {skipped_fresh} via smart TTL ({skip_reasons}), fetching {len(to_fetch)}")'''
    if old_check not in src:
        r.warn("  daily-report: v3.1 fresh-check block not found (code drift?)")
        return src
    src = src.replace(old_check, new_check, 1)
    r.ok("  daily-report: smart-TTL skip logic wired in")

    # Also update fetch_fred so freshly-fetched observations carry a _meta
    # sentinel with fetched_at. This lets condition A (recent-fetch shield)
    # actually work on the next pass.
    old_fetch = '''                obs = json.loads(resp.read()).get('observations', [])
                out = []
                for o in obs:
                    if o['value'] != '.':
                        try: out.append({'date': o['date'], 'value': float(o['value'])})
                        except: pass
                return out if out else []'''
    new_fetch = '''                obs = json.loads(resp.read()).get('observations', [])
                out = []
                for o in obs:
                    if o['value'] != '.':
                        try: out.append({'date': o['date'], 'value': float(o['value'])})
                        except: pass
                # v3.2 — stamp the first observation with fetched_at for TTL tracking
                if out:
                    out[0]['_meta'] = {'fetched_at': datetime.now(timezone.utc).isoformat()}
                return out if out else []'''
    if old_fetch in src:
        src = src.replace(old_fetch, new_fetch, 1)
        r.ok("  daily-report: fetch_fred stamps _meta.fetched_at")
    else:
        r.warn("  daily-report: fetch_fred body pattern mismatch — _meta stamping skipped")

    return src


with report("ship_smart_ttl_v32") as r:
    r.heading("v3.2 — Smart TTL based on per-series FRED cadence")

    # ─────────────────────────────────────────
    # Patch daily-report-v3 first (higher impact, 233 series)
    # ─────────────────────────────────────────
    r.section("Patch daily-report-v3")
    dr_src = DR.read_text(encoding="utf-8")
    dr_src = patch_daily_report(r, dr_src)

    import ast
    try:
        ast.parse(dr_src)
        r.ok(f"  daily-report syntax valid ({len(dr_src)} bytes)")
    except SyntaxError as e:
        r.fail(f"  daily-report SYNTAX ERROR line {e.lineno}: {e.msg}")
        lines = dr_src.splitlines()
        for i in range(max(0, e.lineno - 3), min(len(lines), e.lineno + 3)):
            mark = ">>>" if i + 1 == e.lineno else "   "
            r.log(f"   {mark} {i+1}: {lines[i][:150]}")
        raise SystemExit(1)
    DR.write_text(dr_src, encoding="utf-8")

    # ─────────────────────────────────────────
    # Patch secretary
    # ─────────────────────────────────────────
    r.section("Patch secretary")
    sec_src = SEC.read_text(encoding="utf-8")
    sec_src = patch_secretary(r, sec_src)
    try:
        ast.parse(sec_src)
        r.ok(f"  secretary syntax valid ({len(sec_src)} bytes)")
    except SyntaxError as e:
        r.fail(f"  secretary SYNTAX ERROR line {e.lineno}: {e.msg}")
        lines = sec_src.splitlines()
        for i in range(max(0, e.lineno - 3), min(len(lines), e.lineno + 3)):
            mark = ">>>" if i + 1 == e.lineno else "   "
            r.log(f"   {mark} {i+1}: {lines[i][:150]}")
        raise SystemExit(1)
    SEC.write_text(sec_src, encoding="utf-8")

    # ─────────────────────────────────────────
    # Deploy both
    # ─────────────────────────────────────────
    r.section("Deploy daily-report-v3")
    z1 = build_zip(DR.parent)
    lam.update_function_code(FunctionName="justhodl-daily-report-v3", ZipFile=z1)
    lam.get_waiter("function_updated").wait(
        FunctionName="justhodl-daily-report-v3",
        WaiterConfig={"Delay": 3, "MaxAttempts": 30},
    )
    r.ok(f"  daily-report-v3 deployed ({len(z1)} bytes)")

    r.section("Deploy secretary")
    z2 = build_zip(SEC.parent)
    lam.update_function_code(FunctionName="justhodl-financial-secretary", ZipFile=z2)
    lam.get_waiter("function_updated").wait(
        FunctionName="justhodl-financial-secretary",
        WaiterConfig={"Delay": 3, "MaxAttempts": 30},
    )
    r.ok(f"  secretary deployed ({len(z2)} bytes)")

    # ─────────────────────────────────────────
    # Async trigger on both — no sync wait (we learned our lesson)
    # ─────────────────────────────────────────
    r.section("Trigger async scans on both")
    import json as _json
    for fn in ("justhodl-daily-report-v3", "justhodl-financial-secretary"):
        resp = lam.invoke(
            FunctionName=fn, InvocationType="Event",
            Payload=_json.dumps({"source": "aws.events"}).encode(),
        )
        r.ok(f"  {fn}: async triggered (status {resp['StatusCode']})")

    r.log("Done — scans will complete in ~2-5 min; verify with a follow-up read")
