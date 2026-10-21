"""
justhodl-fmp-ratios
===================

The fleet's FMP EOD/TTM book, refreshed daily: data/fmp-ratios.json (schema fmp-ratios.v1).

Why it exists (ops 6596, 2026-10-21): the book was a one-off harvest (ops 5576, 2026-09-15) with no writer.  Two
things read it and both were silently five weeks stale:
  * chart.html's Valuation / Financials desks (jh-chart-tvsearch.js fmpRow) - labelled "FMP EOD/TTM (2026-09-15)";
  * aws/shared/fmp_book.try_harvest, which every engine's fmp_get / equity_enrich.fmp consults FIRST for
    ratios-ttm / ratios / key-metrics / income-statement on the harvested names (238 engines), so AAPL's P/E in the
    fleet was the September print.

What it does
  1. Universe, in priority order, deduplicated, capped (FMP_RATIOS_MAX, default 700):
       a. the 40 core names of the original harvest (never dropped);
       b. the US-listed names Khalid has actually opened on the chart - data/warm/tv-bars/universe/_index.json
          (NASDAQ: / NYSE: / AMEX: ... symbols banked by justhodl-tv-bars);
       c. the S&P 500 constituents (FMP /stable/sp500-constituent).
  2. One equity_enrich.fetch_financials(ticker) per name (the same ~10 /stable calls the original harvest made:
     profile, income statement, cash flow, ratios-ttm, earnings, balance sheet, segmentation, insiders, price
     history, ratios history), with the harvest book BYPASSED for this engine (otherwise its own stale rows would
     feed back) and a global throttle of ~12 requests/second across 4 workers - well inside FMP's plan limits and
     gentle enough not to trip the provider's hourly 429 storm.
  3. Last-good merge: a name that fails this run keeps its previous row, marked stale_from=<previous generated_at>;
     a run that gets no usable key (key_status != ok) or fewer than half its names writes NOTHING and raises, so the
     previous book stays and the failure is visible in CloudWatch + fleet integrity.
  4. Output: same top-level shape as before (schema_version, source, cadence, label, generated_at, key_status,
     tickers{}) plus engine, universe counts and per-row ticker / as_of.  CacheControl max-age=300 as before.

Schedule: EventBridge Scheduler justhodl-fmp-ratios-daily, cron(38 22 * * ? *) UTC - after FMP's EOD prints
(~21:30 UTC) and inside the fleet's post-close window.  Cadence floor: once a day.  Cost: ~5-8 minutes x 1 GB.
"""
import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import boto3

import fmp_book

# This engine REFRESHES the book: never let equity_enrich read the (previous) book back as live data.
fmp_book.try_harvest = lambda path, qs="": None  # noqa: E731

import equity_enrich as ee  # noqa: E402  (after the bypass)

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/fmp-ratios.json"
UNIVERSE_IDX_KEY = "data/warm/tv-bars/universe/_index.json"
SCHEMA = "fmp-ratios.v1"
ENGINE = "justhodl-fmp-ratios"
MAX_TICKERS = int(os.environ.get("FMP_RATIOS_MAX", "700") or 700)
WORKERS = int(os.environ.get("FMP_RATIOS_WORKERS", "4") or 4)
MIN_GAP_S = float(os.environ.get("FMP_RATIOS_MIN_GAP_S", "0.08") or 0.08)   # ~12 requests/second across all workers
RESERVE_MS = 75_000                                                          # stop submitting new names with this much time left
CORE = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO", "BRK-B", "JPM", "LLY", "V", "UNH", "XOM", "MA", "COST", "HD", "PG", "JNJ", "ABBV",
        "TSM", "ASML", "NFLX", "CRM", "AMD", "ORCL", "ADBE", "KO", "PEP", "WMT", "MRK", "BAC", "CVX", "LIN", "TMO", "MCD", "ACN", "CSCO", "ABT", "GE"]
US_EXCHANGES = {"NASDAQ", "NYSE", "AMEX", "NYSEARCA", "BATS", "CBOE"}
TICKER_RE = re.compile(r"^[A-Z][A-Z0-9]{0,5}([.\-][A-Z]{1,2})?$")

_gate = threading.Lock()
_last = [0.0]
_raw_fmp = ee.fmp


def _throttled_fmp(path, params):
    """Global pacing across the worker threads: at most one FMP request every MIN_GAP_S."""
    with _gate:
        wait = _last[0] + MIN_GAP_S - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        _last[0] = time.monotonic()
    return _raw_fmp(path, params)


ee.fmp = _throttled_fmp


def norm(tk):
    return str(tk or "").strip().upper().replace(".", "-")


def s3_json(key, default):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:  # noqa: BLE001
        return default


def tv_universe():
    """US-listed names Khalid has opened on the chart (banked by justhodl-tv-bars), in index order."""
    idx = s3_json(UNIVERSE_IDX_KEY, {}) or {}
    out = []
    for sym in (idx.get("symbols") or {}):
        if ":" not in str(sym):
            continue
        exch, tk = str(sym).split(":", 1)
        tk = norm(tk)
        if exch.upper() in US_EXCHANGES and TICKER_RE.match(tk):
            out.append(tk)
    return out


def sp500():
    rows = ee.fmp("sp500-constituent", {})
    if not isinstance(rows, list):
        return []
    return [norm(r.get("symbol")) for r in rows if isinstance(r, dict) and r.get("symbol")]


def build_universe():
    tv, spx = tv_universe(), sp500()
    seen, picked = set(), []
    for src in (CORE, tv, spx):
        for tk in src:
            tk = norm(tk)
            if tk and TICKER_RE.match(tk) and tk not in seen:
                seen.add(tk)
                picked.append(tk)
    return picked[:MAX_TICKERS], {"core": len(CORE), "tv_universe": len(tv), "sp500": len(spx), "selected": min(len(picked), MAX_TICKERS), "candidates": len(picked)}


def harvest_one(tk, as_of):
    try:
        row = ee.fetch_financials(tk)
        if not isinstance(row, dict):
            return tk, {"error": "no_row"}
        if not row.get("name") and not row.get("financials") and row.get("pe") is None:
            return tk, {"error": "empty"}
        row["ticker"] = tk
        row["as_of"] = as_of
        return tk, row
    except Exception as e:  # noqa: BLE001
        return tk, {"error": type(e).__name__}


def lambda_handler(event, context):
    t0 = time.monotonic()
    now = datetime.now(timezone.utc)
    as_of = now.isoformat(timespec="seconds")
    previous = s3_json(OUT_KEY, {}) or {}
    prev_rows = previous.get("tickers") if isinstance(previous.get("tickers"), dict) else {}
    prev_gen = previous.get("generated_at")
    universe, counts = build_universe()
    if (event or {}).get("limit"):
        universe = universe[:int(event["limit"])]
    print("[fmp-ratios] universe %s -> %d names; previous book %s with %d rows" % (counts, len(universe), prev_gen, len(prev_rows)))

    def time_left_ms():
        try:
            return context.get_remaining_time_in_millis()
        except Exception:  # noqa: BLE001
            return 900_000 - int((time.monotonic() - t0) * 1000)

    rows, errors, skipped = {}, {}, []
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        pending = []
        for tk in universe:
            if time_left_ms() < RESERVE_MS:
                skipped.append(tk)
                continue
            pending.append(pool.submit(harvest_one, tk, as_of))
            # keep the queue short so the time budget check above actually governs submissions
            while len(pending) >= WORKERS * 2:
                done_f = next((f for f in pending if f.done()), None)
                if done_f is None:
                    time.sleep(0.05)
                    continue
                pending.remove(done_f)
                tk2, row = done_f.result()
                (rows if not row.get("error") else errors)[tk2] = row
        for f in pending:
            tk2, row = f.result()
            (rows if not row.get("error") else errors)[tk2] = row
    key_status = ee.key_status()
    attempted = len(rows) + len(errors)
    print("[fmp-ratios] attempted %d ok %d errors %d skipped(time) %d key_status %s in %.0fs" % (attempted, len(rows), len(errors), len(skipped), json.dumps(key_status), time.monotonic() - t0))
    if key_status.get("status") != "ok":
        raise RuntimeError("FMP key status %s - previous book kept" % json.dumps(key_status))
    if not rows or len(rows) < max(1, attempted // 2):
        raise RuntimeError("only %d of %d names harvested - previous book kept" % (len(rows), attempted))
    # last-good merge: every previous name stays on the book
    kept = 0
    for tk, old in prev_rows.items():
        if tk in rows or not isinstance(old, dict) or old.get("error"):
            continue
        old = dict(old)
        old.setdefault("stale_from", prev_gen)
        rows[tk] = old
        kept += 1
    out = {
        "schema_version": SCHEMA, "source": "FMP", "cadence": "EOD_TTM", "label": "FMP EOD/TTM",
        "engine": ENGINE, "generated_at": as_of, "prev_generated_at": prev_gen,
        "key_status": key_status,
        "universe": dict(counts, attempted=attempted, ok=len(rows) - kept, errors=len(errors), skipped_for_time=len(skipped), kept_previous=kept, total_rows=len(rows)),
        "errors": {tk: e.get("error") for tk, e in sorted(errors.items())[:200]},
        "fetched_in_s": round(time.monotonic() - t0, 1),
        "tickers": rows,
    }
    body = json.dumps(out, default=str, separators=(",", ":")).encode()
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=body, ContentType="application/json", CacheControl="max-age=300")
    print("[fmp-ratios] wrote s3://%s/%s %d bytes, %d rows (%d fresh, %d kept)" % (BUCKET, OUT_KEY, len(body), len(rows), len(rows) - kept, kept))
    return {"ok": True, "rows": len(rows), "fresh": len(rows) - kept, "kept_previous": kept, "errors": len(errors), "skipped_for_time": len(skipped), "bytes": len(body), "key_status": key_status.get("status")}
