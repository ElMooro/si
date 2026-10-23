"""FMP Ultimate warehouse reader -- S3 first, the provider never, for anything justhodl-fmp-bank has banked.

The bank (aws/lambdas/justhodl-fmp-bank) stores FMP's bulk CSVs verbatim (gzip) under data/warm/fmp-bank/:

    <endpoint>/latest.csv.gz                       snapshot bulks (rating-bulk, dcf-bulk, scores-bulk, ratios-ttm-bulk,
    <endpoint>/part-N/latest.csv.gz                key-metrics-ttm-bulk, price-target-summary-bulk,
                                                   upgrades-downgrades-consensus-bulk, peers-bulk, profile-bulk, etf-holder-bulk)
    <endpoint>/<year>/<period>.csv.gz              statements / growth / ratios / key-metrics / financial-growth bulks
                                                   (period annual | Q1..Q4), earnings-surprises-bulk/<year>/year.csv.gz
    eod-bulk/<year>/<date>.csv.gz                  every listed symbol's close for one session

This module gives consumers the same shapes they got from the per-symbol /stable endpoints (a list of dict rows per
symbol, newest first), read from the bank with a small per-container cache, so the 233 FMP engines can stop paying
~7,000 per-symbol calls a day and all read ONE dated copy of the same number (warehouse-first doctrine, 2026-09-02).
Misses return None so a caller can still fall through to fmp_stable.fmp_get.  Never raises on S3 errors.

Bundle via deploy-lambdas (aws/shared).  No key, no network except S3.
"""
from __future__ import annotations

import csv
import gzip
import io
import json
import time

import boto3

_S3 = boto3.client("s3", region_name="us-east-1")
_BUCKET = "justhodl-dashboard-live"
ROOT = "data/warm/fmp-bank/"
_TTL = 900
_cache: dict = {}
_MAX_CACHED = 24

STATEMENTS = {
    "income-statement": "income-statement-bulk", "balance-sheet-statement": "balance-sheet-statement-bulk",
    "cash-flow-statement": "cash-flow-statement-bulk", "income-statement-growth": "income-statement-growth-bulk",
    "balance-sheet-statement-growth": "balance-sheet-statement-growth-bulk",
    "cash-flow-statement-growth": "cash-flow-statement-growth-bulk", "key-metrics": "key-metrics-bulk",
    "ratios": "ratios-bulk", "financial-growth": "financial-growth-bulk",
}
SNAPSHOTS = {
    "ratios-ttm": "ratios-ttm-bulk", "key-metrics-ttm": "key-metrics-ttm-bulk", "scores": "scores-bulk",
    "financial-scores": "scores-bulk", "rating": "rating-bulk", "ratings-snapshot": "rating-bulk",
    "discounted-cash-flow": "dcf-bulk", "price-target-summary": "price-target-summary-bulk",
    "grades-consensus": "upgrades-downgrades-consensus-bulk", "upgrades-downgrades-consensus": "upgrades-downgrades-consensus-bulk",
    "stock-peers": "peers-bulk", "profile": "profile-bulk",
}


def _get(key):
    now = time.time()
    hit = _cache.get(key)
    if hit and now - hit[0] < _TTL:
        return hit[1]
    try:
        raw = _S3.get_object(Bucket=_BUCKET, Key=key)["Body"].read()
        if key.endswith(".gz"):
            raw = gzip.decompress(raw)
        val = raw
    except Exception:  # noqa: BLE001
        val = None
    if len(_cache) >= _MAX_CACHED:
        _cache.pop(next(iter(_cache)))
    _cache[key] = (now, val)
    return val


def _rows(key):
    raw = _get(key)
    if not raw:
        return None
    try:
        return list(csv.DictReader(io.StringIO(raw.decode("utf-8", "replace"))))
    except Exception:  # noqa: BLE001
        return None


def _num(v):
    if v is None or v == "":
        return None
    try:
        f = float(v)
        return int(f) if f.is_integer() and "." not in str(v) else f
    except (TypeError, ValueError):
        return v


def _typed(row):
    return {k: _num(v) for k, v in row.items()}


def _norm(tk):
    return str(tk or "").upper().strip()


def manifest():
    raw = _get(ROOT + "manifest.json")
    try:
        return json.loads(raw) if raw else {}
    except Exception:  # noqa: BLE001
        return {}


def as_of():
    return manifest().get("updated_at")


def served(endpoint_bulk):
    return endpoint_bulk in (manifest().get("endpoints_served") or [])


def _by_symbol(rows, symbol):
    sym = _norm(symbol)
    alt = sym.replace("-", ".")
    return [_typed(r) for r in rows if _norm(r.get("symbol") or r.get("Symbol")) in (sym, alt)]


def snapshot(path, symbol=None):
    """Latest banked snapshot rows for an endpoint family (ratios-ttm, key-metrics-ttm, scores, rating, dcf,
    price-target-summary, grades-consensus, stock-peers, profile); the whole market when symbol is None."""
    bulk = SNAPSHOTS.get(path)
    if not bulk:
        return None
    rows = _rows(ROOT + bulk + "/latest.csv.gz")
    if rows is None:                                   # parted bulks
        rows = []
        part = 0
        while True:
            r = _rows(ROOT + "%s/part-%d/latest.csv.gz" % (bulk, part))
            if r is None:
                break
            rows.extend(r)
            part += 1
            if part > 40:
                break
        if not rows:
            return None
    if symbol is None:
        return [_typed(r) for r in rows]
    out = _by_symbol(rows, symbol)
    return out or None


def statements(path, symbol, period="annual", limit=None, years=None):
    """History rows for one symbol from the dated bulks, newest first (what /stable/<path>?symbol=&period=&limit= gave).

    period: 'annual' | 'quarter' (Q1..Q4 merged) | 'Q1'..'Q4'.  years: iterable of fiscal years to scan (default: every
    year the bank holds for that endpoint, newest first).  Returns None when the bank holds nothing for the request.
    """
    bulk = STATEMENTS.get(path)
    if not bulk:
        return None
    man = manifest()
    held = sorted({int(k.split("/")[1]) for k in (man.get("dated_keys") or []) if k.startswith(bulk + "/")}, reverse=True)
    if not held:
        held = list(range(int(time.strftime("%Y")), 1984, -1))
    if years is not None:
        held = [y for y in held if y in set(years)]
    periods = ["annual"] if period == "annual" else (["Q1", "Q2", "Q3", "Q4"] if period == "quarter" else [period])
    out = []
    misses = 0
    for y in held:
        got_any = False
        for p in periods:
            rows = _rows(ROOT + "%s/%d/%s.csv.gz" % (bulk, y, p))
            if rows is None:
                continue
            got_any = True
            out.extend(_by_symbol(rows, symbol))
        misses = 0 if got_any else misses + 1
        if misses >= 3 and years is None:           # three consecutive unbanked years: the bank ends here
            break
        if limit and len(out) >= limit:
            break
    if not out:
        return None
    out.sort(key=lambda r: str(r.get("date") or r.get("fiscalYear") or ""), reverse=True)
    return out[:limit] if limit else out


def eod(date_iso, symbol=None):
    """Every listed symbol's close for one session (or one symbol's row)."""
    rows = _rows(ROOT + "eod-bulk/%s/%s.csv.gz" % (date_iso[:4], date_iso))
    if rows is None:
        return None
    if symbol is None:
        return [_typed(r) for r in rows]
    out = _by_symbol(rows, symbol)
    return out[0] if out else None


def try_bank(path, qs=""):
    """Drop-in for fmp_book.try_harvest's contract: answer a /stable path+querystring from the bank or return None.

    Understands symbol=, period=, limit= (the shapes equity_enrich / fmp_stable callers use).  Anything else -> None.
    """
    from urllib.parse import parse_qs
    q = {k: v[0] for k, v in parse_qs(qs or "").items()}
    path = (path or "").strip("/")
    if "/" in path and "symbol" not in q:            # legacy path-style 'ratios/AAPL'
        path, _, q["symbol"] = path.partition("/")
    sym = q.get("symbol")
    if path in STATEMENTS and sym:
        period = q.get("period", "annual")
        if period in ("FY",):
            period = "annual"
        lim = int(q["limit"]) if str(q.get("limit", "")).isdigit() else None
        return statements(path, sym, period=period, limit=lim)
    if path in SNAPSHOTS and sym:
        return snapshot(path, sym)
    return None
