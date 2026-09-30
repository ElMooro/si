"""justhodl-corporate-actions — corporate-action adjustment pipeline (7/10).

Builds per-ticker backward-adjustment factor series from primary sources
(Polygon splits/dividends/aggs, Yahoo Finance chart events as cross-check)
so charts, screeners, and backtests stop showing fake gaps on splits and
ex-div dates.

Per ticker the Lambda:
  1. Fetches Polygon /v3/reference/splits and /v3/reference/dividends
     (0.3s pacing, fail-soft).
  2. For each cash dividend, fetches ONE unadjusted daily agg for the
     session before ex-date to compute the dividend factor
     (pre_ex_close - cash) / pre_ex_close. Cached in the artifact so
     refetches only happen for new dividends.
  3. Cross-checks event dates/counts against Yahoo chart events=div,split;
     disagreements are recorded, never blocking.
  4. Normalizes events -> builds cumulative factors (descending-date
     running product) -> writes the artifact.

Outputs (S3, bucket from S3_BUCKET env):
  data/corporate-actions/{TICKER}.json
      {ticker, generated_at, schema_version, methodology, events, factors,
       disagreements, factor_gaps}
      factors: ascending [[date, cumulative_factor], ...]
  data/corporate-actions-index.json   — whole-market manifest (counts,
      latest event date, stale flags, agreement rate)
  data/corporate-actions/_state.json  — resume cursor {cursor, updated_at}

Scheduling: daily 06:30 UTC, bounded batch (200 tickers/run), resume via
_state.json, soft time budget so a 300s timeout never loses progress.
All provider calls are fail-soft; a ticker with no Polygon key or no
events still gets an artifact (empty series = factor 1.0 everywhere).
"""

from __future__ import annotations

import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

POLYGON_SPLITS_URL = "https://api.polygon.io/v3/reference/splits"
POLYGON_DIVS_URL = "https://api.polygon.io/v3/reference/dividends"
POLYGON_AGGS_URL = ("https://api.polygon.io/v2/aggs/ticker/{t}"
                    "/range/1/day/{d}/{d}?adjusted=false&limit=5")
YAHOO_CHART_URL = ("https://query1.finance.yahoo.com/v8/finance/chart/{t}"
                   "?events=div,split&range=max")

UNIVERSE_KEY = "data/finviz-universe.json"
ART_PREFIX = "data/corporate-actions/"
INDEX_KEY = "data/corporate-actions-index.json"
STATE_KEY = "data/corporate-actions/_state.json"

BATCH = 200
PACE_S = 0.3
SOFT_BUDGET_S = 250.0
STALE_DAYS = 30
SCHEMA_VERSION = "1.0"
UA = "justhodl.ai/1.0 (corporate-actions; contact: ops@justhodl.ai)"

METHODOLOGY = {
    "basis": "backward-adjusted (latest)",
    "convention": "multiplicative",
    "price_factor": "pre-action OHLC x cumulative factor of actions with ex/effective date > bar_date",
    "volume_factor": "pre-action volume / cumulative factor",
    "split_factor": "split_to / split_from (e.g. 2:1 split -> 0.5; 1:10 reverse split -> 10.0)",
    "dividend_factor": "(pre_ex_close - cash_amount) / pre_ex_close; pre_ex_close from Polygon unadjusted daily agg, up to 3 sessions before ex-date",
    "stock_dividend_factor": "1 / (1 + ratio) when ratio known, else factor_pending",
    "spinoff": "recorded only, excluded from factors (unverifiable_factor: true)",
    "sources": ["polygon.io v3 splits/dividends + v2 aggs", "yahoo finance chart events (cross-check only)"],
}


def _now_iso():
    """Current UTC time as ISO string."""
    return datetime.now(timezone.utc).isoformat()


def _http_get_json(url, timeout=25):
    """GET JSON with a declared User-Agent. Returns dict or None (fail-soft)."""
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, urllib.error.HTTPError,
            ValueError, UnicodeDecodeError, TimeoutError, OSError) as e:
        print("[corpact] http failed %s: %s" % (url.split("?")[0], type(e).__name__))
        return None


def _num(v):
    """Coerce to float, or None when not numeric."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f


def load_universe(s3, bucket):
    """Sorted ticker list from data/finviz-universe.json by_ticker."""
    try:
        obj = s3.get_object(Bucket=bucket, Key=UNIVERSE_KEY)
        uni = json.loads(obj["Body"].read().decode("utf-8"))
        by_ticker = uni.get("by_ticker") or {}
        if not isinstance(by_ticker, dict):
            return []
        return sorted(str(t).upper().strip() for t in by_ticker if str(t).strip())
    except (ClientError, ValueError, UnicodeDecodeError, KeyError, AttributeError) as e:
        print("[corpact] load_universe failed: %s" % type(e).__name__)
        return []


def load_resume_state(s3, bucket):
    """Resume cursor {cursor, updated_at}. Returns {} when absent/bad."""
    try:
        obj = s3.get_object(Bucket=bucket, Key=STATE_KEY)
        st = json.loads(obj["Body"].read().decode("utf-8"))
        return st if isinstance(st, dict) else {}
    except (ClientError, ValueError, UnicodeDecodeError, KeyError, AttributeError):
        return {}


def save_resume_state(s3, bucket, state):
    """Persist resume state. Returns True on success."""
    try:
        s3.put_object(Bucket=bucket, Key=STATE_KEY,
                      Body=json.dumps(state).encode("utf-8"),
                      ContentType="application/json")
        return True
    except (ClientError, TypeError, ValueError) as e:
        print("[corpact] save_resume_state failed: %s" % type(e).__name__)
        return False


def load_prior_artifact(s3, bucket, ticker):
    """Prior per-ticker artifact for event caching, or {} (fail-soft)."""
    try:
        obj = s3.get_object(Bucket=bucket, Key=ART_PREFIX + "%s.json" % ticker)
        doc = json.loads(obj["Body"].read().decode("utf-8"))
        return doc if isinstance(doc, dict) else {}
    except (ClientError, ValueError, UnicodeDecodeError, KeyError, AttributeError):
        return {}


def fetch_polygon_splits(ticker, api_key):
    """Normalized split/spinoff events from Polygon. [] on any failure."""
    if not api_key:
        return []
    data = _http_get_json("%s?ticker=%s&limit=1000&apiKey=%s"
                          % (POLYGON_SPLITS_URL, ticker, api_key))
    events = []
    results = (data or {}).get("results") or []
    if not isinstance(results, list):
        return []
    for r in results:
        if not isinstance(r, dict):
            continue
        ex = str(r.get("execution_date") or "").strip()
        if not ex:
            continue
        fr, to = _num(r.get("split_from")), _num(r.get("split_to"))
        if fr and to and fr > 0 and to > 0:
            events.append({
                "date": ex, "type": "split",
                "ratio_from": fr, "ratio_to": to,
                "factor": to / fr,
                "source": "polygon",
            })
        else:
            # Split-like event with ambiguous ratio: record as spinoff,
            # excluded from the factor product (unverifiable).
            events.append({
                "date": ex, "type": "spinoff",
                "note": "ambiguous split record (from=%r to=%r)" % (r.get("split_from"), r.get("split_to")),
                "unverifiable_factor": True,
                "included_in_factors": False,
                "source": "polygon",
            })
    return events


def fetch_polygon_dividends(ticker, api_key):
    """Normalized dividend events from Polygon (factor computed later)."""
    if not api_key:
        return []
    data = _http_get_json("%s?ticker=%s&limit=1000&apiKey=%s"
                          % (POLYGON_DIVS_URL, ticker, api_key))
    events = []
    results = (data or {}).get("results") or []
    if not isinstance(results, list):
        return []
    for r in results:
        if not isinstance(r, dict):
            continue
        ex = str(r.get("ex_dividend_date") or "").strip()
        if not ex:
            continue
        cash = _num(r.get("cash_amount"))
        dtype = str(r.get("dividend_type") or "").upper()
        if cash and cash > 0:
            events.append({
                "date": ex, "type": "dividend_cash",
                "cash_amount": cash,
                "currency": str(r.get("currency") or "USD"),
                "pay_date": str(r.get("pay_date") or ""),
                "source": "polygon",
            })
        else:
            # Non-cash / unknown dividend: keep as stock-dividend record,
            # factor resolved later when/if a ratio is known.
            events.append({
                "date": ex, "type": "dividend_stock",
                "ratio": None,
                "note": "non-cash dividend (type=%s), ratio unknown" % (dtype or "?"),
                "factor_pending": True,
                "source": "polygon",
            })
    return events


def fetch_pre_ex_close(ticker, ex_date, api_key):
    """Unadjusted close up to 3 sessions before ex_date. None on failure."""
    if not api_key:
        return None
    try:
        base = datetime.strptime(ex_date, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None
    for back in (1, 2, 3):
        d = (base - timedelta(days=back)).isoformat()
        data = _http_get_json(POLYGON_AGGS_URL.format(t=ticker, d=d))
        try:
            results = (data or {}).get("results") or []
            close = _num(results[0].get("c")) if results else None
        except (TypeError, IndexError, AttributeError):
            close = None
        if close and close > 0:
            return close
        time.sleep(PACE_S)
    return None


def resolve_dividend_factors(events, prior_cached, ticker, api_key):
    """Attach pre_ex_close + factor to cash dividends (fail-soft).

    Reuses cached closes from the prior artifact keyed by (date, cash).
    Dividends whose factor can't be computed keep factor_pending: True
    and are excluded from the cumulative product.
    """
    cache = {}
    for e in (prior_cached.get("events") or []):
        if isinstance(e, dict) and e.get("type") == "dividend_cash":
            cache[(e.get("date"), e.get("cash_amount"))] = e.get("pre_ex_close")
    for e in events:
        if e.get("type") != "dividend_cash":
            continue
        key = (e.get("date"), e.get("cash_amount"))
        close = cache.get(key)
        if close is None:
            time.sleep(PACE_S)
            close = fetch_pre_ex_close(ticker, e["date"], api_key)
        e["pre_ex_close"] = close
        cash = e["cash_amount"]
        if close and close > 0 and 0 < cash < close:
            e["factor"] = (close - cash) / close
        else:
            e["factor_pending"] = True
            e["note"] = "factor unavailable (pre_ex_close=%r)" % (close,)
    return events


def fetch_yahoo_events(ticker):
    """Yahoo chart div/split events as {(date, kind)} set. None on failure."""
    data = _http_get_json(YAHOO_CHART_URL.format(t=ticker))
    try:
        result = (data or {}).get("chart", {}).get("result", [])[0]
        ev = result.get("events") or {}
    except (TypeError, IndexError, AttributeError, KeyError):
        return None
    out = set()
    divs = ev.get("dividends") or {}
    for v in divs.values():
        try:
            d = datetime.fromtimestamp(int(v["date"]), tz=timezone.utc).date().isoformat()
            out.add((d, "dividend"))
        except (TypeError, ValueError, KeyError):
            continue
    splits = ev.get("splits") or {}
    for v in splits.values():
        try:
            d = datetime.fromtimestamp(int(v["date"]), tz=timezone.utc).date().isoformat()
            out.add((d, "split"))
        except (TypeError, ValueError, KeyError):
            continue
    return out


def cross_check(poly_events, yahoo_set):
    """Symmetric-difference disagreements + agreement rate. Never raises."""
    poly_set = set()
    for e in poly_events:
        kind = "split" if e.get("type") in ("split", "spinoff") else "dividend"
        poly_set.add((str(e.get("date")), kind))
    disagreements = []
    if yahoo_set is None:
        return disagreements, None
    for item in sorted(poly_set - yahoo_set):
        disagreements.append({"date": item[0], "kind": item[1],
                              "polygon": True, "yahoo": False,
                              "note": "in Polygon, missing in Yahoo"})
    for item in sorted(yahoo_set - poly_set):
        disagreements.append({"date": item[0], "kind": item[1],
                              "polygon": False, "yahoo": True,
                              "note": "in Yahoo, missing in Polygon"})
    total = len(poly_set | yahoo_set)
    rate = round(1.0 - len(disagreements) / total, 4) if total else 1.0
    return disagreements, rate


def dedupe_events(events):
    """Dedupe by (date, type, factor/cash key). Keeps first occurrence."""
    seen, out = set(), []
    for e in events:
        key = (e.get("date"), e.get("type"),
               e.get("factor"), e.get("cash_amount"), e.get("ratio_from"))
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out


def build_factors(events):
    """Ascending [[date, cumulative_factor], ...] via descending running product.

    Only events with a numeric factor and no factor_pending participate.
    cumulative(date) = product of factors of events with event_date > date,
    which is exactly what price_adjust.factor_on() bisects against.
    """
    priced = [e for e in events
              if not e.get("factor_pending")
              and not e.get("unverifiable_factor")
              and _num(e.get("factor")) not in (None, 0)]
    priced.sort(key=lambda e: str(e.get("date") or ""), reverse=True)
    cum, rows = 1.0, []
    for e in priced:
        cum *= float(e["factor"])
        rows.append([str(e["date"]), round(cum, 10)])
    rows.reverse()
    return rows


def write_ticker_artifact(s3, bucket, payload):
    """Write data/corporate-actions/{TICKER}.json. Returns True on success."""
    try:
        s3.put_object(Bucket=bucket,
                      Key=ART_PREFIX + "%s.json" % payload["ticker"],
                      Body=json.dumps(payload, default=str).encode("utf-8"),
                      ContentType="application/json")
        return True
    except (ClientError, TypeError, ValueError, KeyError) as e:
        print("[corpact] artifact write failed for %s: %s"
              % (payload.get("ticker"), type(e).__name__))
        return False


def process_ticker(s3, bucket, ticker, poly_key):
    """Full per-ticker pipeline. Returns (payload, stats). Never raises."""
    gen = _now_iso()
    prior = load_prior_artifact(s3, bucket, ticker)
    try:
        splits = fetch_polygon_splits(ticker, poly_key)
        time.sleep(PACE_S)
        divs = fetch_polygon_dividends(ticker, poly_key)
        time.sleep(PACE_S)
        events = dedupe_events(splits + divs)
        events = resolve_dividend_factors(events, prior, ticker, poly_key)
        time.sleep(PACE_S)
        yahoo_set = fetch_yahoo_events(ticker)
        disagreements, agreement_rate = cross_check(events, yahoo_set)
        factors = build_factors(events)
        factor_gaps = any(e.get("factor_pending") or e.get("unverifiable_factor")
                          for e in events)
        latest = max((str(e.get("date") or "") for e in events), default="")
        payload = {
            "ticker": ticker,
            "generated_at": gen,
            "schema_version": SCHEMA_VERSION,
            "methodology": METHODOLOGY,
            "n_events": len(events),
            "events": sorted(events, key=lambda e: str(e.get("date") or "")),
            "factors": factors,
            "disagreements": disagreements,
            "agreement_rate": agreement_rate,
            "latest_event_date": latest,
            "factor_gaps": factor_gaps,
            "yahoo_checked": yahoo_set is not None,
        }
        ok = write_ticker_artifact(s3, bucket, payload)
        return payload, {"ok": ok}
    except Exception as e:  # noqa: BLE001 - per-ticker isolation; never kill the batch
        print("[corpact] process_ticker %s failed: %s" % (ticker, type(e).__name__))
        return None, {"ok": False}


def _load_prior_manifest(s3, bucket):
    """Prior index tickers dict, or {} — keeps the index whole-market."""
    try:
        obj = s3.get_object(Bucket=bucket, Key=INDEX_KEY)
        doc = json.loads(obj["Body"].read().decode("utf-8"))
        tickers = doc.get("tickers")
        return tickers if isinstance(tickers, dict) else {}
    except (ClientError, ValueError, UnicodeDecodeError, KeyError, AttributeError):
        return {}


def write_index(s3, bucket, manifest, generated_at, universe_n):
    """Write data/corporate-actions-index.json (coverage + staleness)."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=STALE_DAYS)).isoformat()
    for entry in manifest.values():
        if isinstance(entry, dict):
            try:
                entry["stale"] = (entry.get("generated_at") or "") < cutoff
            except TypeError:
                entry["stale"] = True
    doc = {
        "generated_at": generated_at,
        "schema_version": SCHEMA_VERSION,
        "universe": universe_n,
        "covered": len(manifest),
        "tickers": manifest,
    }
    try:
        s3.put_object(Bucket=bucket, Key=INDEX_KEY,
                      Body=json.dumps(doc, default=str).encode("utf-8"),
                      ContentType="application/json")
        return True
    except (ClientError, TypeError, ValueError) as e:
        print("[corpact] write_index failed: %s" % type(e).__name__)
        return False


def lambda_handler(event, context):
    """Daily corporate-action factor build (resume-across-runs).

    event overrides (all optional):
      tickers: [..]     — process only these tickers (manual backfill)
      max_tickers: N    — cap the run (manual / testing)
    Returns {"statusCode": 200, "body": {"ok": True, "stats": {...}}}.
    Stops early (saving state + index) when the deadline approaches.
    """
    started = time.time()
    bucket = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
    # POLYGON_API_KEY is authoritative; POLYGON_KEY kept as fleet fallback.
    poly_key = os.environ.get("POLYGON_API_KEY") or os.environ.get("POLYGON_KEY", "")
    event = event if isinstance(event, dict) else {}
    only = {str(t).upper().strip() for t in (event.get("tickers") or []) if str(t).strip()}
    max_tickers = event.get("max_tickers")

    s3 = boto3.client("s3")
    universe = load_universe(s3, bucket)
    if only:
        universe = [t for t in universe if t in only]
    if isinstance(max_tickers, int) and max_tickers > 0:
        universe = universe[:max_tickers]

    n = len(universe)
    state = load_resume_state(s3, bucket)
    manifest = _load_prior_manifest(s3, bucket)
    cutoff_iso = (datetime.now(timezone.utc) - timedelta(days=STALE_DAYS)).isoformat()

    def is_due(t):
        """Never-seen or stale tickers are due."""
        entry = manifest.get(t)
        return not isinstance(entry, dict) or (entry.get("generated_at") or "") < cutoff_iso

    start = 0
    try:
        start = int(state.get("cursor", 0)) % n
    except (TypeError, ValueError, ZeroDivisionError):
        start = 0
    window = [universe[(start + i) % n] for i in range(min(BATCH, n))]
    work = [t for t in window if is_due(t)]

    stats = {
        "universe": n, "window": len(window), "due": len(work),
        "processed": 0, "written": 0, "failed": 0,
        "with_events": 0, "disagreements": 0, "factor_gaps": 0,
        "polygon_key": bool(poly_key),
    }
    manifest_updates = {}

    def out_of_time():
        """True when we should stop and persist (soft budget or context)."""
        if time.time() - started > SOFT_BUDGET_S:
            return True
        try:
            remaining = context.get_remaining_time_in_millis()
            return remaining is not None and remaining < 60000
        except (AttributeError, TypeError):
            return False

    for ticker in work:
        if out_of_time():
            stats["stopped_early"] = True
            break
        payload, res = process_ticker(s3, bucket, ticker, poly_key)
        stats["processed"] += 1
        if payload and res.get("ok"):
            stats["written"] += 1
            if payload["n_events"]:
                stats["with_events"] += 1
            stats["disagreements"] += len(payload["disagreements"])
            if payload["factor_gaps"]:
                stats["factor_gaps"] += 1
            manifest_updates[ticker] = {
                "generated_at": payload["generated_at"],
                "n_events": payload["n_events"],
                "n_splits": sum(1 for e in payload["events"] if e.get("type") == "split"),
                "n_dividends": sum(1 for e in payload["events"] if str(e.get("type") or "").startswith("dividend")),
                "latest_event_date": payload["latest_event_date"],
                "agreement_rate": payload["agreement_rate"],
                "factor_gaps": payload["factor_gaps"],
            }
        else:
            stats["failed"] += 1

    state = {"cursor": (start + len(window)) % n if n else 0, "updated_at": _now_iso()}
    save_resume_state(s3, bucket, state)
    manifest.update(manifest_updates)
    write_index(s3, bucket, manifest, _now_iso(), n)
    save_resume_state(s3, bucket, state)

    stats["duration_s"] = round(time.time() - started, 1)
    print("[corpact] done: %s" % json.dumps(stats))
    return {"statusCode": 200, "body": json.dumps({"ok": True, "stats": stats})}
