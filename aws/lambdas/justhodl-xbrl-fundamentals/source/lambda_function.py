"""
justhodl-xbrl-fundamentals — per-ticker point-in-time XBRL fundamentals (4/10).

Pulls each company's full companyfacts from SEC EDGAR (keyless, free) and
extracts point-in-time series for ~27 fundamental concepts (revenue, EPS,
OCF, capex, balance-sheet items, ...) via aws/shared/edgar.py cf_series().
Point-in-time means: values exactly as known at filing time, filed-date
ordered, no restatements applied.

Outputs (S3, bucket from S3_BUCKET env):
  data/xbrl-fundamentals/{TICKER}.json  — {ticker, cik, generated_at,
      schema_version, source, concepts: {name: [observations]},
      coverage: {name: n}, n_observations, n_concepts_covered}
  data/xbrl-fundamentals-index.json     — whole-market coverage + staleness
  data/xbrl-raw/CIK{cik10}.json         — cached raw companyfacts, ETag-gated
  data/xbrl-fundamentals/_state.json    — resume state {ticker: last_success}

Scheduling: weekly (Sunday 06:00 UTC). Fresh 10-K/Q filers (from
data/10kq-filings.json) are refreshed first; the rest when stale (>7 days).
Resume state persists across runs, and the handler watches its own
deadline, so a 900s timeout never loses progress.

SEC fair-use: declared User-Agent (edgar.USER_AGENT), 0.25s pacing between
SEC calls, and If-None-Match so unchanged companyfacts are never
re-downloaded (304 -> serve the S3 cache).
"""
from __future__ import annotations

import gzip
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

try:
    import edgar
except ImportError:  # alternate layout (shared/ package on path)
    from shared import edgar  # noqa: F401

SEC_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

UNIVERSE_KEY = "data/finviz-universe.json"
FILINGS_KEY = "data/10kq-filings.json"
RAW_PREFIX = "data/xbrl-raw/"
ART_PREFIX = "data/xbrl-fundamentals/"
INDEX_KEY = "data/xbrl-fundamentals-index.json"
STATE_KEY = "data/xbrl-fundamentals/_state.json"

STALE_DAYS = 7
SEC_PACE_S = 0.25
SOFT_BUDGET_S = 840.0  # stop-early budget inside the 900s Lambda timeout
SCHEMA_VERSION = "1.0"


def _now_iso():
    """Current UTC time as an ISO-8601 string (seconds precision)."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def load_universe(s3, bucket):
    """[(ticker, cik_int)] for the whole-market universe, sorted by ticker.

    Reads data/finviz-universe.json (top-level by_ticker). Tickers with no
    CIK in edgar.cik_map() (non-US / unmapped) are skipped — XBRL
    fundamentals are US-EDGAR only. Fail-soft: [] on any read/parse error.
    """
    try:
        cmap = edgar.cik_map()
    except (OSError, ValueError, AttributeError) as e:
        print("[xbrl] cik_map failed: %s" % type(e).__name__)
        return []
    try:
        obj = s3.get_object(Bucket=bucket, Key=UNIVERSE_KEY)
        uni = json.loads(obj["Body"].read().decode("utf-8"))
        by_ticker = uni.get("by_ticker") or {}
        if not isinstance(by_ticker, dict):
            return []
    except (ClientError, ValueError, UnicodeDecodeError, KeyError, AttributeError) as e:
        print("[xbrl] load_universe failed: %s" % type(e).__name__)
        return []
    out = []
    for t in by_ticker:
        ticker = str(t).upper().strip()
        cik = cmap.get(ticker)
        if ticker and cik:
            try:
                out.append((ticker, int(cik)))
            except (TypeError, ValueError):
                continue
    out.sort()
    return out


def get_priority_tickers(s3, bucket, cik_to_ticker):
    """Tickers with fresh 10-K / 10-Q / 10-K/A / 10-Q/A filings.

    Reads data/10kq-filings.json (filings[] + amended[]). Ticker is taken
    from the filing record when present, else resolved via the CIK
    reverse map. Returns a set of tickers. Fail-soft: empty set.
    """
    forms_ok = ("10-K", "10-Q", "10-K/A", "10-Q/A")
    out = set()
    try:
        obj = s3.get_object(Bucket=bucket, Key=FILINGS_KEY)
        doc = json.loads(obj["Body"].read().decode("utf-8"))
    except (ClientError, ValueError, UnicodeDecodeError, KeyError, AttributeError) as e:
        print("[xbrl] get_priority_tickers failed: %s" % type(e).__name__)
        return out
    filings = []
    for k in ("filings", "amended"):
        v = doc.get(k)
        if isinstance(v, list):
            filings.extend(v)
    for f in filings:
        if not isinstance(f, dict):
            continue
        if f.get("form") not in forms_ok:
            continue
        ticker = ""
        raw_t = f.get("ticker")
        if isinstance(raw_t, str) and raw_t.strip():
            ticker = raw_t.upper().strip()
        else:
            try:
                cik_int = int(str(f.get("cik") or "").lstrip("0") or "0")
            except (TypeError, ValueError):
                cik_int = 0
            if cik_int:
                ticker = cik_to_ticker.get(cik_int, "")
        if ticker:
            out.add(ticker)
    return out


def load_resume_state(s3, bucket):
    """Resume state {ticker: last_success_iso}. Returns {} when absent/bad."""
    try:
        obj = s3.get_object(Bucket=bucket, Key=STATE_KEY)
        st = json.loads(obj["Body"].read().decode("utf-8"))
        return st if isinstance(st, dict) else {}
    except (ClientError, ValueError, UnicodeDecodeError, KeyError, AttributeError):
        return {}


def save_resume_state(s3, bucket, state):
    """Persist resume state. Returns True on success, False otherwise."""
    try:
        s3.put_object(Bucket=bucket, Key=STATE_KEY,
                      Body=json.dumps(state).encode("utf-8"),
                      ContentType="application/json")
        return True
    except (ClientError, TypeError, ValueError) as e:
        print("[xbrl] save_resume_state failed: %s" % type(e).__name__)
        return False


def fetch_companyfacts(s3, bucket, cik):
    """Companyfacts for one CIK with ETag-gated S3 caching.

    - HEADs data/xbrl-raw/CIK{cik10}.json for the SEC ETag kept in S3
      user metadata ("sec-etag"); the SEC's ETag is stored opaquely on
      every 200 response so the next run can send it back.
    - Conditional GET with If-None-Match: 304 -> serve the cached copy.
    - 200 -> parse, refresh the cache (with the new SEC ETag), return it.
    Sleeps 0.25s per SEC request (fair-use pacing).
    Returns (facts_dict, from_cache_bool); (None, False) on failure.
    """
    try:
        cik10 = str(int(cik)).zfill(10)
    except (TypeError, ValueError):
        return None, False
    key = RAW_PREFIX + "CIK%s.json" % cik10
    sec_etag = None
    cached = None
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
        sec_etag = (head.get("Metadata") or {}).get("sec-etag")
        cached = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8"))
    except ClientError as e:
        code = (e.response.get("Error") or {}).get("Code")
        if code not in ("404", "NoSuchKey", "NotFound", "NoSuchBucket"):
            print("[xbrl] cache read failed for %s: %s" % (cik10, type(e).__name__))
        cached = None
    except (ValueError, UnicodeDecodeError, KeyError, AttributeError):
        print("[xbrl] corrupt cache for %s, refetching" % cik10)
        cached = None

    headers = {"User-Agent": edgar.USER_AGENT, "Accept-Encoding": "gzip"}
    if sec_etag:
        headers["If-None-Match"] = sec_etag
    req = urllib.request.Request(SEC_FACTS_URL.format(cik=cik10), headers=headers)
    try:
        resp = urllib.request.urlopen(req, timeout=60)
        raw = resp.read()
        if resp.headers.get("Content-Encoding") == "gzip":
            raw = gzip.decompress(raw)
        facts = json.loads(raw.decode("utf-8", "ignore"))
        new_etag = resp.headers.get("ETag")
        meta = {"sec-etag": new_etag} if new_etag else {}
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=raw,
                          ContentType="application/json", Metadata=meta)
        except ClientError as e:
            print("[xbrl] cache write failed for %s: %s" % (cik10, type(e).__name__))
        time.sleep(SEC_PACE_S)
        return facts, False
    except urllib.error.HTTPError as e:
        time.sleep(SEC_PACE_S)
        if e.code == 304 and isinstance(cached, dict):
            return cached, True
        print("[xbrl] SEC HTTP %s for CIK %s" % (e.code, cik10))
        return None, False
    except (urllib.error.URLError, TimeoutError, ValueError, OSError, EOFError) as e:
        time.sleep(SEC_PACE_S)
        print("[xbrl] SEC fetch failed for CIK %s: %s" % (cik10, type(e).__name__))
        return None, False


def parse_ticker_fundamentals(ticker, cik, facts):
    """Point-in-time series for every fundamental concept, one ticker.

    Calls edgar.cf_series() once per metric in FUNDAMENTAL_CONCEPTS.
    Returns (concepts, coverage) where concepts is {name: [observations]}
    and coverage is {name: n_observations}. Fail-soft: a bad concept
    yields an empty series rather than failing the ticker.
    """
    concepts = {}
    coverage = {}
    for name, group in edgar.FUNDAMENTAL_CONCEPTS.items():
        try:
            series = edgar.cf_series(facts, [group])
        except (AttributeError, TypeError, ValueError):
            series = []
        if not isinstance(series, list):
            series = []
        concepts[name] = series
        coverage[name] = len(series)
    return concepts, coverage


def write_ticker_artifact(s3, bucket, ticker, cik, concepts, coverage, generated_at):
    """Write data/xbrl-fundamentals/{TICKER}.json. Returns True on success."""
    payload = {
        "ticker": ticker,
        "cik": cik,
        "generated_at": generated_at,
        "schema_version": SCHEMA_VERSION,
        "source": "SEC EDGAR companyfacts (us-gaap, point-in-time as-filed)",
        "concepts": concepts,
        "coverage": coverage,
        "n_observations": sum(coverage.values()),
        "n_concepts_covered": sum(1 for n in coverage.values() if n),
    }
    try:
        s3.put_object(Bucket=bucket, Key=ART_PREFIX + "%s.json" % ticker,
                      Body=json.dumps(payload, default=str).encode("utf-8"),
                      ContentType="application/json")
        return True
    except (ClientError, TypeError, ValueError) as e:
        print("[xbrl] artifact write failed for %s: %s" % (ticker, type(e).__name__))
        return False


def _load_prior_manifest(s3, bucket):
    """Prior index tickers dict, or {} — keeps the index whole-market."""
    try:
        obj = s3.get_object(Bucket=bucket, Key=INDEX_KEY)
        doc = json.loads(obj["Body"].read().decode("utf-8"))
        tickers = doc.get("tickers")
        return tickers if isinstance(tickers, dict) else {}
    except (ClientError, ValueError, UnicodeDecodeError, KeyError, AttributeError):
        return {}


def write_index(s3, bucket, manifest, generated_at):
    """Write data/xbrl-fundamentals-index.json (coverage + staleness).

    Each manifest entry gains a stale flag (generated_at older than
    STALE_DAYS). Returns True on success.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=STALE_DAYS)).isoformat()
    for entry in manifest.values():
        if isinstance(entry, dict):
            try:
                entry["stale"] = (entry.get("generated_at") or "") < cutoff
            except TypeError:
                entry["stale"] = True
    payload = {
        "generated_at": generated_at,
        "schema_version": SCHEMA_VERSION,
        "n_tickers": len(manifest),
        "tickers": manifest,
    }
    try:
        s3.put_object(Bucket=bucket, Key=INDEX_KEY,
                      Body=json.dumps(payload).encode("utf-8"),
                      ContentType="application/json")
        return True
    except (ClientError, TypeError, ValueError) as e:
        print("[xbrl] index write failed: %s" % type(e).__name__)
        return False


def _is_stale(ticker, state, cutoff_iso):
    """True when the ticker was never processed or its success is old."""
    ts = state.get(ticker)
    if not ts or not isinstance(ts, str):
        return True
    try:
        return ts < cutoff_iso
    except TypeError:
        return True


def lambda_handler(event, context):
    """Weekly point-in-time XBRL fundamentals build (resume-across-runs).

    event overrides (all optional):
      tickers: [..]     — process only these tickers (manual backfill)
      max_tickers: N    — cap the run (manual / testing)
    Returns {"statusCode": 200, "body": {"ok": True, "stats": {...}}}.
    Stops early (saving state + index) when the deadline approaches.
    """
    started = time.time()
    bucket = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
    event = event if isinstance(event, dict) else {}
    only = {str(t).upper().strip() for t in (event.get("tickers") or []) if str(t).strip()}
    max_tickers = event.get("max_tickers")

    s3 = boto3.client("s3")
    universe = load_universe(s3, bucket)
    if only:
        universe = [(t, c) for (t, c) in universe if t in only]
    cik_of = {t: c for (t, c) in universe}
    cik_to_ticker = {c: t for (t, c) in universe}

    priority = get_priority_tickers(s3, bucket, cik_to_ticker)
    state = load_resume_state(s3, bucket)
    cutoff_iso = (datetime.now(timezone.utc) - timedelta(days=STALE_DAYS)).isoformat()

    prio = sorted(t for (t, c) in universe
                  if t in priority and _is_stale(t, state, cutoff_iso))
    rest = sorted((t for (t, c) in universe
                   if t not in priority and _is_stale(t, state, cutoff_iso)),
                  key=lambda t: state.get(t) or "")
    work = prio + rest
    if isinstance(max_tickers, int) and max_tickers > 0:
        work = work[:max_tickers]

    stats = {
        "universe": len(universe),
        "priority_due": len(prio),
        "stale_rest_due": len(rest),
        "processed": 0,
        "from_cache": 0,
        "written": 0,
        "failed": 0,
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
        facts, from_cache = fetch_companyfacts(s3, bucket, cik_of[ticker])
        if not isinstance(facts, dict) or not facts:
            stats["failed"] += 1
            continue
        concepts, coverage = parse_ticker_fundamentals(ticker, cik_of[ticker], facts)
        gen = _now_iso()
        if write_ticker_artifact(s3, bucket, ticker, cik_of[ticker],
                                 concepts, coverage, gen):
            stats["written"] += 1
            stats["processed"] += 1
            if from_cache:
                stats["from_cache"] += 1
            state[ticker] = gen
            manifest_updates[ticker] = {
                "cik": cik_of[ticker],
                "generated_at": gen,
                "n_observations": sum(coverage.values()),
                "n_concepts_covered": sum(1 for n in coverage.values() if n),
            }
        else:
            stats["failed"] += 1
        save_resume_state(s3, bucket, state)

    manifest = _load_prior_manifest(s3, bucket)
    manifest.update(manifest_updates)
    write_index(s3, bucket, manifest, _now_iso())
    save_resume_state(s3, bucket, state)

    stats["duration_s"] = round(time.time() - started, 1)
    print("[xbrl] done: %s" % json.dumps(stats))
    return {"statusCode": 200, "body": json.dumps({"ok": True, "stats": stats})}
