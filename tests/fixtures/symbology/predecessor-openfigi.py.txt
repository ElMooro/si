"""aws/shared/openfigi.py — OpenFIGI v3 client shared across justhodl lambdas.

Covers two spines:
  * Equity symbology: TICKER -> FIGI (used by justhodl-symbology-master).
  * Bond bridge (9/10): ID_CUSIP -> ticker/FIGI/security descriptors for
    corporate bonds, feeding the TRACE layer (6/10) and the securities
    master once per-issuer tape lands (Phase 2).

Fail-soft everywhere: every public function returns None (or per-job
error placeholders) on auth/network/schema failures so callers degrade
to explicit "unresolved" states instead of inventing identifiers.

Keyless mode: without an SSM key the client still works at the anonymous
rate limit (5 requests/min, 10 jobs/request on /v3/mapping); 429s are
honored with Retry-After backoff (max 3 retries).

Zero third-party deps beyond boto3/urllib.
"""
import json
import time
import urllib.error
import urllib.request

import boto3

SSM_PARAM = "/justhodl/openfigi/api-key"
MAPPING_URL = "https://api.openfigi.com/v3/mapping"
SEARCH_URL = "https://api.openfigi.com/v3/search"

_KEYED_BATCH = 100      # jobs per /v3/mapping request with an API key
_ANON_BATCH = 10        # jobs per request without a key
_MAX_RETRIES = 3        # 429 backoff attempts
_KEYED_PACE = 0.35      # seconds between keyed batches (polite)
_ANON_PACE = 6.0        # seconds between anonymous batches (polite-ish; 429 backoff is the safety net)

_UNSET = object()
_API_KEY = _UNSET


def get_api_key():
    """Read the OpenFIGI API key from SSM, cached in a module global.

    Returns the key string, or None when SSM is unavailable or the
    parameter does not exist. None means "run keyless at the anonymous
    rate limit" — never an error by itself.
    """
    global _API_KEY
    if _API_KEY is _UNSET:
        try:
            ssm = boto3.client("ssm", region_name="us-east-1")
            _API_KEY = ssm.get_parameter(
                Name=SSM_PARAM, WithDecryption=True)["Parameter"]["Value"]
        except Exception:
            _API_KEY = None
    return _API_KEY


def _post(url, payload, api_key, timeout=25):
    """POST JSON and parse the JSON response, with 429 Retry-After backoff.

    Returns the parsed response, or None after _MAX_RETRIES throttled
    attempts or on any non-retryable failure. Never raises.
    """
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key
    body = json.dumps(payload).encode()
    wait = 1.0
    for attempt in range(_MAX_RETRIES + 1):
        req = urllib.request.Request(url, data=body, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < _MAX_RETRIES:
                try:
                    wait = max(float(e.headers.get("Retry-After") or 0), wait)
                except (TypeError, ValueError):
                    pass
                time.sleep(min(wait, 60.0))
                wait *= 2
                continue
            return None
        except Exception:
            return None
    return None


def mapping(jobs, api_key=None):
    """Map identifiers via OpenFIGI v3 /mapping.

    jobs: list of job dicts, e.g.
        {"idType": "TICKER", "idValue": "AAPL", "exchCode": "US"}
        {"idType": "ID_CUSIP", "idValue": "037833100"}
    Batching is automatic: 100 jobs/request with a key, 10/request
    anonymous, with polite pacing between batches.

    Returns a flat list of per-job result dicts in input order
    (each like {"data": [...]} or {"error": "..."}), or None when every
    batch failed. Callers treat missing/empty "data" as no-match.
    """
    if not jobs:
        return []
    key = api_key if api_key is not None else get_api_key()
    size = _KEYED_BATCH if key else _ANON_BATCH
    pace = _KEYED_PACE if key else _ANON_PACE
    out = []
    any_ok = False
    for i in range(0, len(jobs), size):
        batch = jobs[i:i + size]
        res = _post(MAPPING_URL, batch, key)
        if res is None:
            out.extend({"error": "request failed"} for _ in batch)
            continue
        any_ok = True
        if isinstance(res, list):
            out.extend(res)
        else:
            out.extend({"error": "unexpected response shape"} for _ in batch)
        time.sleep(pace)
    return out if any_ok else None


def search(query, api_key=None):
    """Free-text search via OpenFIGI v3 /search.

    Returns the list of hit dicts, or None on failure / no data.
    """
    q = (query or "").strip()
    if not q:
        return None
    key = api_key if api_key is not None else get_api_key()
    res = _post(SEARCH_URL, {"query": q}, key)
    if isinstance(res, dict):
        data = res.get("data")
        return data if isinstance(data, list) else None
    return None


def ticker_to_figi(ticker, api_key=None):
    """Resolve a US ticker to its FIGI via /v3/mapping.

    Returns the FIGI string, or None on no-match / failure.
    """
    t = (ticker or "").upper().strip()
    if not t:
        return None
    res = mapping([{"idType": "TICKER", "idValue": t, "exchCode": "US"}],
                  api_key=api_key)
    if not res or not isinstance(res, list):
        return None
    item = res[0]
    rows = item.get("data") if isinstance(item, dict) else None
    first = rows[0] if rows else None
    return first.get("figi") if isinstance(first, dict) else None


def cusip_to_security(cusip, api_key=None):
    """Resolve a 9-character CUSIP to security descriptors (bond-aware).

    A single CUSIP root can map to several securities (equity + debt).
    Preference order: marketSector == "Corp" with
    securityType == "Corporate Bond" first, then any "Corp" row, then the
    first hit.

    Returns dict {ticker, figi, name, security_type, market_sector,
    coupon, maturity}, or None on no-match / malformed CUSIP / failure.
    Unresolvable CUSIPs are the caller's cue to record {"no_match": True};
    identifiers are never invented here.
    """
    c = (cusip or "").upper().strip()
    if len(c) != 9:
        return None
    res = mapping([{"idType": "ID_CUSIP", "idValue": c}], api_key=api_key)
    if not res or not isinstance(res, list):
        return None
    item = res[0]
    rows = item.get("data") if isinstance(item, dict) else None
    cands = [r for r in rows if isinstance(r, dict)] if rows else []
    if not cands:
        return None

    def _rank(r):
        ms = (r.get("marketSector") or "").strip().lower()
        st = (r.get("securityType") or "").strip().lower()
        if ms == "corp" and st == "corporate bond":
            return 0
        if ms == "corp":
            return 1
        return 2

    best = min(cands, key=_rank)
    return {
        "ticker": best.get("ticker"),
        "figi": best.get("figi"),
        "name": best.get("name"),
        "security_type": best.get("securityType"),
        "market_sector": best.get("marketSector"),
        "coupon": best.get("coupon"),
        "maturity": best.get("maturity"),
    }


__all__ = ["get_api_key", "mapping", "search", "ticker_to_figi",
           "cusip_to_security"]
