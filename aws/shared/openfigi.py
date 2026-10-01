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
conservative six-second request pacing (10 jobs/request); 429s are
honored with Retry-After backoff (max 3 retries).

Zero third-party deps beyond boto3/urllib.
"""
import json
import math
import re
import time
import urllib.error
import urllib.request

import boto3
from botocore.config import Config

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
            ssm = boto3.client("ssm", region_name="us-east-1", config=Config(
                connect_timeout=2, read_timeout=3, retries={"total_max_attempts": 1}))
            _API_KEY = ssm.get_parameter(
                Name=SSM_PARAM, WithDecryption=True)["Parameter"]["Value"]
        except Exception:
            _API_KEY = None
    return _API_KEY


def _remaining(deadline):
    if deadline is None:
        return float("inf")
    if type(deadline) not in (int, float) or not math.isfinite(deadline):
        return 0.0
    return max(0.0, deadline - time.monotonic())


def _sleep_within(seconds, deadline):
    if not math.isfinite(seconds) or seconds < 0 or seconds >= _remaining(deadline):
        return False
    time.sleep(seconds)
    return True


def _json_pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError("Duplicate response field")
        result[key] = value
    return result


def _json_constant(value):
    raise ValueError("Nonfinite response number")


def _response_json(response, deadline):
    if getattr(response, "status", 200) != 200:
        raise ValueError("Whole successful HTTP response required")
    headers = getattr(response, "headers", {})
    if headers.get("Content-Range"):
        raise ValueError("Partial response refused")
    declared = headers.get("Content-Length")
    length = int(declared) if declared is not None else None
    limit = 16 * 1024 * 1024
    if length is not None and not 0 <= length <= limit:
        raise ValueError("Response exceeds byte bound")
    chunks, total = [], 0
    # read1 returns available bytes rather than waiting to fill a large buffer,
    # allowing a cooperative deadline check between socket reads.
    read = getattr(response, "read1", response.read)
    while True:
        if _remaining(deadline) <= 0:
            raise ValueError("Response deadline exceeded")
        chunk = read(min(65536, limit + 1 - total))
        if not isinstance(chunk, bytes):
            raise ValueError("Binary response required")
        if not chunk:
            break
        total += len(chunk)
        if total > limit or (length is not None and total > length):
            raise ValueError("Response length exceeded")
        chunks.append(chunk)
    if length is not None and total != length:
        raise ValueError("Incomplete HTTP response")
    return json.loads(b"".join(chunks), object_pairs_hook=_json_pairs, parse_constant=_json_constant)


def _post(url, payload, api_key, timeout=25, deadline=None):
    """Return a whole parsed response or None; failure is never a negative match.

    Cooperative request/backoff deadline. A server Retry-After longer than the
    allowed wait is deferred, never shortened into an early retry.
    """
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key
    try:
        body = json.dumps(payload, allow_nan=False).encode()
    except (TypeError, ValueError):
        return None
    wait = 1.0
    for attempt in range(_MAX_RETRIES + 1):
        remaining = _remaining(deadline)
        if remaining <= 0.1:
            return None
        req = urllib.request.Request(url, data=body, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=min(timeout, remaining, 5 if deadline is not None else timeout)) as r:
                return _response_json(r, deadline)
        except urllib.error.HTTPError as e:
            retry_after = e.headers.get("Retry-After") if e.headers else None
            e.close()
            if e.code == 429 and attempt < _MAX_RETRIES:
                try:
                    delay = float(retry_after) if retry_after is not None else wait
                except (TypeError, ValueError):
                    return None
                if not math.isfinite(delay) or delay < 0:
                    return None
                wait = max(delay, wait, (_KEYED_PACE if api_key else _ANON_PACE) if url == MAPPING_URL else 0)
                if wait > 60 or not _sleep_within(wait, deadline):
                    return None
                wait *= 2
                continue
            return None
        except Exception:
            return None
    return None


_NEXT_MAPPING_AT = 0.0


def mapping(jobs, api_key=None, deadline=None):
    """Return one result per job, in order; invalid batches produce only errors.

    Legacy all-request-failed None remains supported. Missing data is NOT proof
    of no match: only the documented nonempty warning is a negative result.
    Existing batch sizes and request pacing are retained. Pacing is enforced
    between calls too, including failed requests; there is no final blind sleep.
    """
    global _NEXT_MAPPING_AT
    if not jobs:
        return []
    if _remaining(deadline) <= 0.1:
        return [{"error": "deadline exhausted"} for _ in jobs]
    key = api_key if api_key is not None else get_api_key()
    size = _KEYED_BATCH if key else _ANON_BATCH
    pace = _KEYED_PACE if key else _ANON_PACE
    out = []
    any_ok = False
    for i in range(0, len(jobs), size):
        batch = jobs[i:i + size]
        delay = max(0.0, _NEXT_MAPPING_AT - time.monotonic())
        if _remaining(deadline) <= 0.1 or (delay and not _sleep_within(delay, deadline)):
            out.extend({"error": "deadline exhausted"} for _ in batch)
            any_ok = True
            continue
        # Advance before the request; a failure must not bypass rate pacing.
        _NEXT_MAPPING_AT = time.monotonic() + pace
        res = _post(MAPPING_URL, batch, key, deadline=deadline)
        _NEXT_MAPPING_AT = max(_NEXT_MAPPING_AT, time.monotonic() + pace)
        if res is None:
            out.extend({"error": "request failed"} for _ in batch)
        elif not isinstance(res, list) or len(res) != len(batch) or not all(isinstance(r, dict) for r in res):
            out.extend({"error": "response cardinality or shape differs"} for _ in batch)
            any_ok = True
        else:
            out.extend(res)
            any_ok = True
    return out if any_ok else None


def resolution(item):
    """Retain the complete job response and refuse ambiguous security identity."""
    base = {"schema": "openfigi-resolution.v1", "response": item}
    if not isinstance(item, dict):
        return {**base, "status": "error", "reason": "invalid response"}
    keys = [key for key in ("data", "warning", "error") if key in item]
    if len(keys) != 1:
        return {**base, "status": "error", "reason": "contradictory or absent result"}
    if keys == ["error"]:
        return {**base, "status": "error", "reason": "provider error"}
    if keys == ["warning"]:
        if isinstance(item["warning"], str) and item["warning"].strip():
            return {**base, "status": "no_match", "reason": "provider warning"}
        return {**base, "status": "error", "reason": "invalid warning"}
    rows = item["data"]
    if not isinstance(rows, list) or not rows or any(
            not isinstance(row, dict) or not isinstance(row.get("figi"), str)
            or not re.fullmatch(r"BBG[A-Z0-9]{9}", row["figi"]) for row in rows):
        return {**base, "status": "error", "reason": "invalid candidate data"}
    # Even duplicate identifiers can carry conflicting descriptors. Keep all
    # candidates and require one whole row rather than choosing the first.
    if len(rows) != 1:
        return {**base, "status": "ambiguous", "reason": "multiple candidate rows"}
    return {**base, "status": "resolved", "security": dict(rows[0])}


def resolve_cusip(cusip, api_key=None, deadline=None):
    """Structured CUSIP resolution; no issuer-preference heuristic."""
    c = cusip.strip().upper() if isinstance(cusip, str) else ""
    query = {"idType": "ID_CUSIP", "idValue": c}
    if not re.fullmatch(r"[A-Z0-9*@#]{8}[0-9]", c):
        return {"schema": "openfigi-resolution.v1", "status": "error", "reason": "invalid CUSIP shape", "query": query}
    result = mapping([query], api_key=api_key, deadline=deadline)
    item = result[0] if isinstance(result, list) and len(result) == 1 else {"error": "request failed"}
    return {**resolution(item), "query": query}


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
    """Compatibility projection for a unique resolution only.

    None covers failed, ambiguous and no-match outcomes; callers must use
    resolve_cusip to distinguish them. None must never create no_match=True.
    """
    result = resolve_cusip(cusip, api_key=api_key)
    if result["status"] != "resolved":
        return None
    row = result["security"]
    return {"ticker": row.get("ticker"), "figi": row["figi"], "name": row.get("name"),
            "security_type": row.get("securityType"), "market_sector": row.get("marketSector"),
            "coupon": row.get("coupon"), "maturity": row.get("maturity")}


__all__ = ["get_api_key", "mapping", "search", "ticker_to_figi",
           "cusip_to_security", "resolve_cusip", "resolution"]
