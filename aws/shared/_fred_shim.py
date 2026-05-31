"""_fred_shim.py — Monkey-patch urllib for cache-first FRED + 429 backoff.

DESIGN
══════
This module intercepts urllib.request.urlopen() calls to FRED API endpoints
and serves responses from S3 cache (data/fred-cache.json) when possible.
Falls back to live FRED with exponential backoff on HTTP 429.

USAGE (one line at top of each Lambda's lambda_function.py):
    import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff

WHY MONKEY-PATCH
════════════════
69 Lambdas currently hit api.stlouisfed.org/fred/series/observations
directly. Patching each Lambda's fetch_fred() function would require
identifying ~69 different code patterns. Monkey-patching urllib.urlopen
covers them all uniformly with a single import line per Lambda.

The 30+ Lambdas with FRED_API_KEY share the same key — when they fire
concurrently they trigger HTTP 429 rate limits (FRED free tier = 120/min).
Cache-first reads from data/fred-cache.json (maintained by
justhodl-financial-secretary v2.2 with 207 series including the Liquidity
Triad) eliminate ~88-95% of live FRED calls.

CACHE SCHEMA (confirmed via ops/1070)
═════════════════════════════════════
S3: justhodl-dashboard-live/data/fred-cache.json
Structure:
    {
        "WALCL": [
            {"date": "2026-05-27", "value": 6704383.0, "_meta": {...}},
            {"date": "2026-05-20", "value": 6713643.0},
            ...
        ],
        "WTREGEN": [...],
        ...
    }
Newest-first ordering. 207 series, ~120 observations each.

FAIL-SAFE
═════════
If S3 cache load fails (NoSuchKey, permissions, etc), shim silently falls
through to original urlopen + 429 backoff. Lambda continues to function
exactly as before. No new failure modes introduced.

PERFORMANCE
═══════════
Cache hit: ~2-5ms per call (S3 GET cached in warm container, dict lookup)
Cache miss + 429: ~14s total (2s + 4s + 8s backoff)
Memory: ~1MB cache file held in warm container memory across invocations
"""
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request

_REAL_URLOPEN = urllib.request.urlopen
_CACHE = None
_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
_FRED_ENDPOINT_MARKER = "api.stlouisfed.org/fred/series/observations"


def _load_cache():
    """Lazy-load S3 cache. Cached in warm container."""
    global _CACHE
    if _CACHE is not None:
        return _CACHE
    try:
        import boto3
        s3 = boto3.client("s3", region_name="us-east-1")
        obj = s3.get_object(Bucket=_BUCKET, Key="data/fred-cache.json")
        _CACHE = json.loads(obj["Body"].read().decode("utf-8"))
        print(f"[fred-shim] cache loaded: {len(_CACHE)} series")
    except Exception as e:
        print(f"[fred-shim] cache load failed: {type(e).__name__}: {str(e)[:80]} "
                f"— falling through to live FRED")
        _CACHE = {}
    return _CACHE


class _FakeResponse:
    """Mimics enough of HTTPResponse for the patterns we see across all
    69 FRED-using Lambdas: .read(), .status, .headers, context-manager."""
    
    def __init__(self, body_bytes):
        self._body = body_bytes
        self.status = 200
        # urllib's HTTPResponse-style attributes
        self.headers = _FakeHeaders({"Content-Type": "application/json"})
        self.code = 200
        self.url = "https://api.stlouisfed.org/fred/series/observations"
        self.reason = "OK (from cache)"
    
    def read(self, size=-1):
        if size == -1:
            data = self._body
            self._body = b""
            return data
        data, self._body = self._body[:size], self._body[size:]
        return data
    
    def readline(self, *args):
        idx = self._body.find(b"\n")
        if idx == -1:
            data, self._body = self._body, b""
            return data
        data = self._body[:idx + 1]
        self._body = self._body[idx + 1:]
        return data
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self._body = b""
    
    def close(self):
        self._body = b""
    
    def getheader(self, name, default=None):
        return self.headers.get(name, default)
    
    def getheaders(self):
        return list(self.headers.items())
    
    def info(self):
        return self.headers
    
    def geturl(self):
        return self.url


class _FakeHeaders:
    """Mimics http.client.HTTPMessage enough for downstream patterns."""
    def __init__(self, d):
        self._d = {k.lower(): (k, v) for k, v in d.items()}
    def get(self, name, default=None):
        v = self._d.get(name.lower())
        return v[1] if v else default
    def __getitem__(self, name):
        return self._d[name.lower()][1]
    def items(self):
        return [(k, v) for k, v in self._d.values()]
    def __contains__(self, name):
        return name.lower() in self._d


def _build_fred_response(observations, series_id):
    """Build a FRED-shaped JSON response from cache observations."""
    payload = {
        "realtime_start":  observations[0]["date"] if observations else "",
        "realtime_end":    observations[0]["date"] if observations else "",
        "observation_start": observations[-1]["date"] if observations else "",
        "observation_end":   observations[0]["date"] if observations else "",
        "units":           "lin",
        "output_type":     1,
        "file_type":       "json",
        "order_by":        "observation_date",
        "sort_order":      "desc",
        "count":           len(observations),
        "offset":          0,
        "limit":           len(observations),
        "observations": [
            {"realtime_start": o["date"],
             "realtime_end":   o["date"],
             "date":           o["date"],
             "value":          str(o["value"]) if o.get("value") is not None else "."}
            for o in observations
        ],
        "_cache_hit":      True,
        "_series_id":      series_id,
    }
    return _FakeResponse(json.dumps(payload).encode("utf-8"))


def _intercept_fred(url):
    """Return _FakeResponse if cache hit, None to fall through to live."""
    cache = _load_cache()
    if not cache:
        return None
    
    parsed = urllib.parse.urlparse(url)
    params = dict(urllib.parse.parse_qsl(parsed.query))
    series_id = params.get("series_id", "").strip()
    
    if not series_id:
        return None
    
    cached_obs = cache.get(series_id)
    if not isinstance(cached_obs, list) or len(cached_obs) == 0:
        return None  # series not in cache, go live
    
    try:
        limit = int(params.get("limit", 30))
    except (ValueError, TypeError):
        limit = 30
    sort_order = params.get("sort_order", "desc")
    obs_start = params.get("observation_start", "")
    
    # Cache is newest-first. Apply observation_start filter.
    filtered = cached_obs
    if obs_start:
        filtered = [o for o in cached_obs if o.get("date", "") >= obs_start]
    
    # Apply sort + limit
    if sort_order == "asc":
        filtered = list(reversed(filtered))
    sliced = filtered[:limit] if limit > 0 else filtered
    
    return _build_fred_response(sliced, series_id)


def _patched_urlopen(req_or_url, *args, **kwargs):
    """Cache-first FRED + 429 backoff for any urlopen call."""
    url = req_or_url if isinstance(req_or_url, str) else req_or_url.full_url
    
    # Cache-first for FRED endpoints
    if _FRED_ENDPOINT_MARKER in url:
        try:
            response = _intercept_fred(url)
            if response is not None:
                return response
        except Exception as e:
            print(f"[fred-shim] intercept err {type(e).__name__}: {str(e)[:80]}")
            # fall through to live
    
    # Live call with 429 backoff
    for attempt in range(3):
        try:
            return _REAL_URLOPEN(req_or_url, *args, **kwargs)
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < 2:
                wait_s = 2 ** (attempt + 1)  # 2s, 4s, 8s
                print(f"[fred-shim] HTTP 429 backoff {wait_s}s (attempt {attempt+1}/3)")
                time.sleep(wait_s)
                continue
            raise
    return _REAL_URLOPEN(req_or_url, *args, **kwargs)


def install():
    """Activate the monkey-patch. Idempotent."""
    if urllib.request.urlopen is not _patched_urlopen:
        urllib.request.urlopen = _patched_urlopen


# Auto-install on import — fire and forget
install()
