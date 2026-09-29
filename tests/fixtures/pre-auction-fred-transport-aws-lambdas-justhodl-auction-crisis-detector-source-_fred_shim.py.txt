"""_fred_shim — cache-first FRED with 429 backoff (canonical, committed).

Transparently hardens FRED API calls platform-wide:
  • Retries api.stlouisfed.org requests on 429 / transient errors with
    exponential backoff (the original ops/1074 intent).
  • Caches successful FRED responses in /tmp for the Lambda's warm lifetime
    so repeated series fetches within a run don't re-hit FRED.
  • Preserves the exact FRED JSON response shape (it still calls FRED) — so
    every existing fred_obs()/parser keeps working unchanged.
  • NEVER raises on import. Importing this module only installs the wrapper;
    if anything goes wrong it silently no-ops.

This file is committed into every FRED-using Lambda source so redeploys are
safe (previously the shim existed only in the deployed package → a missing
import would crash on redeploy).
"""
import time as _time
import urllib.request as _ur

_FRED_HOST = "api.stlouisfed.org"
_CACHE = {}
_CACHE_TTL = 1800  # 30 min warm-lifetime cache


def _install():
    if getattr(_ur, "_jh_fred_shim_installed", False):
        return
    _orig_urlopen = _ur.urlopen

    def _patched(url, *args, **kwargs):
        try:
            target = url.full_url if hasattr(url, "full_url") else (url if isinstance(url, str) else None)
        except Exception:
            target = None
        # Only intercept FRED; everything else passes straight through.
        if not target or _FRED_HOST not in target:
            return _orig_urlopen(url, *args, **kwargs)
        # warm cache
        now = _time.time()
        hit = _CACHE.get(target)
        if hit and (now - hit[0]) < _CACHE_TTL:
            return _CachedResponse(hit[1])
        # retry with backoff on 429 / transient
        last_exc = None
        for attempt in range(4):
            try:
                resp = _orig_urlopen(url, *args, **kwargs)
                body = resp.read()
                _CACHE[target] = (now, body)
                return _CachedResponse(body)
            except Exception as e:  # urllib.error.HTTPError(429), URLError, timeout
                last_exc = e
                code = getattr(e, "code", None)
                if code == 429 or code in (500, 502, 503, 504) or code is None:
                    _time.sleep(min(8, 0.6 * (2 ** attempt)))
                    continue
                raise
        # exhausted — fall back to a stale cache entry if we have one
        if hit:
            return _CachedResponse(hit[1])
        if last_exc:
            raise last_exc
        return _orig_urlopen(url, *args, **kwargs)

    _ur.urlopen = _patched
    _ur._jh_fred_shim_installed = True


class _CachedResponse:
    """Minimal file-like wrapper so callers can .read()/.json() the body."""
    def __init__(self, body):
        self._body = body
        self.status = 200
        self.code = 200

    def read(self, *a, **k):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def close(self):
        pass

    def getcode(self):
        return 200


try:
    _install()
except Exception:
    pass  # never break the importing Lambda
