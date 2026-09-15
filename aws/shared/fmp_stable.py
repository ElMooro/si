"""FMP /stable/ helper. Legacy /api/v3 died 2025-08-31.

Callers that still pass path-style ('earnings-surprises/AAPL') get
rewritten to ?symbol=. Never logs the key. Bundle via deploy-lambdas.
"""
import json
import time
import urllib.error
import urllib.request

STABLE = "https://financialmodelingprep.com/stable"


KEY_STATUS = {"status": "unknown"}


def fmp_url(path, qs, key):
    path = (path or "").lstrip("/")
    if path.startswith("http"):
        return path
    if "/" in path:
        head, _, tail = path.partition("/")
        extra = (qs + "&") if qs else ""
        return "%s/%s?symbol=%s&%sapikey=%s" % (STABLE, head, tail, extra, key)
    extra = (qs + "&") if qs else ""
    return "%s/%s?%sapikey=%s" % (STABLE, path, extra, key)


def fmp_get(path, qs="", key="", budget=None, timeout=14, ua="justhodl-fleet"):
    if not key:
        return None
    if isinstance(budget, dict):
        if budget.get("n", 0) <= 0:
            return None
        budget["n"] = budget.get("n", 0) - 1
    try:
        req = urllib.request.Request(
            fmp_url(path, qs, key), headers={"User-Agent": ua})
        time.sleep(0.12)
        with urllib.request.urlopen(req, timeout=timeout) as h:
            KEY_STATUS["status"] = "ok"
            return json.loads(h.read())
    except urllib.error.HTTPError as e:
        # fail-soft stays, but a rejected key is never silently "no data" (key ship 2026-09-15)
        if e.code in (401, 403):
            KEY_STATUS.update(status="unauthorized", http=e.code, path=path, at=time.time())
        else:
            KEY_STATUS.update(status="http_%d" % e.code, path=path, at=time.time())
        return None
    except Exception:
        return None


def key_status():
    """{'status': 'ok'|'unauthorized'|'http_NNN'|'unknown', ...} -- engines put this in their harvest so a 401 is visible."""
    return dict(KEY_STATUS)
