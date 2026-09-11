"""FMP /stable/ helper. Legacy /api/v3 died 2025-08-31.

Callers that still pass path-style ('earnings-surprises/AAPL') get
rewritten to ?symbol=. Never logs the key. Bundle via deploy-lambdas.
"""
import json
import time
import urllib.request

STABLE = "https://financialmodelingprep.com/stable"


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
            return json.loads(h.read())
    except Exception:
        return None
