"""aws/shared/fmp_etf.py — hardened FMP ETF endpoints for the whole fleet (ops 3374).

Why this exists: FMP has renamed /stable feeds before (grades-news→
grades-latest-news, 2026) and old names 400 while callers silently emptied;
separately, weight fields sometimes arrive as "23.4%" strings and holding
tickers drift between asset|symbol — brittle float()/single-field parsing
zeroed hot-money's drilldowns (fixed inline there, v1.4.0 keeps a local
copy). Every OTHER engine should import from here instead of hand-rolling.

Guarantees: endpoint LADDER per feed (slash + dash spellings), %-tolerant
weights, field-name fallbacks, [] on any failure (never raises), stdlib only.
"""

import json
import urllib.request

_UA = {"User-Agent": "JustHodl-fleet/1.0"}


def pctf(v):
    """'23.45%' | '23.45' | 23.45 | None → float (0.0 on anything else)."""
    try:
        return float(str(v).replace("%", "").strip() or 0)
    except Exception:
        return 0.0


def get_json(url, timeout=12):
    try:
        req = urllib.request.Request(url, headers=_UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception:
        return None


def _ladder(paths, timeout=12):
    for i, u in enumerate(paths):
        j = get_json(u, timeout=timeout)
        if isinstance(j, list) and j:
            if i:
                print("[fmp_etf] rung %d won: %s" % (i, u.split("?")[0].split("/stable/")[-1]))
            return j
    return []


def holdings(symbol, api_key, timeout=12):
    """RAW holdings rows (list of dicts, [] on failure). Fields vary by FMP
    era; use pctf() + asset|symbol fallbacks on the result."""
    return _ladder([
        "https://financialmodelingprep.com/stable/etf/holdings?symbol=%s&apikey=%s" % (symbol, api_key),
        "https://financialmodelingprep.com/stable/etf-holdings?symbol=%s&apikey=%s" % (symbol, api_key),
    ], timeout=timeout)


def sector_weightings(symbol, api_key, timeout=12):
    return _ladder([
        "https://financialmodelingprep.com/stable/etf/sector-weightings?symbol=%s&apikey=%s" % (symbol, api_key),
        "https://financialmodelingprep.com/stable/etf-sector-weightings?symbol=%s&apikey=%s" % (symbol, api_key),
    ], timeout=timeout)


def top_holdings(symbol, api_key, n=15, timeout=12):
    """Convenience: [{ticker, name, weight_pct}] sorted desc, tolerant."""
    out = []
    for h in holdings(symbol, api_key, timeout=timeout):
        if not isinstance(h, dict):
            continue
        tk = h.get("asset") or h.get("symbol") or h.get("ticker")
        if not tk:
            continue
        out.append({"ticker": tk,
                    "name": (h.get("name") or h.get("securityName") or "")[:40],
                    "weight_pct": round(pctf(h.get("weightPercentage") or h.get("weight")), 2)})
    return sorted(out, key=lambda x: -x["weight_pct"])[:n]
