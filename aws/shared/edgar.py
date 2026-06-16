"""
edgar.py — SEC EDGAR authoritative XBRL toolkit (filing-grade financials).

Two access modes:
  • frames()  — one XBRL concept across ALL filers for a calendar period
                (efficient whole-market cross-sections, e.g. an NCAV screen).
  • companyfacts() — every reported value for one company (precise per-ticker,
                e.g. matching a specific fiscal period for a cross-check).

SEC fair-use: declare a real User-Agent w/ contact, stay polite (<10 req/s).
Concept names evolve across filers/eras, so each metric has a fallback list.
"""
import gzip
import json
import time
import urllib.request

USER_AGENT = "JustHodl Research raafouis@gmail.com"
_FRAMES = "https://data.sec.gov/api/xbrl/frames/us-gaap/{concept}/{unit}/{period}.json"
_FACTS = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
_CIKMAP_URL = "https://www.sec.gov/files/company_tickers.json"

_cik_cache = {}

# ── concept fallback lists (XBRL naming evolves across filers & eras) ──
REVENUE = ["RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues",
           "SalesRevenueNet", "RevenueFromContractWithCustomerIncludingAssessedTax"]
NET_INCOME = ["NetIncomeLoss", "ProfitLoss"]
ASSETS = ["Assets"]
ASSETS_CURRENT = ["AssetsCurrent"]
LIABILITIES = ["Liabilities"]
LIABILITIES_CURRENT = ["LiabilitiesCurrent"]
STOCKHOLDERS_EQUITY = ["StockholdersEquity", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"]
CASH = ["CashAndCashEquivalentsAtCarryingValue", "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"]


def _get(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                               "Accept-Encoding": "gzip, deflate"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            if r.headers.get("Content-Encoding") == "gzip":
                raw = gzip.decompress(raw)
            return json.loads(raw.decode("utf-8", "ignore"))
    except Exception:
        return None


def cik_map():
    """{TICKER -> int CIK}. Cached process-wide."""
    global _cik_cache
    if not _cik_cache:
        d = _get(_CIKMAP_URL)
        if isinstance(d, dict):
            for v in d.values():
                if isinstance(v, dict) and v.get("ticker"):
                    _cik_cache[v["ticker"].upper()] = int(v.get("cik_str", 0))
    return _cik_cache


def frames(concept, unit="USD", periods=None, pause=0.2):
    """{int CIK -> val}, taking the most-recent available period per company
    (periods must be ordered most-recent-first)."""
    out = {}
    for p in (periods or []):
        d = _get(_FRAMES.format(concept=concept, unit=unit, period=p))
        time.sleep(pause)
        if not d or "data" not in d:
            continue
        for row in d["data"]:
            cik = row.get("cik")
            if cik is not None and cik not in out:
                out[cik] = row.get("val")
    return out


def frames_multi(concepts, unit="USD", periods=None):
    """Try concept fallbacks in order; first concept with a value per CIK wins."""
    out = {}
    for c in concepts:
        for cik, v in frames(c, unit, periods).items():
            out.setdefault(cik, v)
    return out


def companyfacts(cik):
    """Full companyfacts for one CIK (int or zero-padded str)."""
    cik = str(cik).zfill(10)
    return _get(_FACTS.format(cik=cik)) or {}


def cf_latest_annual(facts, concepts, unit="USD"):
    """Most recent ANNUAL (FY, 10-K) value for the first matching concept.
    Returns (val, fy, end_date) or (None, None, None)."""
    usg = (facts.get("facts", {}) or {}).get("us-gaap", {})
    for c in concepts:
        node = usg.get(c)
        if not node:
            continue
        units = (node.get("units", {}) or {}).get(unit, [])
        anns = [u for u in units if u.get("fp") == "FY" and u.get("form", "").startswith("10-K") and u.get("val") is not None]
        if not anns:
            anns = [u for u in units if u.get("fp") == "FY" and u.get("val") is not None]
        if anns:
            best = max(anns, key=lambda u: (u.get("end", ""), u.get("fy", 0)))
            return best.get("val"), best.get("fy"), best.get("end")
    return None, None, None
