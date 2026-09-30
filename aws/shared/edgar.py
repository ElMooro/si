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


_MF = {}


def cik_map_mf():
    """symbol → CIK for FUNDS (ETFs/mutual funds) from SEC
    company_tickers_mf.json — the file company_tickers.json barely covers.
    wo4585: this is why etf-true-flows' N-PORT index showed 0 funds."""
    if _MF.get("m") is not None:
        return _MF["m"]
    out = {}
    try:
        # rev-E (4585): _get already returns parsed JSON — json.loads(dict)
        # raised TypeError and the except turned it into an empty map,
        # which is exactly how 0 funds indexed while looking like a
        # coverage problem.
        j = _get("https://www.sec.gov/files/company_tickers_mf.json") or {}
        fields = j.get("fields") or []
        data = j.get("data") or []
        if fields and data:
            fi = {str(f).lower(): i for i, f in enumerate(fields)}
            si = fi.get("symbol", fi.get("ticker"))
            ci = fi.get("cik")
            if si is not None and ci is not None:
                for row in data:
                    try:
                        sym = str(row[si]).upper().strip()
                        cik = int(row[ci])
                    except Exception:
                        continue
                    if sym and sym not in out:
                        out[sym] = cik
    except Exception as e:
        print("[edgar] cik_map_mf failed: %s" % str(e)[:100])
    _MF["m"] = out
    return out


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

# ── point-in-time XBRL fundamentals concept lists (4/10) ──
# Additive to the filing-grade set above. XBRL naming evolves across
# filers & eras, so each metric keeps its own ordered fallback list.
EPS_DILUTED = ["EarningsPerShareDiluted"]
EPS_BASIC = ["EarningsPerShareBasic"]
SHARES_DILUTED = ["WeightedAverageNumberOfDilutedSharesOutstanding"]
SHARES_BASIC = ["WeightedAverageNumberOfSharesOutstandingBasic"]
SHARES_OUTSTANDING = ["CommonStockSharesOutstanding"]
OCF = ["NetCashProvidedByUsedInOperatingActivities"]
CAPEX = ["PaymentsToAcquirePropertyPlantAndEquipment"]
DIVIDENDS = ["PaymentsOfDividendsCommonStock", "PaymentsOfDividends"]
DDA = ["DepreciationDepletionAndAmortization"]
LT_DEBT = ["LongTermDebtNoncurrent", "LongTermDebt"]
RETAINED_EARNINGS = ["RetainedEarningsAccumulatedDeficit"]
INVENTORY = ["InventoryNet"]
AR = ["AccountsReceivableNet", "ReceivablesNetCurrent"]
COGS = ["CostOfGoodsAndServicesSold", "CostOfGoodsSold"]
GROSS_PROFIT = ["GrossProfit"]
OP_INCOME = ["OperatingIncomeLoss"]
INTEREST_EXP = ["InterestExpense"]
TAX_EXP = ["IncomeTaxExpenseBenefit"]
RD_EXP = ["ResearchAndDevelopmentExpense"]

# Canonical point-in-time metric table: metric name -> fallback concept list.
# Consumed by justhodl-xbrl-fundamentals via cf_series().
FUNDAMENTAL_CONCEPTS = {
    "revenue": REVENUE,
    "net_income": NET_INCOME,
    "eps_diluted": EPS_DILUTED,
    "eps_basic": EPS_BASIC,
    "shares_diluted": SHARES_DILUTED,
    "shares_basic": SHARES_BASIC,
    "shares_outstanding": SHARES_OUTSTANDING,
    "ocf": OCF,
    "capex": CAPEX,
    "dividends": DIVIDENDS,
    "dda": DDA,
    "assets": ASSETS,
    "assets_current": ASSETS_CURRENT,
    "liabilities": LIABILITIES,
    "liabilities_current": LIABILITIES_CURRENT,
    "lt_debt": LT_DEBT,
    "stockholders_equity": STOCKHOLDERS_EQUITY,
    "retained_earnings": RETAINED_EARNINGS,
    "cash": CASH,
    "inventory": INVENTORY,
    "ar": AR,
    "cogs": COGS,
    "gross_profit": GROSS_PROFIT,
    "op_income": OP_INCOME,
    "interest_exp": INTEREST_EXP,
    "tax_exp": TAX_EXP,
    "rd_exp": RD_EXP,
}

_PIT_FORMS = ("10-K", "10-Q")


def cf_series(facts, concept_lists, form_filter=None):
    """Point-in-time observation series for the first matching concept.

    Unlike cf_latest_annual() (which collapses to a single value), this
    returns EVERY reported observation, preserving the original filing
    context so consumers see values exactly as known at filing time —
    no restatements applied. That is what makes the series point-in-time.

    facts: companyfacts JSON as returned by companyfacts().
    concept_lists: iterable of concept fallback lists, tried in order;
        the first list with ANY matching observation wins (same
        first-match semantics as frames_multi()). A bare string is
        treated as a single-concept list.
    form_filter: optional form prefix or iterable of prefixes to accept,
        e.g. "10-K" or ("10-K", "10-Q"). Defaults to ("10-K", "10-Q").
        Amendments ("/A") match by prefix, which is intentional: the
        amended observation is kept alongside the original, filed-date
        ordered, so consumers can see what changed.

    Returns a list of dicts, oldest-first by (end, filed):
        {val, end, filed, form, fy, fp, accession, concept_used}
    where accession is the filing's accn. Every us-gaap unit present
    (USD, USD/shares, shares, ...) is scanned. Duplicate observations
    reported under two concept tags are de-duplicated.
    Fail-soft: [] on malformed input.
    """
    if isinstance(form_filter, str):
        forms = (form_filter,)
    else:
        forms = tuple(form_filter) if form_filter else _PIT_FORMS
    out = []
    seen = set()
    try:
        if not isinstance(facts, dict):
            return []
        facts_node = facts.get("facts")
        if not isinstance(facts_node, dict):
            return []
        usg = facts_node.get("us-gaap")
        if not isinstance(usg, dict):
            return []
        for group in concept_lists or []:
            if isinstance(group, str):
                group = [group]
            for concept in group or []:
                node = usg.get(concept)
                if not isinstance(node, dict):
                    continue
                units = node.get("units")
                if not isinstance(units, dict):
                    continue
                for unit_name in sorted(units):
                    obs = units[unit_name]
                    if not isinstance(obs, list):
                        continue
                    for o in obs:
                        if not isinstance(o, dict):
                            continue
                        form = o.get("form") or ""
                        if not isinstance(form, str) or not form.startswith(forms):
                            continue
                        val = o.get("val")
                        if val is None:
                            continue
                        key = (o.get("end"), o.get("filed"), form,
                               o.get("fp"), str(val))
                        if key in seen:
                            continue
                        seen.add(key)
                        out.append({
                            "val": val,
                            "end": o.get("end"),
                            "filed": o.get("filed"),
                            "form": form,
                            "fy": o.get("fy"),
                            "fp": o.get("fp"),
                            "accession": o.get("accn"),
                            "concept_used": concept,
                        })
            if out:
                break  # first concept group with observations wins
    except (AttributeError, TypeError, ValueError):
        return []
    out.sort(key=lambda r: ((r.get("end") or ""), (r.get("filed") or "")))
    return out
