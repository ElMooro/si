"""justhodl-13f-book - the 13F institutional book, computed from the native holdings research (2026-11-03).

Khalid, 2026-11-03: "13F - Institutional book needs complete redesign ... make sure all 13F engines' features and
data is displayed and observable."
Khalid, 2026-11-04: "focus on stocks and ETFs being added, stocks and ETFs being deleted, equities vs bonds vs gold vs
crypto adding and selling, most stocks and ETFs held, new contender stocks and ETFs ... getting traction through
funds, also which are getting sold ... which stocks and ETFs they are bullish on, which they are bearish on."

What this engine is
  The page 13f.html shows the book; this engine computes it. It reads ONLY retained, published data and derives the
  boards the way a careful reader of the filings would - counting managers, summing reported values, comparing two
  dated disclosures - and it never infers a purchase, a sale, a flow or a unit:
    * data/13f-desk.json                    the native desk (contract holdings-native-research.v1): roster, cohorts,
                                            quality, limitations, the per-fund artifact pointers
    * data/holdings-research/funds/<sha>.json  one immutable artifact per manager (holdings-native-fund.v1):
                                            periods[date].positions (identity = CUSIP + class + SH/PRN + put/call,
                                            reported_quantity, reported_value_usd) and comparison.rows
    * data/13f-security-master.json         this engine's own keyed CUSIP -> security master (OpenFIGI v3 /mapping,
                                            idType ID_CUSIP): ticker, FIGI, security type, market sector. A ticker
                                            is shown only when the CUSIP itself resolved - never guessed from a name.
    * 13f-etf-classes.json (bundled; twin config/13f-etf-classes.json)  reviewed asset class per exchange-traded
                                            product ticker (equity / bond / cash / gold / silver / commodity / crypto /
                                            volatility). A fund-like security whose ticker is not in the table is
                                            shown as unclassified and listed for review.
    * data/13f-clone-alpha.json             SKILL 0-100 per manager (justhodl-13f-clone-alpha)
    * data/13f-price-divergence.json        justhodl-13f-price-divergence (state, methodology, signals)
    * data/institutional-positions.json     the EDGAR detector (justhodl-sec-13f): latest filing per manager
    * data/ai-commentary/13f.json           the deterministic brief for this page
  and HEADs every other 13F-family output so the page can show each engine's freshness.

Output: data/13f-book.json (contract 13f-book.v2). Sections: book (managers with asset mix, boards per universe
ALL / STOCKS / ETFs, asset-class rotation, smart-money stance, contenders, options overlay, security-master coverage,
period history), season (next period, deadline, who has filed), clone_alpha, price_divergence, ai_brief, engines
(observability), limitations/quality/research carried verbatim from the desk. Also writes
data/13f-security-master.json (state) and data/13f-book/periods/<period>.json (one compact snapshot per report
period, for traction across quarters).

Doctrine
  * "A reported position is not a trade": every board is labelled by what it counts (managers newly disclosing,
    managers whose reported quantity rose...). Reported value changes include the quarter's price move and say so.
  * Stance words (ACCUMULATING / DISTRIBUTING / CONTESTED / QUIET) are patterns in reported quantities across two
    dated disclosures, counted per manager - never a measured flow, never a recommendation.
  * Identity is the filer's: issuer names are the filers' own text. A ticker comes only from the CUSIP master.
  * A newly-present identity whose issuer also appears among the absent identities is flagged reclass_candidate
    (a CUSIP or class change, a spin-off, a conversion) - never counted as new money.
  * An absent identity that no current book reports is flagged no_current_holder (take-private, merger, delisting
    or simply sold by all - the filings cannot say which).
  * Invokes nothing. Reads S3 and OpenFIGI (budgeted, keyed through SSM). Writes three objects. Once a day (cadence
    floor) and after every native research publication (hand-off from justhodl-13f-positions).
"""
from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import json
import math
import os
import re
import time

import chain_guard  # the fleet-wide trigger cap: every engine start is admitted, counted and stamped
import openfigi     # aws/shared: keyed OpenFIGI v3 client (SSM /justhodl/openfigi/api-key), fail-soft
import fmp_bank     # aws/shared: the FMP warehouse reader (banked etf/info rows carry FMP's own assetClass) - S3 only

VERSION = "1.1.1"
CONTRACT = "13f-book.v2"
ENGINE = "justhodl-13f-book"
SCHEME = "v2"   # security-master query scheme: identifier typed by shape (CUSIP / CINS), US pass, any-exchange pass, ISIN pass

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = os.environ.get("OUT_KEY", "data/13f-book.json")
MASTER_KEY = os.environ.get("MASTER_KEY", "data/13f-security-master.json")
LEARNED_KEY = os.environ.get("LEARNED_KEY", "data/13f-etf-classes-learned.json")
PERIODS_PREFIX = os.environ.get("PERIODS_PREFIX", "data/13f-book/periods/")
DESK_KEY = "data/13f-desk.json"
CLONE_KEY = "data/13f-clone-alpha.json"
DIVERGENCE_KEY = "data/13f-price-divergence.json"
DETECTOR_KEY = "data/institutional-positions.json"
BRIEF_KEY = "data/ai-commentary/13f.json"
ETF_TABLE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "13f-etf-classes.json")

CONCENTRATED_MAX = int(os.environ.get("CONCENTRATED_MAX", "100"))
HEDGE_OPTION_ROWS = int(os.environ.get("HEDGE_OPTION_ROWS", "1000"))
MASTER_BUDGET_REQUESTS = int(os.environ.get("MASTER_BUDGET_REQUESTS", "100"))   # OpenFIGI requests per run, both passes
MASTER_BUDGET_S = float(os.environ.get("MASTER_BUDGET_S", "150"))              # wall-clock cap for the refresh
MASTER_RESERVE_S = 90.0                                                           # time kept for the rest of the build
RECHECK_DAYS = {"resolved": 180, "ambiguous": 30, "no_match": 90, "error": 1}   # per-status re-query interval
FMP_LOOKUPS_PER_RUN = int(os.environ.get("FMP_LOOKUPS_PER_RUN", "300"))         # banked etf/info reads per run (S3 only)
LEARNED_RECHECK_DAYS = {"banked": 180, "missing": 14}
MASTER_CANDIDATES = 12
TOP = 10
BOARD = 20
STANCE_TOP = 15
FRESH_H, DEAD_H = 30.0, 168.0
MIN_AGREEMENT = 2   # managers that must agree before a name is called a stance or a contender

# Display names the desk's roster uses (the desk carries official SEC names; these are the short labels the page
# shows beside them). Not identities - labels only.
DISPLAY = {
    "BERKSHIRE": "Berkshire Hathaway", "BRIDGEWATER": "Bridgewater Associates", "RENAISSANCE": "Renaissance Technologies",
    "AQR": "AQR Capital Management", "TWO_SIGMA": "Two Sigma Investments", "CITADEL": "Citadel Advisors",
    "MILLENNIUM": "Millennium Management", "PERSHING": "Pershing Square", "GREENLIGHT": "Greenlight Capital",
    "SOROS": "Soros Fund Management", "TIGER_GLOBAL": "Tiger Global Management", "COATUE": "Coatue Management",
    "BAUPOST": "Baupost Group", "ELLIOTT": "Elliott Investment Management", "SCION": "Scion Asset Management",
    "DURATION": "Swiss National Bank", "POINT72": "Point72 Asset Management", "LONE_PINE": "Lone Pine Capital",
}
SHORT = {
    "AQR": "AQR", "BAUPOST": "BAU", "BERKSHIRE": "BRK", "BRIDGEWATER": "BW", "CITADEL": "CIT", "COATUE": "COA",
    "DURATION": "SNB", "ELLIOTT": "ELL", "GREENLIGHT": "GLC", "LONE_PINE": "LP", "MILLENNIUM": "MLN", "PERSHING": "PSQ",
    "POINT72": "P72", "RENAISSANCE": "REN", "SCION": "SCN", "SOROS": "SOR", "TIGER_GLOBAL": "TGR", "TWO_SIGMA": "2S",
}

# The 13F family as it is scheduled today (tests/deployment/test_13f_book_schedules.py pins these to
# config/schedule-manifest.json and the engines' config.json, so the page never shows a stale cadence).
ENGINES = [
    {"engine": "justhodl-13f-positions", "role": "native research: originals, chains, dated comparisons (the desk + 18 fund artifacts)",
     "outputs": ["data/13f-desk.json", "data/13f-positions.json", "data/holdings-research.json", "data/13f-by-ticker.json", "data/13f-flows-by-ticker.json"],
     "schedule": "daily 15:40 UTC (Scheduler justhodl-holdings-originals-research) + 16:21 UTC (rule justhodl-13f-positions-sched)", "expected_h": 24},
    {"engine": "justhodl-sec-13f", "role": "EDGAR filing detector: latest 13F-HR per tracked manager, new accessions",
     "outputs": ["data/institutional-positions.json"], "schedule": "daily 03:31 UTC (rule justhodl-sec-13f-daily)", "expected_h": 24},
    {"engine": "justhodl-13f-book", "role": "this book: boards, rotation, stance, contenders, security master, season, observability",
     "outputs": ["data/13f-book.json", "data/13f-security-master.json", "data/13f-etf-classes-learned.json"], "schedule": "daily 16:37 UTC (Scheduler justhodl-13f-book-daily) + after every native research publication (hand-off)", "expected_h": 24},
    {"engine": "justhodl-13f-clone-alpha", "role": "clone-alpha: followable skill per manager from ~13 quarters of filings",
     "outputs": ["data/13f-clone-alpha.json"], "schedule": "weekly Monday 08:30 UTC (Scheduler justhodl-13f-clone-alpha-weekly)", "expected_h": 168},
    {"engine": "justhodl-13f-price-divergence", "role": "aggregate 13F position change vs 60-day price return",
     "outputs": ["data/13f-price-divergence.json"], "schedule": "weekdays 21:20 UTC (rule justhodl-13f-price-divergence-sched)", "expected_h": 72},
    {"engine": "justhodl-page-ai-commentary", "role": "the page's brief (deterministic source inventory under the no-paid-AI policy)",
     "outputs": ["data/ai-commentary/13f.json"], "schedule": "weekdays 14:00 UTC (rule justhodl-page-ai-commentary-daily)", "expected_h": 72},
    {"engine": "justhodl-smart-money-13f", "role": "AI-conviction funds' 13F books by bottleneck layer (smart-money-13f.html)",
     "outputs": ["data/smart-money-13f.json"], "schedule": "weekly Monday 11:03 UTC (rule smart-money-13f-weekly)", "expected_h": 168},
    {"engine": "justhodl-smart-money-cluster", "role": "cross-fund overlap clusters read from the 13F tape",
     "outputs": ["data/smart-money-clusters.json"], "schedule": "daily 16:55 UTC (Scheduler justhodl-smart-money-cluster-hourly)", "expected_h": 24},
    {"engine": "justhodl-smart-money-holdings", "role": "famous-fund conviction holdings for the screener (schedule DISABLED)",
     "outputs": ["screener/smart-money-holdings.json"], "schedule": "DISABLED (smart-money-holdings-sched 14:31 UTC); runs only when another tick starts it", "expected_h": None},
    {"engine": "justhodl-smart-money-tracker", "role": "smart-money screener tape (schedule DISABLED)",
     "outputs": ["screener/smart-money.json"], "schedule": "DISABLED (smart-money-tracker-sched 14:00 UTC); runs only when another tick starts it", "expected_h": None},
    {"engine": "justhodl-institutional-footprint", "role": "13F footprint consumer (capital-flow lens)",
     "outputs": ["data/institutional-footprint.json"], "schedule": "weekdays 23:10 UTC (rule justhodl-institutional-footprint-daily)", "expected_h": 72},
]
STATE_KEYS = ["data/13f-cusip-map.json", "data/13f-state/first-seen.json", "data/13f-state/cik-overrides.json"]

# OpenFIGI securityType vocabulary (marketSector Equity). Fund-like types are bucketed through the reviewed ETF table
# by ticker; everything else in the Equity sector is a stock-like security with the sub-type shown.
FUND_TYPES = {"ETP", "ETF", "Closed-End Fund", "Open-End Fund", "Mutual Fund", "Fund of Funds", "Unit Inv Trust", "FUND"}
STOCK_SUBTYPES = {
    "Common Stock": "common", "Tracking Stk": "common", "Stapled Security": "common", "NY Reg Shrs": "adr",
    "ADR": "adr", "GDR": "adr", "Depositary Receipt": "adr", "Dutch Cert": "adr", "CDR": "adr", "Receipt": "adr",
    "REIT": "reit", "Preference": "preferred", "Preferred Stock": "preferred", "Pfd": "preferred",
    "MLP": "mlp", "Ltd Part": "mlp", "Partnership Shs": "mlp", "Royalty Trst": "trust", "Unit": "unit",
    "Warrant": "warrant", "Right": "right", "Equity WRT": "warrant", "SPAC": "spac",
}
CLASS_BUCKET = {"equity": "equities", "bond": "bonds", "cash": "cash", "gold": "gold", "silver": "silver",
                "commodity": "commodities", "crypto": "crypto", "volatility": "volatility", "currency": "currency", "multi_asset": "multi_asset"}
BUCKET_ORDER = ["equities", "bonds", "cash", "gold", "silver", "commodities", "crypto", "volatility", "currency", "multi_asset", "other", "funds_unclassified", "unmapped"]
BUCKET_LABEL = {"equities": "Equities (stocks, preferreds + equity ETFs)", "bonds": "Bonds (bond ETFs + notes/converts at principal)", "cash": "Cash & T-bills (ETFs)",
                "gold": "Gold (ETFs/trusts)", "silver": "Silver (ETFs/trusts)", "commodities": "Commodities (ETFs)", "crypto": "Crypto (ETFs/trusts)",
                "volatility": "Volatility (ETNs/ETFs)", "currency": "Currency (ETFs/ETNs)", "multi_asset": "Multi-asset / alternative (ETFs)",
                "other": "Other (non-equity sector, shares)", "funds_unclassified": "ETF - unclassified (review the table)",
                "unmapped": "Unmapped CUSIPs (no OpenFIGI resolution yet)"}
# FMP etf/info assetClass (the provider's own label, read from the bank) -> reviewed class vocabulary; unknown labels stay unclassified and are surfaced
FMP_CLASS = {"equity": "equity", "fixed income": "bond", "bond": "bond", "commodity": "commodity", "commodities": "commodity", "currency": "currency",
             "volatility": "volatility", "digital assets": "crypto", "crypto": "crypto", "cryptocurrency": "crypto", "real estate": "equity",
             "preferred stock": "equity", "preferred": "equity", "multi-asset": "multi_asset", "multi asset": "multi_asset", "alternative": "multi_asset",
             "alternatives": "multi_asset", "asset allocation": "multi_asset"}
TILT_PCT = 15.0   # a class is ACCUMULATING / DISTRIBUTING when the net manager-identity pairs exceed this share of all pairs
NAV_TICKER = re.compile(r"X[A-Z0-9]{2,6}X")


# ───────────────────────────── pure helpers ─────────────────────────────
def dec(v):
    try:
        d = Decimal(str(v))
        return d if d.is_finite() else Decimal(0)
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(0)


def f(v):
    return float(v) if v is not None else None


def pct(a, b):
    """100 * (a / b - 1), None when b is 0/None. Pure."""
    a, b = dec(a), dec(b)
    return f(round(100 * (a / b - 1), 1)) if b else None


def share(a, b, nd=2):
    a, b = dec(a), dec(b)
    return f(round(100 * a / b, nd)) if b else None


def is_option(identity):
    return bool((identity or {}).get("put_call"))


def is_equity(identity):
    return (identity or {}).get("quantity_type") == "SH" and not is_option(identity)


def is_debt(identity):
    return (identity or {}).get("quantity_type") == "PRN" and not is_option(identity)


def issuer_key(name):
    """First two alphabetic tokens of an issuer name, for reclassification matching. Pure."""
    toks = [t for t in re.split(r"[^A-Z0-9]+", str(name or "").upper()) if t and t not in ("INC", "CORP", "CO", "LTD", "PLC", "LLC", "LP", "NEW", "THE", "DEL", "SA", "NV")]
    return " ".join(toks[:2])


def id_type(identifier):
    """13F identifiers: a CUSIP (digit-led) or a CINS (letter-led, foreign issuers: Chubb H1467J104, Linde G54950103). Pure."""
    return "ID_CINS" if str(identifier)[:1].isalpha() else "ID_CUSIP"


def isin_from_cusip(cusip, country="US"):
    """The ISIN defined by a US CUSIP: 'US' + CUSIP + Luhn check digit over the letter-expanded body. Exact, not a guess. Pure."""
    body = country + cusip
    digits = "".join(str(ord(ch) - 55) if ch.isalpha() else ch for ch in body)
    total, double = 0, True
    for ch in reversed(digits):
        d = int(ch)
        if double:
            d *= 2
            if d > 9:
                d -= 9
        total += d
        double = not double
    return body + str((10 - total % 10) % 10)


def fmp_class(label):
    """FMP's assetClass label -> reviewed class, None when the label is not in the vocabulary. Pure."""
    return FMP_CLASS.get(str(label or "").strip().lower()) or None


def load_etf_table(path=ETF_TABLE):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            doc = json.load(fh)
        return {str(k).upper(): str(v) for k, v in (doc.get("classes") or {}).items()}, doc.get("reviewed")
    except Exception:  # noqa: BLE001
        return {}, None


class Classer:
    """CUSIP -> (kind, sub, bucket, ticker, name) through the security master and the reviewed ETF table. Pure given
    its inputs; caches per identity. kind: stock | fund | debt | other | unmapped (options are never classed)."""

    def __init__(self, master_rows, etf_classes, learned=None):
        self.rows = master_rows or {}
        self.etf = etf_classes or {}
        self.learned = learned or {}       # ticker -> learned row ({"class": ..., "asset_class": raw FMP label, ...})
        self.cache = {}

    def of(self, identity):
        ident = identity or {}
        cusip = str(ident.get("cusip") or "").upper()
        qt = ident.get("quantity_type")
        key = (cusip, qt, bool(ident.get("put_call")))
        if key in self.cache:
            return self.cache[key]
        self.cache[key] = c = self._classify(cusip, qt, bool(ident.get("put_call")))
        return c

    def _classify(self, cusip, qt, option):
        row = self.rows.get(cusip) or {}
        st = row.get("status")
        typed = st == "resolved" or (st == "ambiguous" and row.get("type_agreed"))   # type known from every venue's row, ticker only when resolved
        base = {"ticker": row.get("ticker") if st == "resolved" else None, "name": row.get("name") if typed else None,
                "security_type": row.get("security_type") if typed else None, "master": st or "pending", "class_source": None}
        if option:
            return {**base, "kind": "option", "sub": "option", "bucket": None}
        if qt == "PRN":
            return {**base, "kind": "debt", "sub": "principal", "bucket": "bonds"}
        if not typed:
            return {**base, "kind": "unmapped", "sub": st or "pending", "bucket": "unmapped"}
        sector = row.get("market_sector") or ""
        stype = row.get("security_type") or ""
        stype2 = row.get("security_type2") or ""
        if sector == "Pref":   # OpenFIGI's preferred-securities sector (incl. mandatory convertibles reported in shares)
            return {**base, "kind": "stock", "sub": "preferred", "bucket": "equities"}
        if sector != "Equity":
            return {**base, "kind": "other", "sub": (sector or "unknown").lower(), "bucket": "other"}
        if is_fund_type(stype, stype2):
            t = str(row.get("ticker") or "").upper()
            cls = self.etf.get(t)
            if cls and cls in CLASS_BUCKET:
                return {**base, "kind": "fund", "sub": cls, "bucket": CLASS_BUCKET[cls], "class_source": "table"}
            lr = self.learned.get(t) or {}
            if lr.get("class") in CLASS_BUCKET:
                return {**base, "kind": "fund", "sub": lr["class"], "bucket": CLASS_BUCKET[lr["class"]], "class_source": "fmp"}
            return {**base, "kind": "fund", "sub": "unclassified", "bucket": "funds_unclassified", "fmp_asset_class": lr.get("asset_class")}
        return {**base, "kind": "stock", "sub": STOCK_SUBTYPES.get(stype, "equity_other"), "bucket": "equities"}


def is_fund_type(stype, stype2):
    stype, stype2 = str(stype or ""), str(stype2 or "")
    return stype in FUND_TYPES or stype2 in ("Mutual Fund", "ETP") or stype.startswith("ETF")


def in_universe(cls, universe):
    k = (cls or {}).get("kind")
    if universe == "all":
        return k in ("stock", "fund", "unmapped", "other")   # every SH identity (the equity book as the filings call it)
    if universe == "stocks":
        return k == "stock"
    if universe == "etfs":
        return k == "fund"
    if universe == "rotation":
        return k in ("stock", "fund", "unmapped", "other", "debt")
    return False


def split_positions(detail):
    """One native artifact -> (cur_p, pri_p, cur, pri, comp) where cur/pri hold every non-option position (SH and PRN)
    and the option rows are returned separately. Pure."""
    periods = detail.get("periods") or {}
    cur_p = detail.get("current_holdings_period")
    pri_p = detail.get("prior_holdings_period")
    cur = (periods.get(cur_p) or {}).get("positions") or {}
    pri = (periods.get(pri_p) or {}).get("positions") or {}
    comp = {r["position_id"]: r for r in ((detail.get("comparison") or {}).get("rows") or []) if isinstance(r, dict) and r.get("position_id")}
    return {"cur_p": cur_p, "pri_p": pri_p,
            "eq": {k: v for k, v in cur.items() if is_equity(v.get("identity"))},
            "eq_prior": {k: v for k, v in pri.items() if is_equity(v.get("identity"))},
            "debt": {k: v for k, v in cur.items() if is_debt(v.get("identity"))},
            "debt_prior": {k: v for k, v in pri.items() if is_debt(v.get("identity"))},
            "opt": {k: v for k, v in cur.items() if is_option(v.get("identity"))},
            "comp": comp}


def fund_boards(detail, classer=None):
    """Per-fund boards from one native artifact (holdings-native-fund.v1). Pure."""
    sp = split_positions(detail)
    cur_p, pri_p, eq, eq_prior, opt, comp = sp["cur_p"], sp["pri_p"], sp["eq"], sp["eq_prior"], sp["opt"], sp["comp"]
    classer = classer or Classer({}, {})
    total = sum((dec(v.get("reported_value_usd")) for v in eq.values()), Decimal(0))
    total_prior = sum((dec(v.get("reported_value_usd")) for v in eq_prior.values()), Decimal(0))
    puts = sum((dec(v.get("reported_value_usd")) for v in opt.values() if (v.get("identity") or {}).get("put_call") == "PUT"), Decimal(0))
    calls = sum((dec(v.get("reported_value_usd")) for v in opt.values() if (v.get("identity") or {}).get("put_call") == "CALL"), Decimal(0))

    def row(pid, pos, extra=None):
        ident = pos.get("identity") or {}
        c = classer.of(ident)
        r = {"id": pid[:12], "issuer": (pos.get("issuer_names") or [""])[0], "class": ident.get("class"), "cusip": ident.get("cusip"),
             "ticker": c["ticker"], "kind": c["kind"], "sub": c["sub"],
             "value_usd": f(dec(pos.get("reported_value_usd"))), "qty": f(dec(pos.get("reported_quantity"))),
             "pct_of_book": share(pos.get("reported_value_usd"), total)}
        if extra:
            r.update(extra)
        return r

    top = sorted(eq.items(), key=lambda kv: -dec(kv[1].get("reported_value_usd")))[:TOP]
    top_rows = []
    for pid, pos in top:
        c = comp.get(pid) or {}
        qc = c.get("reported_quantity_change")
        q0 = dec(eq_prior.get(pid, {}).get("reported_quantity")) if pid in eq_prior else Decimal(0)
        top_rows.append(row(pid, pos, {
            "status": c.get("status"),
            "qty_change": f(dec(qc)) if qc is not None else None,
            "qty_change_pct": f(round(100 * dec(qc) / q0, 1)) if qc is not None and q0 else None,
            "value_change_usd": f(dec(c.get("reported_value_change_usd"))) if c.get("reported_value_change_usd") is not None else None}))
    top10_pct = f(round(100 * sum((dec(p.get("reported_value_usd")) for _, p in top), Decimal(0)) / total, 1)) if total else None

    new_rows, absent_rows, inc_rows, dec_rows = [], [], [], []
    for pid, c in comp.items():
        st = c.get("status")
        if st == "newly_present_in_public_disclosure" and pid in eq:
            new_rows.append(row(pid, eq[pid]))
        elif st == "not_present_in_current_public_disclosure" and pid in eq_prior:
            p = eq_prior[pid]
            cl = classer.of(p.get("identity"))
            absent_rows.append({"id": pid[:12], "issuer": (p.get("issuer_names") or [""])[0], "class": (p.get("identity") or {}).get("class"),
                                "cusip": (p.get("identity") or {}).get("cusip"), "ticker": cl["ticker"], "kind": cl["kind"], "sub": cl["sub"],
                                "prior_value_usd": f(dec(p.get("reported_value_usd"))), "prior_qty": f(dec(p.get("reported_quantity")))})
        elif st in ("reported_quantity_increased", "reported_quantity_decreased") and pid in eq and pid in eq_prior:
            q0 = dec(eq_prior[pid].get("reported_quantity"))
            qc = dec(c.get("reported_quantity_change"))
            r = row(pid, eq[pid], {"qty_change": f(qc), "qty_change_pct": f(round(100 * qc / q0, 1)) if q0 else None,
                                   "value_change_usd": f(dec(c.get("reported_value_change_usd")))})
            (inc_rows if st == "reported_quantity_increased" else dec_rows).append(r)
    new_rows.sort(key=lambda r: -(r["value_usd"] or 0))
    absent_rows.sort(key=lambda r: -(r["prior_value_usd"] or 0))
    inc_rows.sort(key=lambda r: -(r["value_change_usd"] or 0))
    dec_rows.sort(key=lambda r: (r["value_change_usd"] or 0))
    counts = dict((detail.get("comparison") or {}).get("counts") or {})
    boards = {
        "current_period": cur_p, "prior_period": pri_p,
        "n_equity_positions": len(eq), "n_option_rows": len(opt), "n_debt_rows": len(sp["debt"]), "n_prior_equity_positions": len(eq_prior),
        "total_value_usd": f(total), "total_value_prior_usd": f(total_prior), "value_change_pct": pct(total, total_prior),
        "debt_value_usd": f(sum((dec(v.get("reported_value_usd")) for v in sp["debt"].values()), Decimal(0))),
        "put_value_usd": f(puts), "call_value_usd": f(calls),
        "top10_pct_of_book": top10_pct, "top": top_rows,
        "newly_present": new_rows[:TOP], "n_newly_present_equity": len(new_rows),
        "absent": absent_rows[:TOP], "n_absent_equity": len(absent_rows),
        "increased": inc_rows[:TOP], "n_increased_equity": len(inc_rows),
        "decreased": dec_rows[:TOP], "n_decreased_equity": len(dec_rows),
        "counts": counts,
        "shared_discretion_positions": sum(1 for v in eq.values() if v.get("other_managers")),
    }
    return boards, sp


def member(fk, sp):
    return {"fk": fk, **sp}


def aggregate(members, classer):
    """members: list of member dicts. One record per identity across the cohort: holders now / prior, reported values
    now / prior, the managers newly disclosing / absent / more / fewer / unchanged, and the identity's class. Pure."""
    agg = {}

    def slot(pid, pos):
        ident = pos.get("identity") or {}
        return agg.setdefault(pid, {"issuer": (pos.get("issuer_names") or [""])[0], "class": ident.get("class"), "cusip": ident.get("cusip"),
                                    "cls": classer.of(ident), "holders": [], "prior_holders": [], "value_usd": Decimal(0), "value_prior_usd": Decimal(0),
                                    "new": [], "absent": [], "inc": [], "dec": [], "flat": []})

    for m in members:
        fk, comp = m["fk"], m["comp"]
        cur = {**m["eq"], **m["debt"]}
        pri = {**m["eq_prior"], **m["debt_prior"]}
        for pid, pos in cur.items():
            a = slot(pid, pos)
            a["holders"].append(fk)
            a["value_usd"] += dec(pos.get("reported_value_usd"))
            st = (comp.get(pid) or {}).get("status")
            if st == "newly_present_in_public_disclosure":
                a["new"].append(fk)
            elif st == "reported_quantity_increased":
                a["inc"].append(fk)
            elif st == "reported_quantity_decreased":
                a["dec"].append(fk)
            elif st == "reported_quantity_unchanged":
                a["flat"].append(fk)
        for pid, pos in pri.items():
            a = slot(pid, pos)
            a["prior_holders"].append(fk)
            a["value_prior_usd"] += dec(pos.get("reported_value_usd"))
            if (comp.get(pid) or {}).get("status") == "not_present_in_current_public_disclosure":
                a["absent"].append(fk)
    absent_cusips = {a["cusip"] for a in agg.values() if a["absent"]}
    absent_issuers = {issuer_key(a["issuer"]) for a in agg.values() if a["absent"]}
    new_cusips = {a["cusip"] for a in agg.values() if a["new"]}
    new_issuers = {issuer_key(a["issuer"]) for a in agg.values() if a["new"]}
    for a in agg.values():
        ik = issuer_key(a["issuer"])
        reclass = bool(a["new"]) and (a["cusip"] in absent_cusips or ik in absent_issuers) and not a["absent"]
        reclass_abs = bool(a["absent"]) and not a["holders"] and (a["cusip"] in new_cusips or ik in new_issuers)
        a["reclass_candidate"] = reclass or reclass_abs
        a["no_current_holder"] = bool(a["absent"]) and not a["holders"]
    return agg


def stance_of(bull, bear):
    """Pattern label from manager counts. Pure."""
    if bull + bear == 0:
        return "QUIET"
    if bull > bear and (bear == 0 or bull >= 2 * bear):
        return "ACCUMULATING"
    if bear > bull and (bull == 0 or bear >= 2 * bull):
        return "DISTRIBUTING"
    return "CONTESTED"


def out_row(pid, a, skill=None):
    """One identity record for the page. Pure."""
    skill = skill or {}
    bull, bear = len(a["new"]) + len(a["inc"]), len(a["absent"]) + len(a["dec"])
    w_bull = [skill[k] for k in a["new"] + a["inc"] if isinstance(skill.get(k), (int, float))]
    w_bear = [skill[k] for k in a["absent"] + a["dec"] if isinstance(skill.get(k), (int, float))]
    c = a["cls"]
    return {"id": pid[:12], "issuer": a["issuer"], "class": a["class"], "cusip": a["cusip"], "ticker": c["ticker"], "name": c["name"], "kind": c["kind"], "sub": c["sub"],
            "bucket": c["bucket"], "security_type": c["security_type"],
            "n_holders": len(a["holders"]), "n_prior_holders": len(a["prior_holders"]), "holder_delta": len(a["holders"]) - len(a["prior_holders"]),
            "value_usd": f(a["value_usd"]), "value_prior_usd": f(a["value_prior_usd"]),
            "holders": sorted(a["holders"]), "new": sorted(a["new"]), "absent": sorted(a["absent"]), "inc": sorted(a["inc"]), "dec": sorted(a["dec"]),
            "n_new": len(a["new"]), "n_absent": len(a["absent"]), "n_inc": len(a["inc"]), "n_dec": len(a["dec"]), "n_flat": len(a["flat"]),
            "votes": {"bull": bull, "bear": bear, "net": bull - bear, "weighted_net": round((sum(w_bull) - sum(w_bear)) / 100.0, 2) if (w_bull or w_bear) else None,
                      "weighted_coverage": len(w_bull) + len(w_bear)},
            "stance": stance_of(bull, bear),
            "reclass_candidate": a["reclass_candidate"], "no_current_holder": a["no_current_holder"]}


def boards_from(agg, universe, skill=None):
    """The six boards for one universe (all / stocks / etfs). Pure."""
    items = [(p, a) for p, a in agg.items() if in_universe(a["cls"], universe)]
    rows = {p: out_row(p, a, skill) for p, a in items}
    held = [p for p, a in items if a["holders"]]
    return {
        "n_identities": len(items), "n_held": len(held),
        "value_usd": f(sum((a["value_usd"] for _, a in items), Decimal(0))),
        "widely_held": [rows[p] for p in sorted(held, key=lambda p: (-len(agg[p]["holders"]), -agg[p]["value_usd"]))][:BOARD],
        "largest_value": [rows[p] for p in sorted(held, key=lambda p: -agg[p]["value_usd"])][:BOARD],
        "newly_present": [rows[p] for p in sorted((p for p, a in items if a["new"]), key=lambda p: (-len(agg[p]["new"]), -agg[p]["value_usd"]))][:BOARD],
        "dropped": [rows[p] for p in sorted((p for p, a in items if a["absent"]), key=lambda p: (-len(agg[p]["absent"]), -len(agg[p]["holders"]), -agg[p]["value_prior_usd"]))][:BOARD],
        "increased": [rows[p] for p in sorted((p for p, a in items if a["inc"]), key=lambda p: (-len(agg[p]["inc"]), -agg[p]["value_usd"]))][:BOARD],
        "decreased": [rows[p] for p in sorted((p for p, a in items if a["dec"]), key=lambda p: (-len(agg[p]["dec"]), -agg[p]["value_usd"]))][:BOARD],
    }


def stance_from(agg, universe, skill=None):
    """Names the cohort agrees on: net votes >= MIN_AGREEMENT (bullish) or <= -MIN_AGREEMENT (bearish), ranked by net
    votes, then SKILL-weighted net, then reported value. Pure."""
    rows = [out_row(p, a, skill) for p, a in agg.items() if in_universe(a["cls"], universe) and not a["reclass_candidate"]]
    def wkey(r):
        return (r["votes"]["weighted_net"] if r["votes"]["weighted_net"] is not None else 0.0)
    bull = sorted((r for r in rows if r["votes"]["net"] >= MIN_AGREEMENT), key=lambda r: (-r["votes"]["net"], -wkey(r), -(r["value_usd"] or 0)))
    bear = sorted((r for r in rows if r["votes"]["net"] <= -MIN_AGREEMENT), key=lambda r: (r["votes"]["net"], wkey(r), -(r["value_prior_usd"] or 0)))
    contested = sorted((r for r in rows if r["stance"] == "CONTESTED" and r["votes"]["bull"] + r["votes"]["bear"] >= 2 * MIN_AGREEMENT),
                       key=lambda r: (-(r["votes"]["bull"] + r["votes"]["bear"]), -(r["value_usd"] or 0)))
    counts = Counter(r["stance"] for r in rows)
    return {"bullish": bull[:STANCE_TOP], "bearish": bear[:STANCE_TOP], "contested": contested[:STANCE_TOP],
            "n_bullish": len(bull), "n_bearish": len(bear), "n_contested": len(contested),
            "counts": {k: counts.get(k, 0) for k in ("ACCUMULATING", "DISTRIBUTING", "CONTESTED", "QUIET")}, "min_agreement": MIN_AGREEMENT}


def contenders_from(agg, universe, skill=None):
    """Holder-count traction: names whose number of cohort holders rose or fell by >= MIN_AGREEMENT between the two
    disclosures; new_to_cohort when no cohort member reported it last period. Pure."""
    rows = [out_row(p, a, skill) for p, a in agg.items() if in_universe(a["cls"], universe) and not a["reclass_candidate"]]
    rising = sorted((r for r in rows if r["holder_delta"] >= MIN_AGREEMENT), key=lambda r: (-r["holder_delta"], -r["n_holders"], -(r["value_usd"] or 0)))
    fading = sorted((r for r in rows if r["holder_delta"] <= -MIN_AGREEMENT), key=lambda r: (r["holder_delta"], -r["n_prior_holders"], -(r["value_prior_usd"] or 0)))
    for r in rising:
        r["new_to_cohort"] = r["n_prior_holders"] == 0
    for r in fading:
        r["left_cohort"] = r["n_holders"] == 0
    return {"rising": rising[:STANCE_TOP], "fading": fading[:STANCE_TOP], "n_rising": len(rising), "n_fading": len(fading),
            "n_new_to_cohort": sum(1 for r in rising if r["new_to_cohort"]), "n_left_cohort": sum(1 for r in fading if r["left_cohort"])}


def bucket_stance(up, down):
    """Class-level word: with few pairs the per-identity rule; otherwise the net share of all pairs (tilt). Pure."""
    if up + down < 4:
        return stance_of(up, down)
    tilt = 100.0 * (up - down) / (up + down)
    return "ACCUMULATING" if tilt >= TILT_PCT else "DISTRIBUTING" if tilt <= -TILT_PCT else "BALANCED"


def rotation(members, classer):
    """Asset-class rotation across members: reported value now / prior per bucket (prior at prior-quarter prices), the
    manager-identity pairs newly disclosing / absent / more / fewer, and the top names. Pure."""
    agg = aggregate(members, classer)
    b = {}

    def slot(bucket):
        return b.setdefault(bucket, {"value_usd": Decimal(0), "value_prior_usd": Decimal(0), "identities": 0, "identities_prior": 0, "holders": set(),
                                     "pairs_new": 0, "pairs_absent": 0, "pairs_more": 0, "pairs_fewer": 0, "pairs_flat": 0, "top": []})
    for p, a in agg.items():
        if not in_universe(a["cls"], "rotation"):
            continue
        s = slot(a["cls"]["bucket"])
        s["value_usd"] += a["value_usd"]
        s["value_prior_usd"] += a["value_prior_usd"]
        s["identities"] += 1 if a["holders"] else 0
        s["identities_prior"] += 1 if a["prior_holders"] else 0
        s["holders"].update(a["holders"])
        s["pairs_new"] += len(a["new"])
        s["pairs_absent"] += len(a["absent"])
        s["pairs_more"] += len(a["inc"])
        s["pairs_fewer"] += len(a["dec"])
        s["pairs_flat"] += len(a["flat"])
        if a["holders"]:
            s["top"].append((a["value_usd"], a["cls"]["ticker"] or a["issuer"], len(a["holders"]), p[:12]))
    total = sum((s["value_usd"] for s in b.values()), Decimal(0))
    total_prior = sum((s["value_prior_usd"] for s in b.values()), Decimal(0))
    rows = []
    for bucket in BUCKET_ORDER:
        s = b.get(bucket)
        if not s:
            continue
        up, down = s["pairs_new"] + s["pairs_more"], s["pairs_absent"] + s["pairs_fewer"]
        tilt = round(100.0 * (up - down) / (up + down), 1) if (up + down) else None
        sh_now, sh_pri = share(s["value_usd"], total), share(s["value_prior_usd"], total_prior)
        rows.append({"bucket": bucket, "label": BUCKET_LABEL[bucket], "value_usd": f(s["value_usd"]), "value_prior_usd": f(s["value_prior_usd"]),
                     "value_change_pct": pct(s["value_usd"], s["value_prior_usd"]), "share_pct": sh_now, "share_prior_pct": sh_pri,
                     "share_delta_pp": round(sh_now - sh_pri, 2) if sh_now is not None and sh_pri is not None else None,
                     "n_identities": s["identities"], "n_identities_prior": s["identities_prior"], "n_managers": len(s["holders"]),
                     "pairs": {"new": s["pairs_new"], "absent": s["pairs_absent"], "more": s["pairs_more"], "fewer": s["pairs_fewer"], "flat": s["pairs_flat"]},
                     "net_pairs": up - down, "tilt_pct": tilt, "stance": bucket_stance(up, down),
                     "top": [{"name": n, "n_holders": h, "value_usd": f(v), "id": i} for v, n, h, i in sorted(s["top"], key=lambda t: -t[0])[:5]]})
    unmapped = next((r for r in rows if r["bucket"] == "unmapped"), None)
    return {"buckets": rows, "value_usd": f(total), "value_prior_usd": f(total_prior), "n_identities": sum(r["n_identities"] for r in rows),
            "unmapped_value_pct": unmapped["share_pct"] if unmapped else 0.0,
            "note": ("Reported values at each period's own prices (a bucket's change carries the quarter's move); 'pairs' count manager x identity "
                     "reports across the two disclosures - a pattern, not a measured flow; tilt = net pairs as a share of all pairs, ACCUMULATING / "
                     "DISTRIBUTING beyond +/-%d%%, BALANCED between." % int(TILT_PCT))}


def status_of(meta, boards, required):
    """Primary status chip + flags for one manager. Pure."""
    ls = meta.get("latest_submission") or {}
    form = str(ls.get("form") or "")
    flags = []
    if meta.get("valuation_reviews"):
        flags.append("VALUATION_REVIEW")
    if boards.get("n_option_rows", 0) >= HEDGE_OPTION_ROWS:
        flags.append("HEDGE_BOOK")
    period = boards.get("current_period")
    if form == "13F-NT" and period != required:
        return "NOTICE", flags
    if period == required and form.endswith("/A"):
        return "AMENDED", flags
    if period == required:
        return "CURRENT", flags
    return "DATED", flags


def quarter_end_after(day):
    """The first calendar quarter end strictly after `day` (date). Pure."""
    y, m = day.year, day.month
    for qm in (3, 6, 9, 12):
        qe = date(y, qm, [31, 30, 30, 31][(qm // 3) - 1])
        if qe > day:
            return qe
    return date(y + 1, 3, 31)


def us_federal_holidays(year):
    """Observed US federal holidays for a year (weekend observance shifted). Pure."""
    def nth_weekday(month, weekday, n):
        d = date(year, month, 1)
        d += timedelta(days=(weekday - d.weekday()) % 7)
        return d + timedelta(weeks=n - 1)

    def last_weekday(month, weekday):
        d = date(year, month + 1, 1) - timedelta(days=1) if month < 12 else date(year, 12, 31)
        return d - timedelta(days=(d.weekday() - weekday) % 7)

    fixed = [date(year, 1, 1), date(year, 6, 19), date(year, 7, 4), date(year, 11, 11), date(year, 12, 25)]
    out = set()
    for d in fixed:
        if d.weekday() == 5:
            out.add(d - timedelta(days=1))
        elif d.weekday() == 6:
            out.add(d + timedelta(days=1))
        else:
            out.add(d)
    out.add(nth_weekday(1, 0, 3))    # MLK
    out.add(nth_weekday(2, 0, 3))    # Presidents
    out.add(last_weekday(5, 0))      # Memorial
    out.add(nth_weekday(9, 0, 1))    # Labor
    out.add(nth_weekday(10, 0, 2))   # Columbus
    out.add(nth_weekday(11, 3, 4))   # Thanksgiving
    return out


def filing_deadline(period_end):
    """13F deadline: 45 days after quarter end, rolled to the next business day. Pure."""
    d = period_end + timedelta(days=45)
    hol = us_federal_holidays(d.year)
    while d.weekday() >= 5 or d in hol:
        d += timedelta(days=1)
    return d


def season(required, today, detector, desk_by_fund):
    """Next period, deadline, days left and who has already filed for it. Pure."""
    req = date.fromisoformat(required) if required else None
    nxt = quarter_end_after(req) if req else quarter_end_after(today)
    deadline = filing_deadline(nxt)
    filed = []
    for fk, m in (desk_by_fund or {}).items():
        ls = (m or {}).get("latest_submission") or {}
        if ls.get("period_of_report") == nxt.isoformat():
            filed.append(fk)
    for fk, m in ((detector or {}).get("by_fund") or {}).items():
        lf = (m or {}).get("latest_filing") or {}
        if lf.get("period_of_report") == nxt.isoformat() and fk not in filed:
            filed.append(fk)
    return {"required_period": required, "next_period": nxt.isoformat(), "deadline": deadline.isoformat(),
            "deadline_rule": "45 days after quarter end, next business day when that is a weekend or US federal holiday",
            "days_to_deadline": (deadline - today).days, "filed_for_next": sorted(filed), "n_filed_for_next": len(filed),
            "tracked": len(desk_by_fund or {}),
            "detector": {"generated_at": (detector or {}).get("generated_at"), "tracked_funds": (detector or {}).get("tracked_funds"),
                         "filings_seen": (detector or {}).get("filings_seen"), "fetch_errors": (detector or {}).get("fetch_errors"),
                         "new_filings": ((detector or {}).get("new_filings") or [])[:20]}}


def clone_summary(managers):
    rows = {}
    for key, m in (managers or {}).items():
        if not isinstance(m, dict):
            continue
        row = {k: m.get(k) for k in ("name", "n_windows", "hedge_book", "ann_clone_pct", "ann_spy_pct", "ann_alpha_pct", "hit_rate", "ir",
                                     "best_alpha", "worst_alpha", "skill_score", "label", "note", "reason", "status") if k in m}
        w = m.get("windows") or []
        if w:
            row["last_window"] = {k: w[-1].get(k) for k in ("q", "entry", "exit", "ret", "spy", "alpha", "cov")}
            row["windows"] = [{k: x.get(k) for k in ("q", "ret", "spy", "alpha")} for x in w]
        rows[key] = row
    return rows


def tier(age_h, expected_h=24):
    """fresh / stale / dead against the engine's own cadence: fresh within 1.25x its expected interval (30 h for a
    daily engine - the fleet-monitor tier), stale within 7x, dead beyond; a None cadence (DISABLED trigger) is
    reported as 'unscheduled' with the age shown but never judged. Pure."""
    if age_h is None:
        return "missing"
    if expected_h is None:
        return "unscheduled"
    fresh = max(FRESH_H, expected_h * 1.25)
    if age_h < fresh:
        return "fresh"
    if age_h < max(DEAD_H, expected_h * 7):
        return "stale"
    return "dead"


def sum_counts(managers, keys):
    c = Counter()
    for fk in keys:
        for k, v in (managers.get(fk, {}).get("counts") or {}).items():
            c[k] += int(v or 0)
    return dict(c)


# ───────────────────────────── security master (OpenFIGI, keyed) ─────────────────────────────
def parse_ts(s):
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def master_due(rows, universe, now):
    """Which CUSIPs to (re)query, in priority order: never-checked SH identities by reported value, then PRN, then
    re-checks whose per-status interval has passed. Pure."""
    due = []
    for c, u in universe.items():
        r = rows.get(c)
        if r is None:
            due.append((0 if u["sh"] else 1, -u["value"], c))
            continue
        checked = parse_ts(r.get("checked_at"))
        days = RECHECK_DAYS.get(r.get("status"), 1)
        if r.get("status") != "resolved" and r.get("scheme") != SCHEME:
            due.append((2, -u["value"], c))          # the query scheme improved since this unresolved row was written
        elif checked is None or (now - checked) >= timedelta(days=days):
            due.append((3, -u["value"], c))
    due.sort()
    return [c for _, _, c in due]


def _collapse(rows):
    """Multiple OpenFIGI rows for one identifier -> the one row that identifies the security, or None (ambiguous):
    the single US composite row; several US rows that agree on ticker and type (share-class listings); or one
    composite across every venue (the composite row). Pure."""
    rows = [r for r in rows if isinstance(r, dict)]
    us = [r for r in rows if r.get("exchCode") == "US"]
    if len(us) == 1:
        return us[0]
    if len(us) > 1:
        primary = [r for r in us if not NAV_TICKER.fullmatch(str(r.get("ticker") or ""))]   # Bloomberg's NAV-quote tickers for closed-end funds (XFSLX is the NAV of FSSL)
        if len(primary) == 1:
            return primary[0]
    if us and len({(r.get("ticker"), r.get("securityType")) for r in us}) == 1:
        return next((r for r in us if r.get("figi") == r.get("compositeFIGI")), us[0])
    comps = {r.get("compositeFIGI") for r in rows}
    if len(comps) == 1 and None not in comps:
        comp = comps.pop()
        return next((r for r in rows if r.get("figi") == comp), rows[0])
    return None


def master_row(item, now_iso, query):
    """One OpenFIGI mapping result -> a master row. Pure."""
    res = openfigi.resolution(item)
    st = res.get("status")
    if st == "resolved" or (st == "ambiguous" and _collapse(item.get("data") or [])):
        row = res.get("security") if st == "resolved" else _collapse(item.get("data") or [])
        return {"status": "resolved", "ticker": row.get("ticker"), "name": row.get("name"), "figi": row.get("figi"),
                "composite_figi": row.get("compositeFIGI"), "share_class_figi": row.get("shareClassFIGI"),
                "security_type": row.get("securityType"), "security_type2": row.get("securityType2"), "market_sector": row.get("marketSector"),
                "exch_code": row.get("exchCode"), "checked_at": now_iso, "query": query, "scheme": SCHEME, "n_rows": len(item.get("data") or [])}
    if st == "ambiguous":
        data = [r for r in (item.get("data") or []) if isinstance(r, dict)]
        data.sort(key=lambda r: 0 if r.get("exchCode") == "US" else 1 if str(r.get("exchCode") or "").startswith("U") else 2)
        cands = [{k: r.get(k) for k in ("ticker", "exchCode", "name", "figi", "securityType", "marketSector", "compositeFIGI")} for r in data[:MASTER_CANDIDATES]]
        out = {"status": "ambiguous", "reason": res.get("reason"), "candidates": cands, "n_rows": len(data), "checked_at": now_iso, "query": query, "scheme": SCHEME}
        types = {(r.get("securityType"), r.get("securityType2"), r.get("marketSector")) for r in data}
        if len(types) == 1:   # every venue agrees on what the security is: the type is known even when no US ticker is
            stype, stype2, sector = types.pop()
            out.update({"type_agreed": True, "security_type": stype, "security_type2": stype2, "market_sector": sector, "name": data[0].get("name")})
        return out
    if st == "no_match":
        return {"status": "no_match", "reason": res.get("reason"), "checked_at": now_iso, "query": query, "scheme": SCHEME}
    reason = (item.get("error") if isinstance(item, dict) else None) or res.get("reason")
    return {"status": "error", "reason": str(reason)[:120], "checked_at": now_iso, "query": query, "scheme": SCHEME}


def _send(ids, make_job, api_key, deadline, batch, stats, now_iso, query, log):
    """One OpenFIGI pass over `ids` (list of identifiers). Returns (results dict id -> master row or None when the
    answer must not be written (deadline), set of ids that came back no_match)."""
    if not ids:
        return {}, []
    jobs = [make_job(c) for c in ids]
    res = openfigi.mapping(jobs, api_key=api_key, deadline=deadline)   # None api_key -> the client's own (cached) SSM read -> anonymous pacing when absent
    stats["requests"] += math.ceil(len(jobs) / batch)
    stats["sent"] += len(jobs)
    if res is None:
        res = [{"error": "request failed"} for _ in jobs]
    out, missed = {}, []
    for c, item in zip(ids, res):
        if isinstance(item, dict) and item.get("error") == "deadline exhausted":
            stats["deadline_hit"] = True
            continue
        row = master_row(item, now_iso, query)
        if row["status"] == "no_match":
            missed.append(c)
        out[c] = row
    log("security master pass %s: %d jobs, %d answered, %d no_match" % (query, len(jobs), len(out), len(missed)))
    return out, missed


def master_refresh(rows, universe, now, api_key, deadline_s, budget_requests, log=print):
    """Query OpenFIGI for the due identifiers within a request budget and a wall-clock deadline, three passes:
      A. identifier typed by its shape (ID_CUSIP for digit-led, ID_CINS for letter-led) restricted to exchCode US;
      B. the same identifier on any exchange for A's no-matches (collapsed to the US row / one composite when possible);
      C. the ISIN defined by the CUSIP ('US' + CUSIP + check digit) on exchCode US for digit-led identifiers still
         unresolved after B (OpenFIGI withholds some US CUSIP links but answers the ISIN).
    Mutates and returns rows; returns stats. Network only through openfigi.mapping (fail-soft)."""
    started = time.time()
    now_iso = now.isoformat(timespec="seconds")
    due = master_due(rows, universe, now)
    batch = 100 if api_key else 10
    deadline = time.monotonic() + max(1.0, deadline_s)
    stats = {"universe": len(universe), "due": len(due), "sent": 0, "sent_us": 0, "sent_any": 0, "sent_isin": 0, "requests": 0, "keyed": bool(api_key),
             "deadline_s": deadline_s, "budget_requests": budget_requests, "deadline_hit": False, "changed": 0, "scheme": SCHEME}

    def room():
        return max(0, budget_requests - stats["requests"]) * batch

    # A. typed, US
    first = due[:min(room(), max(1, int(budget_requests * 0.6)) * batch)]
    a_rows, a_missed = _send(first, lambda c: {"idType": id_type(c), "idValue": c, "exchCode": "US"}, api_key, deadline, batch, stats, now_iso, "typed/US", log)
    stats["sent_us"] = len(first)
    pending = {}
    for c, row in a_rows.items():
        if row["status"] == "no_match":
            pending[c] = row            # written only if nothing better comes from B / C
        else:
            rows[c] = row
            stats["changed"] += 1
    # B. typed, any exchange
    second = a_missed[:room()] if time.monotonic() < deadline else []
    b_rows, b_missed = _send(second, lambda c: {"idType": id_type(c), "idValue": c}, api_key, deadline, batch, stats, now_iso, "typed/any", log)
    stats["sent_any"] = len(second)
    isin_candidates = []
    for c, row in b_rows.items():
        if row["status"] == "resolved":
            rows[c] = row
            stats["changed"] += 1
            pending.pop(c, None)
        else:
            pending[c] = row            # ambiguous abroad or no match anywhere: try the ISIN for US CUSIPs
            if id_type(c) == "ID_CUSIP":
                isin_candidates.append(c)
    # C. ISIN derived from the CUSIP, US
    third = isin_candidates[:room()] if time.monotonic() < deadline else []
    c_rows, _ = _send(third, lambda c: {"idType": "ID_ISIN", "idValue": isin_from_cusip(c), "exchCode": "US"}, api_key, deadline, batch, stats, now_iso, "isin/US", log)
    stats["sent_isin"] = len(third)
    for c, row in c_rows.items():
        if row["status"] == "resolved":
            rows[c] = row | {"isin": isin_from_cusip(c)}
            stats["changed"] += 1
            pending.pop(c, None)
        elif pending.get(c, {}).get("status") == "no_match" and row["status"] == "ambiguous":
            pending[c] = row            # keep the richer answer (candidates) over a bare no-match
    a_missed_set, second_set, isin_set, third_set = set(a_missed), set(second), set(isin_candidates), set(third)
    stats["partial"] = 0
    for c, row in pending.items():
        partial = (c in a_missed_set and c not in second_set) or (c in isin_set and c not in third_set)
        if partial:                     # a later pass never ran for it (budget or deadline): keep the answer, stay due for the next run
            row = row | {"scheme": SCHEME + "-partial"}
            stats["partial"] += 1
        rows[c] = row
        stats["changed"] += 1
    stats["seconds"] = round(time.time() - started, 1)
    stats["pending_after"] = len([c for c in universe if c not in rows])
    log("security master: universe %d due %d sent %d (US %d, any %d, isin %d) requests %d keyed %s changed %d pending %d %.1fs%s" % (
        len(universe), len(due), stats["sent"], stats["sent_us"], stats["sent_any"], stats["sent_isin"], stats["requests"], stats["keyed"], stats["changed"],
        stats["pending_after"], stats["seconds"], " DEADLINE" if stats["deadline_hit"] else ""))
    return stats


def learn_etf_classes(learned, rows, universe, etf_classes, now, budget, log=print):
    """For fund-like resolved identities the reviewed table does not know, read FMP's own assetClass from the banked
    etf/info row (S3 only, fail-soft) and keep it in `learned` (ticker -> row). Returns stats. Mutates learned."""
    started = time.time()
    now_iso = now.isoformat(timespec="seconds")
    want = {}
    for c, u in universe.items():
        r = rows.get(c) or {}
        if r.get("status") != "resolved" or not is_fund_type(r.get("security_type"), r.get("security_type2")):
            continue
        t = str(r.get("ticker") or "").upper()
        if not t or t in etf_classes:
            continue
        lr = learned.get(t)
        if lr:
            checked = parse_ts(lr.get("checked_at"))
            days = LEARNED_RECHECK_DAYS["banked" if lr.get("banked") else "missing"]
            if checked is not None and (now - checked) < timedelta(days=days):
                continue
        want[t] = max(want.get(t, Decimal(0)), u["value"])
    order = sorted(want, key=lambda t: -want[t])[:max(0, budget)]
    stats = {"fund_like_unknown_to_table": len(want), "looked_up": 0, "banked": 0, "classed": 0, "unknown_label": 0, "errors": 0, "labels": {}}
    for t in order:
        stats["looked_up"] += 1
        try:
            doc = fmp_bank.walker("etf-info", t)
        except Exception:  # noqa: BLE001
            stats["errors"] += 1
            doc = None
        row0 = doc[0] if isinstance(doc, list) and doc and isinstance(doc[0], dict) else (doc if isinstance(doc, dict) else None)
        if row0 is None:
            learned[t] = {"banked": False, "asset_class": None, "class": None, "checked_at": now_iso, "source": "fmp-bank etf/info (not banked)"}
            continue
        label = row0.get("assetClass")
        cls = fmp_class(label)
        learned[t] = {"banked": True, "asset_class": label, "class": cls, "name": row0.get("name"), "cusip": row0.get("securityCusip") or row0.get("cusip"),
                      "updated_at": row0.get("updatedAt"), "checked_at": now_iso, "source": "fmp-bank etf/info"}
        stats["banked"] += 1
        stats["labels"][str(label)] = stats["labels"].get(str(label), 0) + 1
        if cls:
            stats["classed"] += 1
        else:
            stats["unknown_label"] += 1
    stats["seconds"] = round(time.time() - started, 1)
    stats["learned_total"] = len(learned)
    log("etf classes learned: %d fund-like tickers unknown to the table, looked up %d, banked %d, classed %d, unknown label %d, errors %d, %.1fs" % (
        len(want), stats["looked_up"], stats["banked"], stats["classed"], stats["unknown_label"], stats["errors"], stats["seconds"]))
    return stats

def master_coverage(rows, universe):
    """Status counts over the current universe, by identity and by reported value. Pure."""
    c, v = Counter(), Counter()
    for cusip, u in universe.items():
        st = (rows.get(cusip) or {}).get("status") or "pending"
        c[st] += 1
        v[st] += u["value"]
    total_v = sum(v.values()) or Decimal(1)
    typed = sum(1 for cusip in universe if (rows.get(cusip) or {}).get("status") == "ambiguous" and (rows.get(cusip) or {}).get("type_agreed"))
    typed_v = sum((u["value"] for cusip, u in universe.items() if (rows.get(cusip) or {}).get("status") == "ambiguous" and (rows.get(cusip) or {}).get("type_agreed")), Decimal(0))
    return {"identities": len(universe), **{k: c.get(k, 0) for k in ("resolved", "no_match", "ambiguous", "error", "pending")},
            "typed_only": typed, "typed_only_value_pct": f(round(100 * typed_v / total_v, 1)) if universe else None,
            "resolved_pct": round(100.0 * c.get("resolved", 0) / len(universe), 1) if universe else None,
            "resolved_value_pct": f(round(100 * v.get("resolved", Decimal(0)) / total_v, 1)) if universe else None}


# ───────────────────────────── AWS I/O ─────────────────────────────
def _s3():
    import boto3
    from botocore.config import Config
    return boto3.client("s3", config=Config(retries={"max_attempts": 5, "mode": "standard"}, connect_timeout=10, read_timeout=120))


def get_json(s3, key):
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    body = obj["Body"].read()
    return json.loads(body or b"{}"), obj["LastModified"], len(body)


def put_json(s3, key, doc, cache="max-age=300"):
    body = json.dumps(doc, separators=(",", ":"), ensure_ascii=False, default=str, allow_nan=False).encode("utf-8")
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/json; charset=utf-8", CacheControl=cache)
    return len(body)


def head(s3, key, now, expected_h=24):
    try:
        h = s3.head_object(Bucket=S3_BUCKET, Key=key)
        age = (now - h["LastModified"]).total_seconds() / 3600
        return {"key": key, "bytes": h["ContentLength"], "last_modified": h["LastModified"].isoformat(timespec="seconds"), "age_h": round(age, 1), "tier": tier(age, expected_h)}
    except Exception as exc:  # noqa: BLE001
        return {"key": key, "bytes": None, "last_modified": None, "age_h": None, "tier": "missing", "error": type(exc).__name__}


def list_periods(s3):
    try:
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=PERIODS_PREFIX, MaxKeys=200)
        return sorted([{"key": o["Key"], "period": o["Key"][len(PERIODS_PREFIX):].replace(".json", ""), "bytes": o["Size"],
                        "last_modified": o["LastModified"].isoformat(timespec="seconds")} for o in resp.get("Contents", []) if o["Key"].endswith(".json")],
                      key=lambda r: r["period"])
    except Exception as exc:  # noqa: BLE001
        return [{"error": type(exc).__name__}]


def period_snapshot(required, cohort_keys, agg, now):
    """Compact per-period record of the cohort's identities (for traction across quarters). Pure."""
    by_id = {}
    for p, a in agg.items():
        if not in_universe(a["cls"], "rotation") or not (a["holders"] or a["prior_holders"]):
            continue
        by_id[p[:12]] = [a["cls"]["ticker"], a["cls"]["kind"], a["cls"]["bucket"], len(a["holders"]), f(a["value_usd"]), len(a["prior_holders"]), a["cusip"]]
    return {"contract": "13f-book-period.v1", "engine": ENGINE, "period": required, "generated_at": now.isoformat(timespec="seconds"),
            "cohort": sorted(cohort_keys), "n_identities": len(by_id),
            "columns": ["ticker", "kind", "bucket", "n_holders", "value_usd", "n_prior_holders", "cusip"], "by_identity": by_id}


def build(s3, now, log=print, remaining_s=300.0, master_enabled=True):
    started = time.time()
    desk, desk_mod, desk_bytes = get_json(s3, DESK_KEY)
    required = desk.get("required_period_for_current_cohort")
    by_fund = desk.get("by_fund") or {}
    etf_classes, etf_reviewed = load_etf_table()

    # 1. load every artifact once; collect the CUSIP universe (all managers, so drill-downs carry tickers too)
    loaded, errors = {}, []
    universe = {}
    for fk in sorted(by_fund):
        meta = by_fund[fk] or {}
        key = (meta.get("detail") or {}).get("key")
        if not key:
            errors.append({"fund": fk, "error": "no artifact key in the desk"})
            continue
        try:
            detail, mod, size = get_json(s3, key)
        except Exception as exc:  # noqa: BLE001
            errors.append({"fund": fk, "error": "%s: %s" % (type(exc).__name__, str(exc)[:120])})
            continue
        loaded[fk] = (meta, key, detail, mod, size)
        sp = split_positions(detail)
        for group, sh in (("eq", True), ("eq_prior", True), ("debt", False), ("debt_prior", False)):
            for pos in sp[group].values():
                c = str(((pos.get("identity") or {}).get("cusip")) or "").upper()
                if not re.fullmatch(r"[A-Z0-9*@#]{8}[0-9]", c):
                    continue
                u = universe.setdefault(c, {"value": Decimal(0), "sh": False})
                u["value"] += dec(pos.get("reported_value_usd"))
                u["sh"] = u["sh"] or sh

    # 2. the security master: read, refresh within budget, write back when changed
    try:
        master, master_mod, master_bytes = get_json(s3, MASTER_KEY)
        master_err = None
    except Exception as exc:  # noqa: BLE001
        master, master_mod, master_bytes, master_err = {}, None, None, type(exc).__name__
    rows = master.get("by_cusip") if isinstance(master.get("by_cusip"), dict) else {}
    refresh = {"skipped": True, "reason": "disabled"}
    if master_enabled:
        budget_s = max(0.0, min(MASTER_BUDGET_S, remaining_s - (time.time() - started) - MASTER_RESERVE_S))
        if budget_s < 5:
            refresh = {"skipped": True, "reason": "no time left (%.0fs remaining)" % remaining_s}
        else:
            api_key = openfigi.get_api_key()   # SSM; None -> anonymous pacing (10 jobs / 6 s)
            refresh = master_refresh(rows, universe, now, api_key, budget_s, MASTER_BUDGET_REQUESTS, log)
            refresh["skipped"] = False
            if refresh.get("changed"):
                doc = {"contract": "13f-security-master.v1", "engine": ENGINE, "version": VERSION, "generated_at": now.isoformat(timespec="seconds"),
                       "source": "OpenFIGI v3 /mapping, idType ID_CUSIP, exchCode US first then any exchange; keyed via SSM when available",
                       "recheck_days": RECHECK_DAYS, "n_rows": len(rows), "last_refresh": refresh, "by_cusip": rows}
                master_bytes = put_json(s3, MASTER_KEY, doc, cache="max-age=600")
                master_mod = now
    coverage = master_coverage(rows, universe)

    # 2b. the learned ETF classes (FMP's own assetClass from the bank) for fund-like tickers the reviewed table lacks
    try:
        learned_doc, learned_mod, _ = get_json(s3, LEARNED_KEY)
        learned_err = None
    except Exception as exc:  # noqa: BLE001
        learned_doc, learned_mod, learned_err = {}, None, type(exc).__name__
    learned = learned_doc.get("by_ticker") if isinstance(learned_doc.get("by_ticker"), dict) else {}
    learn_stats = {"skipped": True, "reason": "disabled"}
    if master_enabled:
        learn_stats = learn_etf_classes(learned, rows, universe, etf_classes, now, FMP_LOOKUPS_PER_RUN, log)
        learn_stats["skipped"] = False
        if learn_stats.get("looked_up"):
            put_json(s3, LEARNED_KEY, {"contract": "13f-etf-classes-learned.v1", "engine": ENGINE, "version": VERSION, "generated_at": now.isoformat(timespec="seconds"),
                                       "source": "FMP etf/info assetClass as banked by justhodl-fmp-walker (data/warm/fmp/etf-info/<ticker>.json.gz); mapped through FMP_CLASS",
                                       "vocabulary": FMP_CLASS, "recheck_days": LEARNED_RECHECK_DAYS, "n_rows": len(learned), "last_run": learn_stats, "by_ticker": learned}, cache="max-age=600")
            learned_mod = now
    classer = Classer(rows, etf_classes, learned)

    # 3. per-manager boards
    managers, members, members_conc = {}, [], []
    for fk, (meta, key, detail, mod, size) in loaded.items():
        boards, sp = fund_boards(detail, classer)
        status, flags = status_of(meta, boards, required)
        ls = meta.get("latest_submission") or {}
        mem = member(fk, sp)
        mix = rotation([mem], classer)
        boards.update({
            "fund_key": fk, "display_name": DISPLAY.get(fk, fk.title()), "short": SHORT.get(fk, fk[:3]),
            "official_name": meta.get("official_name") or meta.get("name"), "cik": meta.get("cik"),
            "latest_submission": {k: ls.get(k) for k in ("form", "filed_at", "accepted_at", "accession", "period_of_report")},
            "effective_accessions": meta.get("effective_accessions"), "chain_status": meta.get("current_chain_status"),
            "cohort": bool(meta.get("current_cohort_eligible")), "observation_age_days": meta.get("observation_age_days"),
            "status": status, "flags": flags, "valuation_reviews": meta.get("valuation_reviews") or [],
            "artifact": {"key": key, "bytes": size, "sha256": (meta.get("detail") or {}).get("sha256"), "last_modified": mod.isoformat(timespec="seconds")},
            "mix": {"buckets": [{k: r[k] for k in ("bucket", "label", "value_usd", "value_prior_usd", "share_pct", "share_prior_pct", "share_delta_pp",
                                                   "n_identities", "pairs", "net_pairs", "stance")} for r in mix["buckets"]],
                    "unmapped_value_pct": mix["unmapped_value_pct"]},
            "filings": [{k: v for k, v in (fs or {}).items() if k in ("accession", "form", "period_of_report", "filed_at", "accepted_at")} | {
                "amendment_number": ((fs or {}).get("cover") or {}).get("amendment_number"),
                "amendment_type": ((fs or {}).get("cover") or {}).get("amendment_type"),
                "reported_rows": ((fs or {}).get("cover") or {}).get("reported_rows"),
                "reported_value_sum": ((fs or {}).get("cover") or {}).get("reported_value_sum"),
                "confidential_omissions_declared": ((fs or {}).get("cover") or {}).get("confidential_omissions_declared")}
                        for fs in (detail.get("filing_summaries") or {}).values()],
        })
        managers[fk] = boards
        if boards["cohort"] and boards["current_period"] == required:
            members.append(mem)
            if boards["n_equity_positions"] <= CONCENTRATED_MAX:
                members_conc.append(mem)
        log("%s: %s %s | eq %d opt %d debt %d | $%.2fB | status %s %s" % (fk, boards["current_period"], ls.get("form"), boards["n_equity_positions"],
                                                                             boards["n_option_rows"], boards["n_debt_rows"], (boards["total_value_usd"] or 0) / 1e9, status, flags))

    # 4. clone-alpha first (its SKILL weights the stance), then the cross-manager layers
    def optional(key):
        try:
            doc, mod, size = get_json(s3, key)
            return doc, mod, size, None
        except Exception as exc:  # noqa: BLE001
            return {}, None, None, "%s" % type(exc).__name__

    ca, ca_mod, _, ca_err = optional(CLONE_KEY)
    skill = {}
    for fk, row in clone_summary(ca.get("managers")).items():
        if fk in managers:
            managers[fk]["clone"] = row
            if isinstance(row.get("skill_score"), (int, float)):
                skill[fk] = row["skill_score"]
    clone_block = {"as_of": ca.get("as_of"), "status": ca.get("status"), "pct_complete": ca.get("pct_complete"), "filings_done": ca.get("filings_done"),
                   "filings_total": ca.get("filings_total"), "method": ca.get("method"), "warns": (ca.get("warns") or [])[:10], "error": ca_err,
                   "ranked": sorted([{"fund_key": fk, "display_name": managers[fk]["display_name"], **managers[fk]["clone"]} for fk in managers if managers[fk].get("clone")],
                                    key=lambda r: -(r.get("skill_score") if isinstance(r.get("skill_score"), (int, float)) else -1))}

    cohort_keys = [m["fk"] for m in members]
    conc_keys = [m["fk"] for m in members_conc]
    cohort_total = sum((managers[fk]["total_value_usd"] or 0) for fk in cohort_keys)
    hedge = [fk for fk in cohort_keys if managers[fk]["n_option_rows"] >= HEDGE_OPTION_ROWS]
    scopes = {}
    agg_cohort = None
    for name, mems in (("cohort", members), ("concentrated", members_conc)):
        agg = aggregate(mems, classer)
        if name == "cohort":
            agg_cohort = agg
        scopes[name] = {
            "members": [m["fk"] for m in mems], "n_identities": len(agg),
            "boards": {u: boards_from(agg, u, skill) for u in ("all", "stocks", "etfs")},
            "stance": {u: stance_from(agg, u, skill) for u in ("all", "stocks", "etfs")},
            "contenders": {u: contenders_from(agg, u, skill) for u in ("all", "stocks", "etfs")},
            "rotation": rotation(mems, classer),
        }
    # unclassified exchange-traded products and the largest unmapped identities, for review
    unclassified, unmapped = {}, {}
    class_sources = Counter()
    for p, a in (agg_cohort or {}).items():
        c = a["cls"]
        if c["kind"] == "fund" and a["holders"]:
            class_sources[c.get("class_source") or "none"] += 1
        if c["kind"] == "fund" and c["sub"] == "unclassified" and a["holders"]:
            u = unclassified.setdefault(c["ticker"], {"ticker": c["ticker"], "name": c["name"], "security_type": c["security_type"], "cusip": a["cusip"],
                                                      "fmp_asset_class": c.get("fmp_asset_class"), "value_usd": Decimal(0), "n_holders": 0})
            u["value_usd"] += a["value_usd"]
            u["n_holders"] = max(u["n_holders"], len(a["holders"]))
        elif c["kind"] == "unmapped" and a["holders"]:
            unmapped[p] = {"id": p[:12], "issuer": a["issuer"], "cusip": a["cusip"], "class": a["class"], "master": c["master"], "value_usd": f(a["value_usd"]), "n_holders": len(a["holders"])}
    unclassified_value = sum((u["value_usd"] for u in unclassified.values()), Decimal(0))
    review = {"unclassified_etfs": sorted(({**u, "value_usd": f(u["value_usd"])} for u in unclassified.values()), key=lambda r: -(r["value_usd"] or 0))[:150],
              "n_unclassified_etfs": len(unclassified), "unclassified_value_usd": f(unclassified_value),
              "unmapped_top": sorted(unmapped.values(), key=lambda r: -(r["value_usd"] or 0))[:60], "n_unmapped": len(unmapped),
              "etf_table_reviewed": etf_reviewed, "etf_table_size": len(etf_classes), "class_sources": dict(class_sources),
              "learned": {"key": LEARNED_KEY, "n_rows": len(learned), "last_modified": learned_mod.isoformat(timespec="seconds") if learned_mod else None, "error": learned_err, "last_run": learn_stats}}

    # 5. the period snapshot (written once per period, re-written when the cohort or its identity count changes)
    snapshot = {"key": None, "written": False, "reason": None}
    if required and agg_cohort is not None and cohort_keys:
        snap_key = "%s%s.json" % (PERIODS_PREFIX, required)
        snap = period_snapshot(required, cohort_keys, agg_cohort, now)
        try:
            prev, _, _ = get_json(s3, snap_key)
        except Exception:  # noqa: BLE001
            prev = None
        if prev is None or sorted(prev.get("cohort") or []) != snap["cohort"] or prev.get("n_identities") != snap["n_identities"]:
            try:
                n = put_json(s3, snap_key, snap, cache="max-age=3600")
                snapshot = {"key": snap_key, "written": True, "bytes": n, "reason": "new period" if prev is None else "cohort or identity count changed"}
            except Exception as exc:  # noqa: BLE001
                snapshot = {"key": snap_key, "written": False, "reason": "%s" % type(exc).__name__}
        else:
            snapshot = {"key": snap_key, "written": False, "reason": "unchanged"}

    book = {
        "required_period": required, "cohort": cohort_keys, "concentrated": conc_keys, "concentrated_max_positions": CONCENTRATED_MAX,
        "hedge_books": hedge, "hedge_books_equity_value_usd": sum((managers[fk]["total_value_usd"] or 0) for fk in hedge),
        "cohort_equity_value_usd": cohort_total, "cohort_counts": sum_counts(managers, cohort_keys),
        "managers": managers,
        "boards": {s: scopes[s]["boards"] for s in scopes}, "stance": {s: scopes[s]["stance"] for s in scopes},
        "contenders": {s: scopes[s]["contenders"] for s in scopes}, "rotation": {s: scopes[s]["rotation"] for s in scopes},
        "scopes": {s: {"members": scopes[s]["members"], "n_identities": scopes[s]["n_identities"]} for s in scopes},
        "security_master": {"key": MASTER_KEY, "generated_at": master.get("generated_at"), "last_modified": master_mod.isoformat(timespec="seconds") if master_mod else None,
                            "bytes": master_bytes, "error": master_err, "n_rows": len(rows), "coverage": coverage, "refresh": refresh, "recheck_days": RECHECK_DAYS},
        "review": review,
        "history": {"prefix": PERIODS_PREFIX, "periods": list_periods(s3), "snapshot": snapshot},
        "options_overlay": sorted([{"fund_key": fk, "display_name": m["display_name"], "n_option_rows": m["n_option_rows"], "put_value_usd": m["put_value_usd"],
                                    "call_value_usd": m["call_value_usd"], "equity_value_usd": m["total_value_usd"],
                                    "put_share": f(round(100 * dec(m["put_value_usd"]) / (dec(m["put_value_usd"]) + dec(m["call_value_usd"])), 1)) if (m["put_value_usd"] or m["call_value_usd"]) else None,
                                    "puts_over_equity": f(round(dec(m["put_value_usd"]) / dec(m["total_value_usd"]), 2)) if m["total_value_usd"] else None,
                                    "status": m["status"]} for fk, m in managers.items() if m["n_option_rows"]],
                                  key=lambda r: -((r["put_value_usd"] or 0) + (r["call_value_usd"] or 0))),
        "errors": errors,
        "glossary": {
            "ACCUMULATING": "more managers reported a larger or newly disclosed position than a smaller or absent one (at least 2:1)",
            "DISTRIBUTING": "more managers reported a smaller or absent position than a larger or new one (at least 2:1)",
            "CONTESTED": "managers disagree within 2:1", "QUIET": "no manager changed a reported quantity",
            "votes.net": "managers newly disclosing + reporting more units, minus managers absent + reporting fewer units",
            "votes.weighted_net": "the same votes weighted by each manager's clone-alpha SKILL / 100 (managers without a SKILL are not weighted)",
            "holder_delta": "cohort managers reporting the identity now minus last period",
            "kind": "stock / fund (exchange-traded product) / debt (principal amount) / other / unmapped, from the CUSIP master",
            "class_source": "table = the reviewed ETF class table; fmp = FMP's own assetClass read from the bank; none = unclassified",
            "BALANCED": "a class whose net manager-identity pairs are within +/-%d%% of all its pairs" % int(TILT_PCT),
        },
    }

    # the other 13F feeds - each optional, each reported honestly when missing
    pdv, _, _, pdv_err = optional(DIVERGENCE_KEY)
    divergence = {k: pdv.get(k) for k in ("engine", "version", "as_of", "state", "data_sufficiency", "signal_strength", "n_divergences", "n_bullish", "n_bearish",
                                          "n_high_conviction", "feeders", "methodology", "sources", "why_now", "run_seconds")}
    divergence.update({"top_bullish": (pdv.get("top_bullish") or [])[:10], "top_bearish": (pdv.get("top_bearish") or [])[:10], "error": pdv_err})
    det, _, _, det_err = optional(DETECTOR_KEY)
    detector_funds = {fk: {"name": (m or {}).get("name"), **{k: ((m or {}).get("latest_filing") or {}).get(k) for k in ("period_of_report", "filed_at", "accession", "form")}}
                      for fk, m in ((det.get("by_fund") or {}).items())}
    brief, _, _, brief_err = optional(BRIEF_KEY)
    commentary = brief.get("commentary") if isinstance(brief.get("commentary"), dict) else {}
    ai_brief = {k: brief.get(k) for k in ("generated_at", "model", "model_api_calls", "portfolio_action", "contract", "calls_eligible", "execution_eligible",
                                          "sizing_eligible", "forecast_qualified", "llm_attempt_failed", "version")}
    ai_brief.update({"headline": commentary.get("headline"), "narrative": commentary.get("narrative"), "posture": commentary.get("posture"), "mode": commentary.get("mode"),
                     "divergence_signals": commentary.get("divergence_signals"), "source_coverage": brief.get("source_coverage"), "error": brief_err})

    engines = []
    for e in ENGINES:
        exp = e.get("expected_h", 24)
        outs = [head(s3, k, now, exp) for k in e["outputs"]]
        ages = [o["age_h"] for o in outs if o["age_h"] is not None]
        engines.append({**e, "objects": outs, "newest_age_h": min(ages) if ages else None, "tier": tier(min(ages), exp) if ages else "missing"})
    state = [head(s3, k, now, None) for k in STATE_KEYS]   # the retired board's state: age shown, never judged
    hr, _, _, hr_err = optional("data/holdings-research.json")

    out = {
        "engine": ENGINE, "version": VERSION, "contract": CONTRACT, "generated_at": now.isoformat(timespec="seconds"),
        "source": {"desk_key": DESK_KEY, "desk_generated_at": desk.get("generated_at"), "desk_last_modified": desk_mod.isoformat(timespec="seconds"), "desk_bytes": desk_bytes,
                   "desk_contract": desk.get("contract"), "source_generated_at": desk.get("source_generated_at"),
                   "source_originals": desk.get("source_originals") if desk.get("source_originals") is not None else hr.get("source_originals"),
                   "holdings_research": {k: hr.get(k) for k in ("contract", "generated_at", "fund_count", "current_cohort_count", "refresh_status", "acquisition_mode", "source_request_count")} | {"error": hr_err},
                   "research": desk.get("research"), "legacy_snapshot": desk.get("legacy_snapshot"), "security_indexes": desk.get("security_indexes"),
                   "ticker_identity": desk.get("ticker_identity"), "methodology_url": desk.get("methodology_url"), "research_url": desk.get("research_url"),
                   "security_master": "OpenFIGI v3 /mapping by CUSIP / CINS / derived ISIN (keyed); a ticker is shown only for a resolved identifier",
                   "etf_classes_learned": {"key": LEARNED_KEY, "n": len(learned)},
                   "etf_classes": {"file": "13f-etf-classes.json (twin config/13f-etf-classes.json)", "reviewed": etf_reviewed, "n": len(etf_classes)}},
        "cadence": "daily (cadence floor) + hand-off after every native research publication; the native research publishes 15:40 UTC, this book 16:37 UTC; EDGAR detector 03:31 UTC",
        "note": ("Every figure is a reported quantity or a reported value from a retained 13F-HR information table, compared across two dated "
                 "disclosures. Newly disclosed is not newly bought; absent is not sold; a reported value change carries the price move. "
                 "Stances and contenders count managers; tickers come from the CUSIP master, never from a name. No trades, flows or units are inferred."),
        "limitations": desk.get("limitations"), "quality": desk.get("quality"), "report_period_cohorts": desk.get("report_period_cohorts"),
        "permissions": {k: desk.get(k) for k in ("call", "calls_eligible", "sizing_eligible", "execution_eligible", "additional_independent_votes")},
        "book": book,
        "season": season(required, now.date(), det, by_fund) | {"detector_error": det_err, "detector_funds": detector_funds},
        "clone_alpha": clone_block, "price_divergence": divergence, "ai_brief": ai_brief,
        "engines": engines, "state_objects": state,
        "run_seconds": round(time.time() - started, 1),
    }
    return out


def lambda_handler(event, context):
    chain_guard.begin(event, context)  # trigger cap: continue the chain this run was started by (sec-13f -> 13f-positions -> here), a root otherwise
    now = datetime.now(timezone.utc)
    s3 = _s3()
    try:
        remaining = context.get_remaining_time_in_millis() / 1000.0
    except Exception:  # noqa: BLE001
        remaining = 300.0
    out = build(s3, now, remaining_s=remaining)
    trig = event.get("trigger") if isinstance(event, dict) else None
    out["trigger"] = {"kind": trig or "scheduled", "source_generated_at": (event or {}).get("source_generated_at") if isinstance(event, dict) else None}
    n = put_json(s3, OUT_KEY, out)
    b = out["book"]
    cov = b["security_master"]["coverage"]
    print("13f-book: %d managers, cohort %d, concentrated %d, cohort equity $%.1fB, master %s/%s resolved (%.1f%% of value), %d bytes, %.1fs, errors %d" % (
        len(b["managers"]), len(b["cohort"]), len(b["concentrated"]), b["cohort_equity_value_usd"] / 1e9, cov.get("resolved"), cov.get("identities"),
        cov.get("resolved_value_pct") or 0.0, n, out["run_seconds"], len(b["errors"])))
    return {"statusCode": 200, "key": OUT_KEY, "bytes": n, "managers": len(b["managers"]), "cohort": len(b["cohort"]),
            "master_resolved": cov.get("resolved"), "master_identities": cov.get("identities"), "run_seconds": out["run_seconds"], "errors": len(b["errors"])}
