"""
Pro Pack v3 #8 - Refinitiv/MSCI Smart Beta Multi-Factor Composite
=====================================================================

The canonical methodology powering $300B+ in smart-beta ETFs (MSCI Diversified
Multiple-Factor, FTSE Russell Smart Beta, Refinitiv Equally Weighted Factor):

Four independent factors, each percentile-ranked (0-100) across universe,
then equally averaged into a single composite score:

1. VALUE
   - 0.5 * (1/PE) + 0.5 * (1/PB)
   - Invert so HIGH rank = CHEAP
   - Source: FMP /stable/key-metrics-ttm

2. QUALITY
   - 0.5 * ROIC + 0.5 * Gross Margin
   - Higher = better quality
   - Source: FMP /stable/key-metrics-ttm + /stable/ratios-ttm

3. MOMENTUM
   - Total return (price-only) over last 12 months MINUS last 1 month
   - "12-minus-1" momentum: avoids 1-month reversal effect (well-documented)
   - Source: FMP /stable/historical-price-eod/full (~252 days)

4. LOW VOLATILITY
   - Negative of 252-day annualized realized volatility of daily returns
   - High rank = low vol = stable
   - Source: same historical prices

Composite Smart Beta Score (0-100) = average of 4 percentile ranks.

Market Factor Regime: identifies which of the 4 factors has highest MEDIAN
percentile across universe -> leadership rotation signal.

Output:
- top_25 diversified leaders (high composite)
- bottom_25 laggards
- Top-10 leader per individual factor
- Factor regime classification

Universe: STATIC_TOP50_SPX (deterministic, FMP-quota friendly).
Schedule: daily 00:15 UTC (post-midnight, fresh FMP quota).

v1.1.0 (2026-10-26, bank fusion wave 2): the value / quality inputs (PE, PB, ROIC, gross margin) come from the factor
book (FMP TTM bulks at the bank close, labelled `facts_source`), the 252-session price series for momentum and
low-vol from the Polygon EOD bank (data/warm/polygon-full/eod, labelled `price_source`); live FMP stays the fallback
for a name the banks lack.  The STATIC_TOP50_SPX ranks keep their cross-engine meaning; a labelled `market` block
ranks the same four factors across every US common stock >= $2B from the book alone (momentum = the tape's
252-session minus 21-session return; low-vol from the book's 20-day realised vol, a labelled proxy for the 252-day
figure the core universe uses).  Factor formulas, ranks, thresholds and regime rules are unchanged.
"""

try:  # bank-first FMP (aws/shared/fmp_bankfirst): /stable answered from the warehouse banks when fresh, else live
    from fmp_bankfirst import install as _fmp_bankfirst_install
    _fmp_bankfirst_install()
except Exception:  # noqa: BLE001 - the shim never blocks the engine
    pass

import os
import json
import time
import math
import statistics
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone

import boto3
try:  # v1.1.0: the factor book (aws/shared/bank_enrich) - TTM facts + the whole-market factor ranks, projection reads only
    import bank_enrich as _book
except Exception:  # noqa: BLE001
    _book = None
try:  # the Polygon EOD bank layout (aws/shared/polygon_bankfirst) - one gzipped file of daily rows per US ticker
    import polygon_bankfirst as _peod
except Exception:  # noqa: BLE001
    _peod = None

# ---------- Constants ----------
VERSION = "1.1.0"
MARKET_MIN_MCAP_USD = 2_000_000_000
MARKET_CANDIDATES = int(os.environ.get("SMART_BETA_MARKET_CANDIDATES", "1500"))
BOOK_FIELDS = ["pe", "pb", "roic", "gross_margin", "ret_252d", "ret_21d", "volatility_20d", "mcap", "sector"]
EOD_ROOT = "data/warm/polygon-full/eod/"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/smart-beta.json"
FMP_BASE = "https://financialmodelingprep.com/stable"
FMP_KEY = os.environ.get("FMP_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

FMP_SLEEP_SEC = 0.4
HTTP_TIMEOUT = 25
PRICE_HISTORY_DAYS = 380  # ~252 trading days + buffer

# Same universe as StarMine + Predictability for cross-engine consistency
STATIC_TOP50_SPX = [
    {"symbol": "AAPL",  "sector": "Technology"},
    {"symbol": "MSFT",  "sector": "Technology"},
    {"symbol": "NVDA",  "sector": "Technology"},
    {"symbol": "GOOGL", "sector": "Communication Services"},
    {"symbol": "GOOG",  "sector": "Communication Services"},
    {"symbol": "AMZN",  "sector": "Consumer Cyclical"},
    {"symbol": "META",  "sector": "Communication Services"},
    {"symbol": "TSLA",  "sector": "Consumer Cyclical"},
    {"symbol": "BRK-B", "sector": "Financial Services"},
    {"symbol": "JPM",   "sector": "Financial Services"},
    {"symbol": "LLY",   "sector": "Healthcare"},
    {"symbol": "V",     "sector": "Financial Services"},
    {"symbol": "XOM",   "sector": "Energy"},
    {"symbol": "UNH",   "sector": "Healthcare"},
    {"symbol": "JNJ",   "sector": "Healthcare"},
    {"symbol": "MA",    "sector": "Financial Services"},
    {"symbol": "WMT",   "sector": "Consumer Defensive"},
    {"symbol": "PG",    "sector": "Consumer Defensive"},
    {"symbol": "AVGO",  "sector": "Technology"},
    {"symbol": "HD",    "sector": "Consumer Cyclical"},
    {"symbol": "ORCL",  "sector": "Technology"},
    {"symbol": "MRK",   "sector": "Healthcare"},
    {"symbol": "COST",  "sector": "Consumer Defensive"},
    {"symbol": "ABBV",  "sector": "Healthcare"},
    {"symbol": "BAC",   "sector": "Financial Services"},
    {"symbol": "CVX",   "sector": "Energy"},
    {"symbol": "ADBE",  "sector": "Technology"},
    {"symbol": "KO",    "sector": "Consumer Defensive"},
    {"symbol": "CRM",   "sector": "Technology"},
    {"symbol": "PEP",   "sector": "Consumer Defensive"},
    {"symbol": "AMD",   "sector": "Technology"},
    {"symbol": "ACN",   "sector": "Technology"},
    {"symbol": "TMO",   "sector": "Healthcare"},
    {"symbol": "MCD",   "sector": "Consumer Cyclical"},
    {"symbol": "CSCO",  "sector": "Technology"},
    {"symbol": "WFC",   "sector": "Financial Services"},
    {"symbol": "ABT",   "sector": "Healthcare"},
    {"symbol": "LIN",   "sector": "Basic Materials"},
    {"symbol": "DHR",   "sector": "Healthcare"},
    {"symbol": "DIS",   "sector": "Communication Services"},
    {"symbol": "TXN",   "sector": "Technology"},
    {"symbol": "NFLX",  "sector": "Communication Services"},
    {"symbol": "GE",    "sector": "Industrials"},
    {"symbol": "IBM",   "sector": "Technology"},
    {"symbol": "INTU",  "sector": "Technology"},
    {"symbol": "AMGN",  "sector": "Healthcare"},
    {"symbol": "VZ",    "sector": "Communication Services"},
    {"symbol": "PFE",   "sector": "Healthcare"},
    {"symbol": "QCOM",  "sector": "Technology"},
    {"symbol": "CMCSA", "sector": "Communication Services"},
]


# ---------- HTTP ----------
def http_json(url, retries=4):
    backoffs = [5, 15, 30, 60]
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
                return json.loads(r.read().decode("utf-8", errors="ignore"))
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < retries:
                time.sleep(backoffs[min(attempt, len(backoffs)-1)])
                continue
            return {"_error": f"HTTP {e.code}"}
        except Exception as e:
            if attempt < retries:
                time.sleep(2)
                continue
            return {"_error": str(e)[:100]}
    return {"_error": "exhausted"}


# ---------- FMP wrappers ----------
def fmp_key_metrics_ttm(symbol):
    url = f"{FMP_BASE}/key-metrics-ttm?symbol={symbol}&apikey={FMP_KEY}"
    d = http_json(url)
    if isinstance(d, list) and d:
        return d[0]
    return {}


def fmp_ratios_ttm(symbol):
    url = f"{FMP_BASE}/ratios-ttm?symbol={symbol}&apikey={FMP_KEY}"
    d = http_json(url)
    if isinstance(d, list) and d:
        return d[0]
    return {}


def fmp_historical_prices(symbol, days=PRICE_HISTORY_DAYS):
    from datetime import timedelta
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    url = (f"{FMP_BASE}/historical-price-eod/full?symbol={symbol}"
           f"&from={start}&to={end}&apikey={FMP_KEY}")
    d = http_json(url)
    if isinstance(d, list):
        return d
    if isinstance(d, dict):
        return d.get("historical", [])
    return []


ND_FACTOR_FIELDS = ("priceToEarningsRatioTTM", "priceToBookRatioTTM",
                    "returnOnInvestedCapitalTTM", "grossProfitMarginTTM")


# ---------- factor book + Polygon EOD bank ----------
def book_ok():
    try:
        return _book is not None and _book.available()
    except Exception:  # noqa: BLE001
        return False


def facts_from_row(row):
    """A factor-book matrix row in the two live-call shapes the factor functions read (key-metrics-ttm, ratios-ttm)."""
    if not row:
        return None
    km = {"returnOnInvestedCapitalTTM": row.get("roic")}
    ratios = {"priceToEarningsRatioTTM": row.get("pe"), "priceToBookRatioTTM": row.get("pb"), "grossProfitMarginTTM": row.get("gross_margin")}
    if all(v is None for v in (row.get("roic"), row.get("pe"), row.get("pb"), row.get("gross_margin"))):
        return None
    return km, ratios


def book_facts(symbol):
    if not book_ok():
        return None
    try:
        return facts_from_row(_book.lookup(symbol, BOOK_FIELDS))
    except Exception:  # noqa: BLE001
        return None


_EOD_S3 = None


def bank_prices(symbol, days=PRICE_HISTORY_DAYS):
    """Daily closes for the last `days` calendar days from the Polygon EOD bank (one file per ticker, rows
    [t_ms, o, h, l, c, v, vw, n]) as [{date, close}] oldest -> newest, with the last session's date; ([], None) when
    the bank has no file for the name.  Bank-only: no key, no live call."""
    global _EOD_S3
    import gzip
    from datetime import timedelta
    root = getattr(_peod, "ROOT", None) or EOD_ROOT
    bucket = getattr(_peod, "BUCKET", None) or S3_BUCKET
    safe = "".join(ch if (ch.isalnum() or ch in "._-^") else "_" for ch in str(symbol))[:80]
    try:
        if _EOD_S3 is None:
            _EOD_S3 = boto3.client("s3", region_name="us-east-1")
        raw = _EOD_S3.get_object(Bucket=bucket, Key=root + safe + ".json.gz")["Body"].read()
        doc = json.loads(gzip.decompress(raw))
    except Exception:  # noqa: BLE001
        return [], None
    rows = [r for r in (doc.get("rows") or []) if isinstance(r, list) and len(r) >= 5 and r[0] is not None and r[4] is not None] if isinstance(doc, dict) else []
    if not rows:
        return [], None
    start = datetime.now(timezone.utc).date() - timedelta(days=days)
    out = []
    for r in rows:
        d = datetime.fromtimestamp(r[0] / 1000.0, tz=timezone.utc).date()
        if d >= start:
            out.append({"date": d.isoformat(), "close": r[4]})
    out.sort(key=lambda x: x["date"])
    return out, (out[-1]["date"] if out else None)


# ---------- Factor computation ----------
def compute_value(km, ratios):
    """Value = 0.5 * (1/PE) + 0.5 * (1/PB). Higher = cheaper.
    PE/PB live in /stable/ratios-ttm (not /stable/key-metrics-ttm)."""
    pe = ratios.get("priceToEarningsRatioTTM")
    pb = ratios.get("priceToBookRatioTTM")
    try:
        pe = float(pe) if pe is not None else None
        pb = float(pb) if pb is not None else None
    except (ValueError, TypeError):
        return None, None, None
    if not pe or pe <= 0 or not pb or pb <= 0:
        return None, None, None
    v = 0.5 * (1.0 / pe) + 0.5 * (1.0 / pb)
    return round(v, 6), pe, pb


def compute_quality(km, ratios):
    """Quality = 0.5 * ROIC + 0.5 * GrossMargin. Higher = better.
    ROIC in /stable/key-metrics-ttm (returnOnInvestedCapitalTTM).
    GrossMargin in /stable/ratios-ttm (grossProfitMarginTTM)."""
    roic = km.get("returnOnInvestedCapitalTTM")
    gm = ratios.get("grossProfitMarginTTM")
    if roic is None or gm is None:
        return None, None, None
    try:
        roic = float(roic)
        gm = float(gm)
        q = 0.5 * roic + 0.5 * gm
        return round(q, 4), roic, gm
    except (ValueError, TypeError):
        return None, None, None


def compute_momentum(prices):
    """12m return - 1m return ("12-minus-1" momentum).
    prices: list of {date, close} sorted oldest->newest."""
    if not prices or len(prices) < 252:
        return None, None, None
    closes = [p.get("close") or p.get("adjClose") for p in prices
              if p.get("close") or p.get("adjClose")]
    if len(closes) < 252:
        return None, None, None
    px_now = closes[-1]
    px_1m = closes[-22] if len(closes) >= 22 else None
    px_12m = closes[-252] if len(closes) >= 252 else None
    if not px_now or not px_12m or not px_1m or px_12m <= 0 or px_1m <= 0:
        return None, None, None
    ret_12m = (px_now / px_12m) - 1.0
    ret_1m = (px_now / px_1m) - 1.0
    mom_12m1 = ret_12m - ret_1m
    return round(mom_12m1 * 100, 2), round(ret_12m * 100, 2), round(ret_1m * 100, 2)


def compute_low_vol(prices):
    """Negative of 252-day annualized realized vol. Higher rank = lower vol."""
    if not prices or len(prices) < 252:
        return None, None
    closes = [p.get("close") or p.get("adjClose") for p in prices
              if p.get("close") or p.get("adjClose")]
    if len(closes) < 252:
        return None, None
    # Take last 252 closes
    closes = closes[-252:]
    returns = [(closes[i] / closes[i-1]) - 1.0
               for i in range(1, len(closes)) if closes[i-1] > 0]
    if len(returns) < 100:
        return None, None
    try:
        vol_daily = statistics.stdev(returns)
        vol_annual = vol_daily * math.sqrt(252)
        # Negate so higher = better (lower vol)
        return round(-vol_annual, 4), round(vol_annual * 100, 2)
    except statistics.StatisticsError:
        return None, None


# ---------- Per-ticker pipeline ----------
def analyze_ticker(symbol, sector):
    facts = book_facts(symbol)
    if facts:
        km, ratios = facts
        facts_source = "factor-book (bank close)"
    else:
        km = fmp_key_metrics_ttm(symbol)
        time.sleep(FMP_SLEEP_SEC)
        ratios = fmp_ratios_ttm(symbol)
        time.sleep(FMP_SLEEP_SEC)
        facts_source = "fmp live" if (km or ratios) else ("factor book unavailable; live unavailable" if not book_ok() else "live unavailable")
    prices, last_session = bank_prices(symbol)
    if len(prices) >= 252:
        price_source = "polygon-eod bank (last session %s)" % last_session
    else:
        prices_raw = fmp_historical_prices(symbol)
        time.sleep(FMP_SLEEP_SEC)
        # Ensure prices are oldest->newest
        prices = sorted([p for p in prices_raw if p.get("date")],
                        key=lambda x: x["date"])
        price_source = "fmp live historical-price-eod" if prices else "unavailable"

    value_raw, pe, pb = compute_value(km, ratios)
    quality_raw, roic, gm = compute_quality(km, ratios)
    momentum_pct, ret_12m, ret_1m = compute_momentum(prices)
    lowvol_raw, vol_annual_pct = compute_low_vol(prices)

    return {
        "ticker": symbol,
        "sector": sector,
        "value": {"raw": value_raw, "pe_ttm": pe, "pb_ttm": pb},
        "quality": {"raw": quality_raw, "roic_ttm": roic, "gross_margin_ttm": gm},
        "momentum": {"twelve_minus_one_pct": momentum_pct,
                      "ret_12m_pct": ret_12m, "ret_1m_pct": ret_1m},
        "low_vol": {"raw": lowvol_raw, "annualized_vol_pct": vol_annual_pct},
        "facts_source": facts_source,
        "price_source": price_source,
    }


# ---------- whole-market pass (factor book, bank-only) ----------
def market_pass():
    """The four factors ranked across every US common stock >= $2B from the factor book alone (no live call):
    value / quality exactly as the core; momentum = ret_252d - ret_21d from the tape (percent); low-vol from the
    book's 20-day realised vol (annualised %), a labelled proxy for the core's 252-day figure."""
    if not book_ok():
        return {"available": False, "reason": "factor book unavailable"}
    try:
        rows = _book.screen(lambda r: (r.get("mcap") or 0) >= MARKET_MIN_MCAP_USD and not r.get("is_adr"), fields=BOOK_FIELDS)
    except Exception as e:  # noqa: BLE001
        return {"available": False, "reason": "factor book screen failed: %s" % str(e)[:80]}
    rows.sort(key=lambda r: -(r.get("mcap") or 0))
    rows = rows[:MARKET_CANDIDATES]
    per = []
    for r in rows:
        facts = facts_from_row(r)
        km, ratios = facts if facts else ({}, {})
        value_raw, pe, pb = compute_value(km, ratios)
        quality_raw, roic, gm = compute_quality(km, ratios)
        r252, r21, vol = r.get("ret_252d"), r.get("ret_21d"), r.get("volatility_20d")
        mom = round(float(r252) - float(r21), 2) if isinstance(r252, (int, float)) and isinstance(r21, (int, float)) else None
        lv_raw = round(-float(vol) / 100.0, 4) if isinstance(vol, (int, float)) and vol > 0 else None
        per.append({"ticker": r["symbol"], "sector": r.get("sector"), "market_cap_usd": r.get("mcap"),
                    "value": {"raw": value_raw, "pe_ttm": pe, "pb_ttm": pb},
                    "quality": {"raw": quality_raw, "roic_ttm": roic, "gross_margin_ttm": gm},
                    "momentum": {"twelve_minus_one_pct": mom, "ret_12m_pct": r252, "ret_1m_pct": r21},
                    "low_vol": {"raw": lv_raw, "annualized_vol_pct": vol}})
    val_raws = [x["value"]["raw"] for x in per]
    qual_raws = [x["quality"]["raw"] for x in per]
    mom_raws = [x["momentum"]["twelve_minus_one_pct"] for x in per]
    lv_raws = [x["low_vol"]["raw"] for x in per]
    for x in per:
        x["value"]["pct"] = percentile_rank(x["value"]["raw"], val_raws)
        x["quality"]["pct"] = percentile_rank(x["quality"]["raw"], qual_raws)
        x["momentum"]["pct"] = percentile_rank(x["momentum"]["twelve_minus_one_pct"], mom_raws)
        x["low_vol"]["pct"] = percentile_rank(x["low_vol"]["raw"], lv_raws)
        pcts = [p for p in (x["value"]["pct"], x["quality"]["pct"], x["momentum"]["pct"], x["low_vol"]["pct"]) if p is not None]
        x["composite"] = round(sum(pcts) / len(pcts), 1) if pcts else None
        x["n_factors_valid"] = len(pcts)
    medians = {}
    for f in ("value", "quality", "momentum", "low_vol"):
        pcts = [x[f]["pct"] for x in per if x[f]["pct"] is not None]
        medians[f] = round(statistics.median(pcts), 1) if pcts else None
    valid = [x for x in per if x.get("composite") is not None and x["n_factors_valid"] >= 3]
    by_comp = sorted(valid, key=lambda x: -x["composite"])
    diversified = sorted([x for x in valid if x["n_factors_valid"] == 4 and all(x[f]["pct"] >= 60 for f in ("value", "quality", "momentum", "low_vol"))],
                         key=lambda x: -x["composite"])[:25]

    def slim(x):
        return {"ticker": x["ticker"], "sector": x.get("sector"), "market_cap_usd": x.get("market_cap_usd"), "composite": x.get("composite"),
                "n_factors_valid": x.get("n_factors_valid"), "value_pct": x["value"].get("pct"), "quality_pct": x["quality"].get("pct"),
                "momentum_pct": x["momentum"].get("pct"), "low_vol_pct": x["low_vol"].get("pct"), "pe_ttm": x["value"].get("pe_ttm"),
                "pb_ttm": x["value"].get("pb_ttm"), "roic_ttm": x["quality"].get("roic_ttm"), "gross_margin_ttm": x["quality"].get("gross_margin_ttm"),
                "twelve_minus_one_pct": x["momentum"].get("twelve_minus_one_pct"), "vol_20d_annualized_pct": x["low_vol"].get("annualized_vol_pct")}
    return {
        "available": True,
        "universe_basis": "factor book: US common stocks >= $2B (cap %d, bank-only reads; not ADRs)" % MARKET_CANDIDATES,
        "book_as_of": _book.as_of(),
        "factor_notes": {"value": "0.5/PE + 0.5/PB from the TTM ratios bulk (as the core)",
                         "quality": "0.5*ROIC + 0.5*gross margin from the TTM bulks (as the core)",
                         "momentum": "tape ret_252d - ret_21d (percent; the core uses 252/22 closes)",
                         "low_vol": "PROXY: the book's 20-day realised vol, annualised % (the core uses 252-day realised vol)"},
        "n_candidates": len(per), "n_valid": len(valid), "factor_medians": medians,
        "top_25": [slim(x) for x in by_comp[:25]],
        "bottom_25": [slim(x) for x in list(reversed(by_comp[-25:]))],
        "diversified_leaders": [slim(x) for x in diversified],
        "factor_leaders": {f: [slim(x) for x in sorted([x for x in per if x[f]["pct"] is not None], key=lambda x: -x[f]["pct"])[:10]]
                           for f in ("value", "quality", "momentum", "low_vol")},
    }


def percentile_rank(value, all_values):
    """Percentile rank of value within all_values (0-100). Higher = better."""
    if value is None or not all_values:
        return None
    valid = [v for v in all_values if v is not None]
    if len(valid) < 5:
        return None
    n_below = sum(1 for v in valid if v < value)
    n_equal = sum(1 for v in valid if v == value)
    # Average rank method (handles ties)
    rank = (n_below + 0.5 * n_equal) / len(valid)
    return round(rank * 100, 1)


def telegram_notify(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": TELEGRAM_CHAT_ID,
            "text": msg,
            "parse_mode": "Markdown",
        }).encode("utf-8")
        urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=10)
    except Exception:
        pass


def lambda_handler(event, context):
    started = datetime.now(timezone.utc)

    if not FMP_KEY:
        return {"statusCode": 500,
                "body": json.dumps({"ok": False, "error": "FMP_KEY not set"})}

    # 1. Per-ticker raw factor values
    per_ticker = []
    for t in STATIC_TOP50_SPX:
        try:
            row = analyze_ticker(t["symbol"], t["sector"])
            per_ticker.append(row)
        except Exception as e:
            per_ticker.append({
                "ticker": t["symbol"], "sector": t["sector"],
                "value": {"raw": None}, "quality": {"raw": None},
                "momentum": {"twelve_minus_one_pct": None},
                "low_vol": {"raw": None}, "_error": str(e)[:120]
            })

    # 2. Cross-sectional percentile ranks
    val_raws = [r["value"]["raw"] for r in per_ticker]
    qual_raws = [r["quality"]["raw"] for r in per_ticker]
    mom_raws = [r["momentum"]["twelve_minus_one_pct"] for r in per_ticker]
    lv_raws = [r["low_vol"]["raw"] for r in per_ticker]

    for r in per_ticker:
        r["value"]["pct"] = percentile_rank(r["value"]["raw"], val_raws)
        r["quality"]["pct"] = percentile_rank(r["quality"]["raw"], qual_raws)
        r["momentum"]["pct"] = percentile_rank(
            r["momentum"]["twelve_minus_one_pct"], mom_raws)
        r["low_vol"]["pct"] = percentile_rank(r["low_vol"]["raw"], lv_raws)
        # Composite = equal-weight avg of 4 pct ranks (skip None)
        pcts = [p for p in (r["value"]["pct"], r["quality"]["pct"],
                            r["momentum"]["pct"], r["low_vol"]["pct"])
                if p is not None]
        r["composite"] = round(sum(pcts) / len(pcts), 1) if pcts else None
        r["n_factors_valid"] = len(pcts)

    facts_counts, price_counts = {}, {}
    for r in per_ticker:
        k = str(r.get("facts_source") or "none").split(" (")[0]
        facts_counts[k] = facts_counts.get(k, 0) + 1
        k2 = str(r.get("price_source") or "none").split(" (")[0]
        price_counts[k2] = price_counts.get(k2, 0) + 1
    try:
        market = market_pass()
    except Exception as e:  # noqa: BLE001
        market = {"available": False, "reason": "market pass failed: %s" % str(e)[:100]}

    # 3. Market factor regime: which factor has highest median pct
    factor_medians = {}
    for f in ("value", "quality", "momentum", "low_vol"):
        pcts = [r[f]["pct"] for r in per_ticker if r[f]["pct"] is not None]
        factor_medians[f] = round(statistics.median(pcts), 1) if pcts else None
    leading_factor = max(
        (f for f in factor_medians if factor_medians[f] is not None),
        key=lambda f: factor_medians[f] or 0, default=None)
    lagging_factor = min(
        (f for f in factor_medians if factor_medians[f] is not None),
        key=lambda f: factor_medians[f] or 100, default=None)

    # 4. Rankings
    valid = [r for r in per_ticker
             if r.get("composite") is not None and r["n_factors_valid"] >= 3]
    by_composite = sorted(valid, key=lambda x: -x["composite"])
    top_25 = by_composite[:25]
    bottom_25 = list(reversed(by_composite[-25:]))

    # 5. Single-factor leaders (top 10 per factor)
    def top_n_by_factor(factor, n=10):
        valid_f = [r for r in per_ticker if r[factor]["pct"] is not None]
        return sorted(valid_f, key=lambda x: -x[factor]["pct"])[:n]

    factor_leaders = {
        "value": top_n_by_factor("value"),
        "quality": top_n_by_factor("quality"),
        "momentum": top_n_by_factor("momentum"),
        "low_vol": top_n_by_factor("low_vol"),
    }

    # 6. Diversified factor leaders (high in ALL 4 factors)
    diversified = sorted(
        [r for r in valid if r["n_factors_valid"] == 4 and
         all(r[f]["pct"] is not None and r[f]["pct"] >= 60
             for f in ("value", "quality", "momentum", "low_vol"))],
        key=lambda x: -x["composite"])[:10]

    # 7. Sector breakdown - average composite by sector
    sector_avg = {}
    for r in valid:
        sec = r.get("sector", "Unknown")
        s = sector_avg.setdefault(sec, {"n": 0, "sum": 0.0})
        s["n"] += 1
        s["sum"] += r["composite"]
    sector_breakdown = {
        sec: {"n": d["n"], "avg_composite": round(d["sum"] / d["n"], 1)}
        for sec, d in sector_avg.items()}

    # 8. Format helpers for output
    def fmt_ticker_summary(r, include_raw=False):
        out = {
            "ticker": r["ticker"], "sector": r.get("sector"),
            "composite": r.get("composite"),
            "n_factors_valid": r.get("n_factors_valid"),
            "value_pct": r["value"].get("pct"),
            "quality_pct": r["quality"].get("pct"),
            "momentum_pct": r["momentum"].get("pct"),
            "low_vol_pct": r["low_vol"].get("pct"),
            "facts_source": r.get("facts_source"),
            "price_source": r.get("price_source"),
        }
        if include_raw:
            out.update({
                "pe_ttm": r["value"].get("pe_ttm"),
                "pb_ttm": r["value"].get("pb_ttm"),
                "roic_ttm": r["quality"].get("roic_ttm"),
                "gross_margin_ttm": r["quality"].get("gross_margin_ttm"),
                "ret_12m_pct": r["momentum"].get("ret_12m_pct"),
                "ret_1m_pct": r["momentum"].get("ret_1m_pct"),
                "twelve_minus_one_pct": r["momentum"].get("twelve_minus_one_pct"),
                "annualized_vol_pct": r["low_vol"].get("annualized_vol_pct"),
            })
        return out

    # 9. Regime label
    factor_spread = max(factor_medians.values()) - min(
        v for v in factor_medians.values() if v is not None) \
        if all(v is not None for v in factor_medians.values()) else 0
    if factor_spread >= 25:
        regime_label = f"STRONG_{leading_factor.upper()}_LEADERSHIP"
    elif factor_spread >= 15:
        regime_label = f"MODERATE_{leading_factor.upper()}_TILT"
    else:
        regime_label = "BALANCED_MULTI_FACTOR"

    out = {
        "ok": True,
        "version": VERSION,
        "generated_at": started.isoformat(),
        "n_universe": len(STATIC_TOP50_SPX),
        "universe_basis": "STATIC_TOP50_SPX; TTM facts %s; prices %s" % (
            "from the factor book (bank close) where held, live FMP otherwise" if book_ok() else "live FMP (factor book unavailable)",
            "from the Polygon EOD bank where held, live FMP otherwise"),
        "book_as_of": (_book.as_of() if book_ok() else None),
        "facts_source_counts": facts_counts,
        "price_source_counts": price_counts,
        "market": market,
        "n_analyzed": len(per_ticker),
        "n_valid": len(valid),
        "n_diversified_leaders": len(diversified),
        "factor_regime": regime_label,
        "leading_factor": leading_factor,
        "lagging_factor": lagging_factor,
        "factor_medians": factor_medians,
        "factor_spread_points": round(factor_spread, 1),
        "sector_breakdown": sector_breakdown,
        "top_25_diversified": [fmt_ticker_summary(r, True) for r in top_25],
        "bottom_25_laggards": [fmt_ticker_summary(r, True) for r in bottom_25],
        "diversified_factor_leaders": [fmt_ticker_summary(r, True)
                                         for r in diversified],
        "factor_leaders": {
            f: [fmt_ticker_summary(r, True) for r in factor_leaders[f]]
            for f in factor_leaders
        },
        "all_tickers": [fmt_ticker_summary(r) for r in per_ticker],
        "methodology": {
            "factors": {
                "value": ("0.5 * (1/PE_ttm) + 0.5 * (1/PB_ttm), inverted "
                          "(high rank = cheap)"),
                "quality": ("0.5 * ROIC_ttm + 0.5 * Gross_Margin_ttm "
                             "(high rank = high quality)"),
                "momentum": ("12-month return MINUS 1-month return "
                              "('12-minus-1' momentum, avoids 1m reversal)"),
                "low_vol": ("Negated 252-day annualized realized vol "
                             "(high rank = low vol)"),
            },
            "composite": ("Equal-weight average of 4 percentile ranks "
                           "(0-100 scale)"),
            "diversified_leader_threshold": ("All 4 factor percentiles >= 60 "
                                              "AND composite top-decile"),
            "regime_classification": {
                "STRONG_*_LEADERSHIP": "Factor spread >= 25 pp - clear rotation",
                "MODERATE_*_TILT":      "Factor spread 15-25 pp",
                "BALANCED_MULTI_FACTOR": "Factor spread < 15 pp - no single tilt",
            },
            "universe": "STATIC_TOP50_SPX (shared with #4 #7)",
        },
        "edge_basis": ("MSCI Diversified Multiple-Factor backtests show "
                       "equal-weight 4-factor composites generated 1.8-2.5% "
                       "annual alpha vs cap-weighted S&P 500 over 1990-2020 "
                       "with lower drawdowns. Refinitiv's equally-weighted "
                       "factor approach is the methodology powering iShares "
                       "MSCI USA Multifactor (LRGF, ~$2B AUM) and similar."),
        "sources": {
            "factor_book": "data/warm/factor-book (PE, PB, ROIC, gross margin at the bank close; the market pass)",
            "polygon_eod_bank": "data/warm/polygon-full/eod/<ticker>.json.gz (252-session closes)",
            "key_metrics": "FMP /stable/key-metrics-ttm - fallback",
            "ratios": "FMP /stable/ratios-ttm - fallback",
            "prices": "FMP /stable/historical-price-eod/full - fallback",
            "universe": "STATIC_TOP50_SPX",
        },
    }

    # 10. Persist
    try:
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=S3_BUCKET, Key=S3_KEY,
            Body=json.dumps(out, default=str).encode("utf-8"),
            ContentType="application/json",
            CacheControl="public, max-age=300",
        )
    except Exception as e:
        return {"statusCode": 500,
                "body": json.dumps({"ok": False,
                                    "error": f"s3 put failed: {str(e)[:200]}"})}

    # 11. Alert on strong factor leadership
    if "STRONG_" in regime_label:
        top_3 = top_25[:3]
        names = ", ".join(f"{t['ticker']}({t['composite']:.0f})" for t in top_3)
        telegram_notify(
            f"📊 *Smart Beta {regime_label.replace('_', ' ')}*\n"
            f"Leading factor: *{leading_factor}* (median pct: "
            f"{factor_medians[leading_factor]:.0f}, spread: "
            f"{factor_spread:.0f}pp)\n"
            f"Top diversified: {names}\n"
            f"justhodl.ai/smart-beta.html"
        )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "version": VERSION,
            "factor_regime": regime_label,
            "leading_factor": leading_factor,
            "factor_medians": factor_medians,
            "n_valid": len(valid),
            "n_diversified": len(diversified),
            "top_3": [{"t": r["ticker"], "score": r["composite"]}
                      for r in top_25[:3]],
        }),
    }


if __name__ == "__main__":
    r = lambda_handler({}, None)
    print(json.dumps(r, indent=2))
