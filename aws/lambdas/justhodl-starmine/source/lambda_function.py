"""
Pro Pack v3 #4 - StarMine Analyst Skill Ranking (Refinitiv gap-closer)
=======================================================================

Refinitiv StarMine flagship: ranks stocks by aggregate analyst conviction +
predicted surprise. Hedge funds pay $24k/yr for this. We replicate the
analytic intent using FMP /stable/ data:

Three alpha factors per ticker (computed for top-150 S&P by market cap):

1. Rating Revision Momentum (RRM, 50% weight)
   - count(upgrades in last 90d) - count(downgrades in last 90d)
   - Firm-tier weighted: Tier A (Goldman, MS, JPM, BofA, Citi) = 1.5x,
     Tier B (Wells, Barclays, Deutsche, UBS, RBC) = 1.0x, others = 0.6x

2. Price Target Drift (PTD, 25% weight)
   - PT_consensus_current - PT_consensus_baseline (90d implicit)
   - Normalized as pct of current price
   - Captures sell-side conviction trajectory

3. Earnings Surprise Persistence (ESP, 25% weight)
   - % of last 8 quarters where actual EPS > estimate
   - Magnitude: median (actual - estimate) / |estimate|

Composite StarMine Score (0-100): z-score blend of three factors,
percentile-ranked across universe.

Output:
- top_25_starmine_score: highest conviction stocks
- bottom_25_starmine_score: lowest conviction stocks  
- universe_regime: BULLISH_REVISIONS / NEUTRAL / BEARISH_REVISIONS
- per-ticker breakdown of all 3 components

Schedule: daily 23:30 UTC (after FMP daily quota reset window).

v1.1.0 (2026-10-26, bank fusion wave 2): every per-ticker input is read from the warehouse first - grades from the FMP
walker's per-symbol `grades` file, the price-target consensus from its `price-target-consensus` file (then the factor
book's price-target summary), surprises from its `earnings` file (then the book's surprises block) - and the live
/stable call is only the fallback (`basis` per row, `basis_counts` on the document; the 0.4 s pacing applies to live
calls only).  Prices / market caps come from the factor book's bank close (`price_source`).  The universe is the static
S&P top-50 plus the book's analyst-covered US common stocks (>= 5 targets last quarter, >= $5B; `universe_basis`,
`n_book_candidates`).  Top / bottom rows carry `bank_context`.  Factor weights, z-scoring and regime bands unchanged.
"""

try:  # bank-first FMP (aws/shared/fmp_bankfirst): /stable answered from the warehouse banks when fresh, else live
    from fmp_bankfirst import install as _fmp_bankfirst_install
    _fmp_bankfirst_install()
except Exception:  # noqa: BLE001 - the shim never blocks the engine
    pass

import os
import sys
import json
import time
import statistics
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone, timedelta

import boto3

try:  # v1.1.0 bank fusion: the factor book + the FMP walker bank (bank-only, no key)
    import bank_enrich as _book
except Exception:  # noqa: BLE001
    _book = None
try:
    import fmp_bank as _bank
except Exception:  # noqa: BLE001
    _bank = None

# ---------- Constants ----------
VERSION = "1.1.0"
BOOK_CANDIDATES = int(os.environ.get("STARMINE_BOOK_CANDIDATES", "150"))
BOOK_MIN_MCAP = float(os.environ.get("STARMINE_BOOK_MIN_MCAP", "5000000000"))
BOOK_MIN_PT_N = int(os.environ.get("STARMINE_BOOK_MIN_PT_N", "5"))
BANK_CONTEXT_FIELDS = ("consensus_score", "pt_upside_pct", "pt_n_1q", "beat_rate_4q", "altman_z", "short_pct_float", "composite_street", "ret_63d")
BASIS = {"grades": {}, "pt": {}, "esp": {}, "price": {}}
UNIVERSE_META = {}
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/starmine.json"
FMP_BASE = "https://financialmodelingprep.com/stable"
FMP_KEY = os.environ.get("FMP_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

UNIVERSE_TOP_N = 50   # top-50 SP by mcap; single batched FMP quote call fits URL
FMP_SLEEP_SEC = 0.4   # ~300 req/min, well under FMP 750/min Starter cap
HTTP_TIMEOUT = 20

# Firm tier classification (institutional pedigree -> weight multiplier)
TIER_A_FIRMS = {  # global bulge bracket - track records well documented
    "Goldman Sachs", "Morgan Stanley", "JP Morgan", "JPMorgan",
    "Bank of America", "BofA Securities", "Citi", "Citigroup",
}
TIER_B_FIRMS = {  # large but non-bulge
    "Wells Fargo", "Barclays", "Deutsche Bank", "UBS", "RBC Capital",
    "RBC", "Credit Suisse", "BMO Capital", "HSBC", "BNP Paribas",
    "Jefferies", "Wedbush", "Stifel", "Evercore ISI", "Oppenheimer",
}
TIER_A_WT = 1.5
TIER_B_WT = 1.0
TIER_C_WT = 0.6
RRM_LOOKBACK_DAYS = 90

# Universe regime bands (median composite z-score)
REGIME_BANDS = [
    (1.0,  "BULLISH_REVISIONS"),
    (-1.0, "NEUTRAL_REVISIONS"),
    (-99,  "BEARISH_REVISIONS"),
]

# ---------- HTTP helper ----------
def http_json(url, retries=2):
    last_err = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
                return json.loads(r.read().decode("utf-8", errors="ignore"))
        except urllib.error.HTTPError as e:
            last_err = f"HTTP {e.code}"
            if e.code == 429 and attempt < retries:
                time.sleep(5 * (attempt + 1))
                continue
            return {"_error": last_err, "_code": e.code}
        except Exception as e:
            last_err = str(e)[:100]
            if attempt < retries:
                time.sleep(2)
                continue
    return {"_error": last_err}


# ---------- Universe acquisition (3-tier resilient fallback) ----------
# Static top-50 S&P 500 by market cap as of 2025-2026 (last-resort fallback)
STATIC_TOP50_SPX = [
    {"symbol": "AAPL", "sector": "Technology"},
    {"symbol": "MSFT", "sector": "Technology"},
    {"symbol": "NVDA", "sector": "Technology"},
    {"symbol": "GOOGL", "sector": "Communication Services"},
    {"symbol": "GOOG", "sector": "Communication Services"},
    {"symbol": "AMZN", "sector": "Consumer Cyclical"},
    {"symbol": "META", "sector": "Communication Services"},
    {"symbol": "TSLA", "sector": "Consumer Cyclical"},
    {"symbol": "BRK-B", "sector": "Financial Services"},
    {"symbol": "JPM", "sector": "Financial Services"},
    {"symbol": "LLY", "sector": "Healthcare"},
    {"symbol": "V", "sector": "Financial Services"},
    {"symbol": "XOM", "sector": "Energy"},
    {"symbol": "UNH", "sector": "Healthcare"},
    {"symbol": "JNJ", "sector": "Healthcare"},
    {"symbol": "MA", "sector": "Financial Services"},
    {"symbol": "WMT", "sector": "Consumer Defensive"},
    {"symbol": "PG", "sector": "Consumer Defensive"},
    {"symbol": "AVGO", "sector": "Technology"},
    {"symbol": "HD", "sector": "Consumer Cyclical"},
    {"symbol": "ORCL", "sector": "Technology"},
    {"symbol": "MRK", "sector": "Healthcare"},
    {"symbol": "COST", "sector": "Consumer Defensive"},
    {"symbol": "ABBV", "sector": "Healthcare"},
    {"symbol": "BAC", "sector": "Financial Services"},
    {"symbol": "CVX", "sector": "Energy"},
    {"symbol": "ADBE", "sector": "Technology"},
    {"symbol": "KO", "sector": "Consumer Defensive"},
    {"symbol": "CRM", "sector": "Technology"},
    {"symbol": "PEP", "sector": "Consumer Defensive"},
    {"symbol": "AMD", "sector": "Technology"},
    {"symbol": "ACN", "sector": "Technology"},
    {"symbol": "TMO", "sector": "Healthcare"},
    {"symbol": "MCD", "sector": "Consumer Cyclical"},
    {"symbol": "CSCO", "sector": "Technology"},
    {"symbol": "WFC", "sector": "Financial Services"},
    {"symbol": "ABT", "sector": "Healthcare"},
    {"symbol": "LIN", "sector": "Basic Materials"},
    {"symbol": "DHR", "sector": "Healthcare"},
    {"symbol": "DIS", "sector": "Communication Services"},
    {"symbol": "TXN", "sector": "Technology"},
    {"symbol": "NFLX", "sector": "Communication Services"},
    {"symbol": "GE", "sector": "Industrials"},
    {"symbol": "IBM", "sector": "Technology"},
    {"symbol": "INTU", "sector": "Technology"},
    {"symbol": "AMGN", "sector": "Healthcare"},
    {"symbol": "VZ", "sector": "Communication Services"},
    {"symbol": "PFE", "sector": "Healthcare"},
    {"symbol": "QCOM", "sector": "Technology"},
    {"symbol": "CMCSA", "sector": "Communication Services"},
]


def fmp_sp500_with_retry(max_retries=5):
    """Tier 1: FMP /stable/sp500-constituent with exponential backoff on 429."""
    url = f"{FMP_BASE}/sp500-constituent?apikey={FMP_KEY}"
    backoffs = [5, 15, 30, 60, 90]
    for attempt in range(max_retries):
        d = http_json(url)
        if isinstance(d, list) and d:
            return d, "fmp_sp500_constituent"
        # Check if it's a 429 specifically
        if isinstance(d, dict) and d.get("_code") == 429 and attempt < max_retries - 1:
            time.sleep(backoffs[attempt])
            continue
        # Any other error or final retry exhausted
        break
    return None, None


def s3_screener_fallback():
    """Tier 2: read SP500 universe from existing justhodl-stock-screener S3 cache."""
    try:
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=S3_BUCKET, Key="screener/data.json")
        data = json.loads(obj["Body"].read().decode("utf-8"))
        # Screener data structure may vary - try common keys
        tickers = (data.get("tickers") or data.get("stocks") or
                   data.get("data") or data.get("results") or [])
        if not isinstance(tickers, list) or not tickers:
            return None, None
        # Normalize to {symbol, sector} shape
        normalized = []
        for t in tickers:
            if isinstance(t, dict):
                sym = (t.get("symbol") or t.get("ticker") or
                       t.get("Symbol") or "")
                sec = (t.get("sector") or t.get("Sector") or "")
                if sym:
                    normalized.append({"symbol": sym, "sector": sec})
            elif isinstance(t, str):
                normalized.append({"symbol": t, "sector": ""})
        return (normalized[:500] if normalized else None,
                "s3_screener_fallback")
    except Exception:
        return None, None


def acquire_sp500_universe():
    """3-tier resilient fetch: FMP -> S3 screener cache -> static top-50."""
    # Tier 1: FMP with retry
    sp, source = fmp_sp500_with_retry()
    if sp and len(sp) >= 50:
        return sp, source
    # Tier 2: S3 screener cache
    sp, source = s3_screener_fallback()
    if sp and len(sp) >= 30:
        return sp, source
    # Tier 3: static hardcoded top-50
    return list(STATIC_TOP50_SPX), "static_top50_hardcoded"


# ---------- FMP wrappers ----------


def fmp_quote_batch(symbols):
    """Single batched quote call (up to 50 symbols fits comfortably in URL)."""
    if not symbols:
        return []
    url = f"{FMP_BASE}/quote?symbol={','.join(symbols)}&apikey={FMP_KEY}"
    d = http_json(url, retries=3)
    if isinstance(d, list):
        return d
    return []


def book_ok():
    return _book is not None and _book.available()


def _count(kind, basis):
    BASIS[kind][basis] = BASIS[kind].get(basis, 0) + 1


def book_candidates(exclude):
    """Analyst-covered US common stocks from the factor book (>= BOOK_MIN_PT_N price targets last quarter,
    >= BOOK_MIN_MCAP, not ADRs; ETF/OTC/fund/preferred excluded by the SDK), largest first, minus `exclude`."""
    if not book_ok():
        return []
    try:
        rows = _book.prescreen_rows(lambda r: (r.get("pt_n_1q") or 0) >= BOOK_MIN_PT_N and (r.get("mcap") or 0) >= BOOK_MIN_MCAP and not r.get("is_adr"),
                               order_by="mcap", descending=True, limit=BOOK_CANDIDATES + len(exclude), fields=["pt_n_1q", "mcap", "sector"])
    except Exception:  # noqa: BLE001
        return []
    out = []
    for r in rows:
        sym = r.get("symbol")
        if not sym or sym in exclude:
            continue
        out.append({"symbol": sym, "sector": r.get("sector") or "", "from_book": True})
        if len(out) >= BOOK_CANDIDATES:
            break
    return out


def acquire_universe_with_prices(target_n=UNIVERSE_TOP_N):
    """Universe + prices, bank first.

    STATIC_TOP50_SPX stays the deterministic core (mcap-ranked S&P names); the factor book adds its analyst-covered
    US common stocks (`from_book`).  Prices / market caps come from the book's bank close (`price_source`
    factor-book close); only the names the book lacks go to ONE batched live quote call per 50.  If every source
    fails an entry keeps price=None (PTD lens null; RRM + ESP still work).
    """
    core = list(STATIC_TOP50_SPX)[:target_n]
    book = book_candidates({u["symbol"] for u in core})
    universe = core + book
    price_map, mcap_map, src_map = {}, {}, {}
    if book_ok():
        for u in universe:
            try:
                row = _book.lookup(u["symbol"], ["price", "mcap"])
            except Exception:  # noqa: BLE001
                row = None
            if row and row.get("price"):
                price_map[u["symbol"]], mcap_map[u["symbol"]], src_map[u["symbol"]] = row["price"], row.get("mcap"), "factor-book close"
    missing = [u["symbol"] for u in universe if u["symbol"] not in price_map]
    for i in range(0, len(missing), 50):
        for q in fmp_quote_batch(missing[i:i + 50]):
            sym = q.get("symbol")
            if sym and q.get("price"):
                price_map[sym], mcap_map[sym], src_map[sym] = q.get("price"), q.get("marketCap"), "fmp live"
    out = []
    for u in universe:
        sym = u["symbol"]
        out.append({
            "symbol": sym,
            "sector": u.get("sector", ""),
            "price": price_map.get(sym),
            "price_source": src_map.get(sym),
            "market_cap_usd": mcap_map.get(sym),
            "from_book": bool(u.get("from_book")),
        })
        if src_map.get(sym):
            _count("price", src_map[sym])
    n_priced = sum(1 for u in out if u["price"])
    source = (f"static_top{target_n}_live_prices" if n_priced > 0
              else f"static_top{target_n}_no_prices")
    if book:
        source += f"+factor_book_{len(book)}"
    UNIVERSE_META.update(
        universe_basis=(f"static S&P top-{target_n} ({len(core)}) + factor book analyst-covered US common stocks "
                        f"(>= {BOOK_MIN_PT_N} targets last quarter, >= ${int(BOOK_MIN_MCAP / 1e9)}B; {len(book)})") if book
        else f"static S&P top-{target_n} ({len(core)}); factor book unavailable",
        n_book_candidates=len(book), book_as_of=(_book.as_of() if book_ok() else None))
    return out, source, n_priced


# ---------- FMP wrappers ----------
    url = (f"{FMP_BASE}/grades?symbol={symbol}&limit=60&apikey={FMP_KEY}")
    d = http_json(url)
    return d if isinstance(d, list) else []


def fmp_pt_consensus(symbol):
    url = f"{FMP_BASE}/price-target-consensus?symbol={symbol}&apikey={FMP_KEY}"
    d = http_json(url)
    if isinstance(d, list) and d:
        return d[0]
    if isinstance(d, dict) and "_error" not in d:
        return d
    return {}


def fmp_grades(symbol):
    url = f"{FMP_BASE}/grades?symbol={symbol}&limit=60&apikey={FMP_KEY}"
    d = http_json(url)
    return d if isinstance(d, list) else []


def fmp_earnings_surprises(symbol):
    url = (f"{FMP_BASE}/earnings?symbol={symbol}"
           f"&limit=12&apikey={FMP_KEY}")
    d = http_json(url)
    return d if isinstance(d, list) else []


# ---------- Bank-first readers (v1.1.0) ----------
def grades_for(symbol):
    """(rows, basis): the walker's per-symbol grades file, else the live /stable/grades call."""
    if _bank is not None:
        try:
            rows = _bank.walker("grades", symbol)
        except Exception:  # noqa: BLE001
            rows = None
        if isinstance(rows, list) and rows:
            return rows, "fmp-walker grades"
    rows = fmp_grades(symbol)
    time.sleep(FMP_SLEEP_SEC)
    return rows, ("fmp live" if rows else "none")


def pt_for(symbol):
    """(pt_data, basis): the walker's price-target-consensus file, else the factor book's price-target summary
    (last-quarter average target + count), else the live call."""
    if _bank is not None:
        try:
            rows = _bank.walker("price-target-consensus", symbol)
        except Exception:  # noqa: BLE001
            rows = None
        if isinstance(rows, list) and rows and isinstance(rows[0], dict):
            return rows[0], "fmp-walker price-target-consensus"
        if isinstance(rows, dict) and rows:
            return rows, "fmp-walker price-target-consensus"
    if book_ok():
        try:
            row = _book.lookup(symbol, ["pt_avg_1q", "pt_avg_1m", "pt_n_1q", "pt_n_1m"])
        except Exception:  # noqa: BLE001
            row = None
        if row and (row.get("pt_avg_1q") or row.get("pt_avg_1m")):
            return {"targetMean": row.get("pt_avg_1q") or row.get("pt_avg_1m"),
                    "numberOfAnalysts": row.get("pt_n_1q") or row.get("pt_n_1m")}, "factor-book price-target summary (1q avg)"
    pt = fmp_pt_consensus(symbol)
    time.sleep(FMP_SLEEP_SEC)
    return pt, ("fmp live" if pt else "none")


def surprises_for(symbol):
    """(rows, basis): reported quarters from the walker's earnings file, else the factor book's surprises block
    (actualEarningResult / estimatedEarning), else the live /stable/earnings call."""
    if _bank is not None:
        try:
            rows = _bank.walker("earnings", symbol)
        except Exception:  # noqa: BLE001
            rows = None
        if isinstance(rows, list) and rows:
            past = [r for r in rows if isinstance(r, dict) and r.get("epsActual") is not None]
            if past:
                return past, "fmp-walker earnings"
    if book_ok():
        try:
            doc = _book.factor(symbol)
        except Exception:  # noqa: BLE001
            doc = None
        sur = [r for r in ((doc or {}).get("surprises") or []) if isinstance(r, dict)]
        if sur:
            return sur, "factor-book surprises"
    rows = fmp_earnings_surprises(symbol)
    time.sleep(FMP_SLEEP_SEC)
    return rows, ("fmp live" if rows else "none")


def bank_context(symbol):
    if not book_ok():
        return None
    try:
        row = _book.lookup(symbol, list(BANK_CONTEXT_FIELDS))
    except Exception:  # noqa: BLE001
        row = None
    if not row:
        return None
    out = {k: row.get(k) for k in BANK_CONTEXT_FIELDS}
    out["source"] = "factor-book"
    return out


# ---------- Factor computation ----------
def classify_firm(firm_name):
    if not firm_name:
        return TIER_C_WT
    fn = firm_name.strip()
    for tier_a in TIER_A_FIRMS:
        if tier_a in fn:
            return TIER_A_WT
    for tier_b in TIER_B_FIRMS:
        if tier_b in fn:
            return TIER_B_WT
    return TIER_C_WT


def is_upgrade(action, new_grade, prev_grade):
    """Determine if a rating change is an upgrade, downgrade, or neutral.
    Returns +1, -1, or 0."""
    if not action:
        action = ""
    a = action.lower()
    if "upgrade" in a:
        return 1
    if "downgrade" in a:
        return -1
    if "initiat" in a or "resum" in a or "reiterat" in a or "maintain" in a:
        # Use new grade if Buy/Outperform/Overweight = positive initiation
        g = (new_grade or "").lower()
        if any(k in g for k in ("buy", "outperform", "overweight", "positive",
                                "strong", "accumulate", "add")):
            return 0  # neutral - already known to market
        if any(k in g for k in ("sell", "underperform", "underweight",
                                "negative", "reduce")):
            return 0
    return 0


def compute_rrm(grades_list):
    """Rating Revision Momentum: net weighted upgrades over RRM_LOOKBACK_DAYS."""
    if not grades_list:
        return {"rrm_raw": 0.0, "n_upgrades": 0, "n_downgrades": 0,
                "n_total_revisions": 0, "n_tier_a_actions": 0}
    cutoff = datetime.now(timezone.utc) - timedelta(days=RRM_LOOKBACK_DAYS)
    n_up = 0
    n_down = 0
    n_a = 0
    weighted_sum = 0.0
    for g in grades_list:
        date_str = g.get("date") or g.get("publishedDate") or ""
        try:
            d = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            if d.tzinfo is None:
                d = d.replace(tzinfo=timezone.utc)
        except Exception:
            continue
        if d < cutoff:
            continue
        action = g.get("action") or ""
        new_grade = g.get("newGrade") or g.get("gradingCompany") or ""
        prev_grade = g.get("previousGrade") or ""
        direction = is_upgrade(action, new_grade, prev_grade)
        if direction == 0:
            continue
        firm = g.get("gradingCompany") or ""
        wt = classify_firm(firm)
        if wt == TIER_A_WT:
            n_a += 1
        weighted_sum += direction * wt
        if direction > 0:
            n_up += 1
        else:
            n_down += 1
    return {
        "rrm_raw": round(weighted_sum, 2),
        "n_upgrades": n_up,
        "n_downgrades": n_down,
        "n_total_revisions": n_up + n_down,
        "n_tier_a_actions": n_a,
    }


def compute_ptd(pt_data, current_price):
    """Price Target Drift: (PT_consensus - px) / px - implicit conviction."""
    if not pt_data or not current_price or current_price <= 0:
        return {"ptd_pct": None, "pt_consensus": None,
                "n_pt_analysts": None}
    pt = (pt_data.get("targetConsensus") or
          pt_data.get("targetMedian") or
          pt_data.get("targetMean"))
    if pt is None:
        return {"ptd_pct": None, "pt_consensus": None,
                "n_pt_analysts": None}
    n_anal = (pt_data.get("numberOfAnalysts") or
              pt_data.get("targetCount"))
    ptd_pct = round((float(pt) - current_price) / current_price * 100, 2)
    # Cap extreme outliers
    ptd_pct = max(-99.0, min(199.0, ptd_pct))
    return {"ptd_pct": ptd_pct, "pt_consensus": round(float(pt), 2),
            "n_pt_analysts": n_anal}


def compute_esp(surprises_list):
    """Earnings Surprise Persistence: hit rate + avg magnitude over last 8q."""
    if not surprises_list:
        return {"esp_hit_pct": None, "esp_avg_magnitude_pct": None,
                "n_quarters": 0}
    surprises = sorted(surprises_list,
                       key=lambda x: x.get("date", "") or "", reverse=True)[:8]
    n = 0
    n_beats = 0
    mags = []
    for s in surprises:
        est = s.get("estimatedEarning") or s.get("epsEstimated")
        act = s.get("actualEarningResult") or s.get("epsActual") or s.get("eps")
        if est is None or act is None:
            continue
        try:
            est = float(est)
            act = float(act)
        except (ValueError, TypeError):
            continue
        n += 1
        if act > est:
            n_beats += 1
        if abs(est) > 0.01:
            mags.append((act - est) / abs(est) * 100)
    if n == 0:
        return {"esp_hit_pct": None, "esp_avg_magnitude_pct": None,
                "n_quarters": 0}
    hit_pct = round(100 * n_beats / n, 1)
    avg_mag = round(statistics.median(mags), 2) if mags else None
    return {"esp_hit_pct": hit_pct, "esp_avg_magnitude_pct": avg_mag,
            "n_quarters": n}


def safe_zscore(values, value):
    """Z-score of a value within a non-empty list of comparable values."""
    if not values or value is None:
        return None
    try:
        vals = [v for v in values if v is not None]
        if len(vals) < 5:
            return None
        m = statistics.mean(vals)
        sd = statistics.stdev(vals)
        if sd == 0:
            return 0.0
        z = (value - m) / sd
        # Cap at +/- 3 sigma
        return round(max(-3.0, min(3.0, z)), 2)
    except Exception:
        return None


def composite_score(z_rrm, z_ptd, z_esp):
    """Blend three factors -> 0-100 normalized score.
    Weights: RRM 50%, PTD 25%, ESP 25% (mirrors StarMine's revision focus)."""
    parts = []
    weights = []
    if z_rrm is not None:
        parts.append(z_rrm)
        weights.append(0.50)
    if z_ptd is not None:
        parts.append(z_ptd)
        weights.append(0.25)
    if z_esp is not None:
        parts.append(z_esp)
        weights.append(0.25)
    if not parts:
        return None
    # Renormalize weights for available factors
    wsum = sum(weights)
    z_composite = sum(p * w for p, w in zip(parts, weights)) / wsum
    # Map z to 0-100 using cumulative normal approximation
    # z=-2 -> ~2, z=-1 -> ~16, z=0 -> 50, z=+1 -> ~84, z=+2 -> ~98
    import math
    cdf = 0.5 * (1 + math.erf(z_composite / math.sqrt(2)))
    return round(cdf * 100, 1)


# ---------- Main analyze ----------
def analyze_ticker(symbol, price):
    """Run all 3 factor computations for one ticker (bank first; live calls paced)."""
    grades, g_basis = grades_for(symbol)
    pt, p_basis = pt_for(symbol)
    surp, s_basis = surprises_for(symbol)
    _count("grades", g_basis)
    _count("pt", p_basis)
    _count("esp", s_basis)
    rrm = compute_rrm(grades)
    ptd = compute_ptd(pt, price)
    esp = compute_esp(surp)
    return {
        "ticker": symbol,
        "price": price,
        "rrm": rrm,
        "ptd": ptd,
        "esp": esp,
        "basis": {"grades": g_basis, "pt": p_basis, "esp": s_basis},
    }


def classify_regime(median_z):
    if median_z is None:
        return "DATA_UNAVAILABLE"
    for thresh, regime in REGIME_BANDS:
        if median_z >= thresh:
            return regime
    return "BEARISH_REVISIONS"


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
        urllib.request.urlopen(urllib.request.Request(url, data=data),
                               timeout=10)
    except Exception:
        pass


def lambda_handler(event, context):
    started = datetime.now(timezone.utc)
    log = []
    for k in BASIS:
        BASIS[k] = {}
    UNIVERSE_META.clear()

    if not FMP_KEY and not book_ok():
        return {"statusCode": 500,
                "body": json.dumps({"ok": False, "error": "FMP_KEY not set and the factor book is unavailable"})}

    # 1. Acquire universe + current prices in ONE batched FMP call (was 11)
    universe, universe_source, n_priced = acquire_universe_with_prices()
    log.append(f"universe: {len(universe)} tickers, {n_priced} priced, "
               f"source={universe_source}")
    if not universe:
        return {"statusCode": 500,
                "body": json.dumps({"ok": False,
                                    "error": "universe acquisition failed",
                                    "log": log})}

    # 2. Per-ticker analyst skill factors
    per_ticker = []
    for i, u in enumerate(universe):
        sym = u["symbol"]
        px = u.get("price")
        if not sym:
            continue
        try:
            row = analyze_ticker(sym, px)
            row["sector"] = u.get("sector", "")
            row["market_cap_usd"] = u.get("market_cap_usd")
            row["price_source"] = u.get("price_source")
            row["from_book"] = u.get("from_book", False)
            per_ticker.append(row)
        except Exception as e:
            log.append(f"analyze_err {sym}: {str(e)[:80]}")
        if i % 10 == 9:
            log.append(f"progress: {i+1}/{len(universe)}")

    log.append(f"per_ticker_completed: {len(per_ticker)}")
    log.append("basis: " + json.dumps(BASIS, sort_keys=True))

    # 4. Z-score normalization across universe
    rrm_vals = [t["rrm"]["rrm_raw"] for t in per_ticker]
    ptd_vals = [t["ptd"]["ptd_pct"] for t in per_ticker
                if t["ptd"]["ptd_pct"] is not None]
    esp_vals = [t["esp"]["esp_hit_pct"] for t in per_ticker
                if t["esp"]["esp_hit_pct"] is not None]

    for t in per_ticker:
        t["z_rrm"] = safe_zscore(rrm_vals, t["rrm"]["rrm_raw"])
        t["z_ptd"] = safe_zscore(ptd_vals, t["ptd"]["ptd_pct"])
        t["z_esp"] = safe_zscore(esp_vals, t["esp"]["esp_hit_pct"])
        t["starmine_score"] = composite_score(t["z_rrm"], t["z_ptd"], t["z_esp"])

    # 5. Universe regime
    scored = [t for t in per_ticker if t["starmine_score"] is not None]
    if scored:
        z_composites = []
        for t in scored:
            zs = [z for z in (t["z_rrm"], t["z_ptd"], t["z_esp"])
                  if z is not None]
            if zs:
                z_composites.append(statistics.mean(zs))
        median_z = statistics.median(z_composites) if z_composites else None
    else:
        median_z = None
    regime = classify_regime(median_z)

    # 6. Rankings
    by_score = sorted(scored, key=lambda x: x["starmine_score"], reverse=True)
    top_25 = by_score[:25]
    bottom_25 = list(reversed(by_score[-25:]))

    # 7. Sector breakdown for top 25
    sector_top25 = {}
    for t in top_25:
        sec = t.get("sector", "Unknown") or "Unknown"
        sector_top25[sec] = sector_top25.get(sec, 0) + 1

    # 8. Build output
    out = {
        "ok": True,
        "version": VERSION,
        "generated_at": started.isoformat(),
        "universe_regime": regime,
        "universe_median_composite_z": (round(median_z, 2)
                                         if median_z is not None else None),
        "universe_source_tier": universe_source,
        "universe_basis": UNIVERSE_META.get("universe_basis"),
        "n_book_candidates": UNIVERSE_META.get("n_book_candidates", 0),
        "book_as_of": UNIVERSE_META.get("book_as_of"),
        "basis_counts": BASIS,
        "n_universe_analyzed": len(per_ticker),
        "n_scored": len(scored),
        "median_rrm_raw": (round(statistics.median(rrm_vals), 2)
                           if rrm_vals else None),
        "median_ptd_pct": (round(statistics.median(ptd_vals), 2)
                           if ptd_vals else None),
        "median_esp_hit_pct": (round(statistics.median(esp_vals), 1)
                               if esp_vals else None),
        "sector_breakdown_top_25": sector_top25,
        "top_25_conviction": [
            {"ticker": t["ticker"], "sector": t["sector"],
             "price": t["price"], "price_source": t.get("price_source"), "basis": t.get("basis"),
             "from_book": t.get("from_book", False), "bank_context": bank_context(t["ticker"]),
             "starmine_score": t["starmine_score"],
             "z_rrm": t["z_rrm"], "z_ptd": t["z_ptd"], "z_esp": t["z_esp"],
             "rrm_raw": t["rrm"]["rrm_raw"],
             "n_upgrades_90d": t["rrm"]["n_upgrades"],
             "n_downgrades_90d": t["rrm"]["n_downgrades"],
             "n_tier_a_actions": t["rrm"]["n_tier_a_actions"],
             "ptd_pct": t["ptd"]["ptd_pct"],
             "pt_consensus": t["ptd"]["pt_consensus"],
             "n_pt_analysts": t["ptd"]["n_pt_analysts"],
             "esp_hit_pct": t["esp"]["esp_hit_pct"],
             "esp_avg_magnitude_pct": t["esp"]["esp_avg_magnitude_pct"],
             "n_quarters": t["esp"]["n_quarters"],
             "market_cap_usd": t.get("market_cap_usd")}
            for t in top_25
        ],
        "bottom_25_conviction": [
            {"ticker": t["ticker"], "sector": t["sector"],
             "price": t["price"], "price_source": t.get("price_source"), "basis": t.get("basis"),
             "from_book": t.get("from_book", False), "bank_context": bank_context(t["ticker"]),
             "starmine_score": t["starmine_score"],
             "z_rrm": t["z_rrm"], "z_ptd": t["z_ptd"], "z_esp": t["z_esp"],
             "rrm_raw": t["rrm"]["rrm_raw"],
             "n_upgrades_90d": t["rrm"]["n_upgrades"],
             "n_downgrades_90d": t["rrm"]["n_downgrades"],
             "ptd_pct": t["ptd"]["ptd_pct"],
             "pt_consensus": t["ptd"]["pt_consensus"],
             "esp_hit_pct": t["esp"]["esp_hit_pct"],
             "market_cap_usd": t.get("market_cap_usd")}
            for t in bottom_25
        ],
        "methodology": {
            "factors": {
                "rrm": ("Rating Revision Momentum: weighted (Tier A 1.5x / "
                        "Tier B 1.0x / others 0.6x) net upgrades minus "
                        "downgrades over last 90 days"),
                "ptd": ("Price Target Drift: (consensus PT - current price) "
                        "/ current price, in percent"),
                "esp": ("Earnings Surprise Persistence: % of last 8 "
                        "quarters where actual EPS beat estimate"),
            },
            "composite_weights": {"rrm": 0.50, "ptd": 0.25, "esp": 0.25},
            "score_mapping": "z -> standard-normal CDF -> 0-100 percentile",
            "universe": (UNIVERSE_META.get("universe_basis") or f"S&P 500 top-{UNIVERSE_TOP_N} by market cap"),
            "regime_bands": {"BULLISH_REVISIONS": "median z >= +1.0",
                             "NEUTRAL_REVISIONS": "-1.0 < median z < +1.0",
                             "BEARISH_REVISIONS": "median z <= -1.0"},
        },
        "sources": {
            "constituents": "static S&P top-50 + factor book data/warm/factor-book (analyst-covered US common stocks)",
            "quotes": "factor book bank close (FMP /stable/quote live fallback)",
            "grades": "FMP walker bank data/warm/fmp/grades (FMP /stable/grades live fallback)",
            "price_target": "FMP walker bank price-target-consensus, factor book price-target summary (FMP /stable/price-target-consensus live fallback)",
            "earnings_surprises": "FMP walker bank earnings, factor book surprises (FMP /stable/earnings live fallback)",
        },
        "edge_basis": ("Refinitiv StarMine showed analyst-revision-weighted "
                       "composites generate ~5-8% annualized alpha vs "
                       "consensus weighting (Liu & Su 2018). Tier weighting "
                       "captures the well-documented bulge-bracket signal "
                       "quality premium."),
        "log_summary": log[-8:],
    }

    # 9. Persist to S3
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

    # 10. Telegram alert on strong bullish/bearish revision regime
    if regime in ("BULLISH_REVISIONS", "BEARISH_REVISIONS"):
        top_3 = top_25[:3] if regime == "BULLISH_REVISIONS" else bottom_25[:3]
        emoji = "📈" if regime == "BULLISH_REVISIONS" else "📉"
        names = ", ".join(f"{t['ticker']}({t['starmine_score']})" for t in top_3)
        telegram_notify(
            f"{emoji} *StarMine {regime}*\n"
            f"Median composite z: {round(median_z, 2)}\n"
            f"Top {('conviction' if regime == 'BULLISH_REVISIONS' else 'bearish')}: {names}\n"
            f"justhodl.ai/starmine.html"
        )

    # Lightweight summary in response body (S3 has the full payload)
    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "version": VERSION,
            "regime": regime,
            "n_analyzed": len(per_ticker),
            "n_scored": len(scored),
            "median_composite_z": (round(median_z, 2)
                                    if median_z is not None else None),
            "top_3": [{"t": t["ticker"], "score": t["starmine_score"]}
                      for t in top_25[:3]],
            "bottom_3": [{"t": t["ticker"], "score": t["starmine_score"]}
                         for t in bottom_25[:3]],
        }),
    }


if __name__ == "__main__":
    r = lambda_handler({}, None)
    print(json.dumps(r, indent=2))
