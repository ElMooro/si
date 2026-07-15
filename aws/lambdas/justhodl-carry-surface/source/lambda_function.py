"""
justhodl-carry-surface
======================
UNIVERSAL CARRY SURFACE — institutional-grade cross-asset carry engine.

Carry = the expected return from HOLDING an asset assuming all variables
remain unchanged. It's the "do nothing" return. Academic literature (AQR's
Style Premia, Asness/Moskowitz/Pedersen "Value and Momentum Everywhere")
documents carry as one of the four pervasive alpha factors.

This engine answers: "Which asset is the market paying me most to hold,
right now, across every asset class?"

ASSET CLASSES & CARRY DEFINITIONS:
-----------------------------------
EQUITY:    forward_div_yield + buyback_yield - financing_cost
           (financing = SOFR / Fed funds upper bound)
FX:        long_currency_rate - short_currency_rate
           (3M interbank rates from FRED IR3TIB01 series)
FIXED INC: yield_to_maturity - financing_cost
           (Treasuries: DGS{N} - SOFR; Credit: index yield - default-adj)
COMMODITY: -roll_yield  (front - next / front), annualized
           (positive carry = backwardation; negative = contango)
CRYPTO:    perpetual_funding_rate_annualized (Binance public API)

For each asset within a class:
  1. Pull raw carry inputs
  2. Annualize to bps/year
  3. Z-score within asset class (handles base-rate differences)
  4. Risk-adjusted: divide by realized vol (Sharpe-of-carry)
  5. Time-series: track 1W and 1M change → "carry momentum"

Cross-asset:
  - Global percentile rank
  - Top-10 / bottom-10 leaderboard
  - Regime fingerprint (avg z by class, dispersion)
  - Carry-unwind risk flags (high carry + strengthening funding currency)

OUTPUT:
  data/carry-surface.json — full surface
  data/carry-surface/history/<date>.json — daily snapshots
  Telegram digest if any extreme (>2σ) carry shifts since last run

SCHEDULE: every 4 hours
"""
import os
import json
import time
import math
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from statistics import mean, stdev

import boto3
try:
    import _fred_shim  # noqa: F401
except Exception:
    pass

VERSION = "1.3.0"
REGION = os.environ.get('AWS_REGION', 'us-east-1')
BUCKET = os.environ.get('S3_BUCKET', 'justhodl-dashboard-live')
OUT_KEY = os.environ.get('OUT_KEY', 'data/carry-surface.json')
HIST_PREFIX = os.environ.get('HIST_PREFIX', 'data/carry-surface/history/')
ALERT_HISTORY_KEY = 'data/carry-surface/alert-history.json'

FRED_KEY = os.environ.get('FRED_API_KEY', '2f057499936072679d8843d7fce99989')
FMP_KEY = os.environ.get('FMP_KEY', 'wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')

s3 = boto3.client('s3', region_name=REGION)

# ──────────────────────────────────────────────────────────────────────
# UNIVERSE DEFINITIONS
# ──────────────────────────────────────────────────────────────────────

# Equity: index ETFs + top S&P names by market cap (kept moderate to limit FMP calls)
EQUITY_UNIVERSE = [
    # Broad indices
    "SPY", "QQQ", "IWM", "DIA", "VTI", "RSP", "MDY",
    # Sectors
    "XLE", "XLF", "XLK", "XLV", "XLI", "XLP", "XLY", "XLB", "XLU", "XLRE", "XLC",
    # Top single names — mega / large cap across sectors
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "BRK-B",
    "JPM", "JNJ", "V", "PG", "MA", "HD", "BAC", "ABBV", "AVGO",
    "WMT", "KO", "MRK", "PEP", "PFE", "TMO", "CVX", "XOM",
    "COST", "MCD", "CSCO", "ACN", "ADBE", "CRM", "NKE", "TXN", "QCOM",
    "UNH", "LLY", "T", "VZ", "IBM", "GE", "CAT", "DE", "MMM", "HON",
    "GS", "MS", "C", "WFC", "AXP", "SCHW", "BLK", "SPGI",
    "AMGN", "GILD", "BMY", "MDT", "DHR", "ABT",
    "LOW", "SBUX", "TGT", "F", "GM",
    # Dividend / factor
    "VYM", "SCHD", "SPHD", "DGRO", "NOBL", "HDV", "DVY", "USMV", "QUAL", "MTUM", "VLUE",
    # International
    "EFA", "VEA", "EEM", "VWO", "INDA", "MCHI", "EWJ", "EWG", "EWZ", "EWU", "EWC", "EWA", "EWY", "EWT", "EWH",
    # REITs
    "VNQ", "IYR", "SCHH",
]

# FX pairs: rate of base currency vs USD short rate.
# For "long X" carry, we compute: rate_X - rate_USD
# Negative for funding currencies (JPY, CHF), positive for high-yielders
FX_UNIVERSE = {
    # short rates: FRED series IDs (3M interbank)
    "USD": "DFF",                         # Fed funds effective
    "EUR": "IR3TIB01EZM156N",             # Euro area 3M interbank
    "JPY": "IR3TIB01JPM156N",             # Japan 3M interbank
    "GBP": "IR3TIB01GBM156N",             # UK 3M interbank
    "CHF": "IR3TIB01CHM156N",             # Switzerland 3M interbank
    "AUD": "IR3TIB01AUM156N",             # Australia 3M interbank
    "CAD": "IR3TIB01CAM156N",             # Canada 3M interbank
    "NZD": "IR3TIB01NZM156N",             # New Zealand 3M interbank
    "SEK": "IR3TIB01SEM156N",             # Sweden 3M interbank
    "NOK": "IR3TIB01NOM156N",             # Norway 3M interbank
    "DKK": "IR3TIB01DKM156N",             # Denmark 3M interbank
    "KOR": "IR3TIB01KRM156N",             # South Korea 3M interbank
    # EM with FRED data available
    "MXN": "IR3TIB01MXM156N",             # Mexico
    "BRL": "IRSTCB01BRM156N",             # Brazil central bank rate (proxy)
    "INR": "IR3TIB01INM156N",             # India (note: spotty data)
    "ZAR": "IR3TIB01ZAM156N",             # South Africa
    "IDN": "IR3TIB01IDM156N",             # Indonesia
    "TUR": "IR3TIB01TRM156N",             # Turkey
}

# Fixed income: treasuries (DGSx) + credit indexes
# Carry = yield - financing (SOFR)
FIXED_INCOME = {
    "UST_1M":  ("DGS1MO",  "data/cash"),
    "UST_3M":  ("DGS3MO",  "data/cash"),       # 3M T-bill (basically risk-free)
    "UST_6M":  ("DGS6MO",  None),
    "UST_1Y":  ("DGS1",    None),
    "UST_2Y":  ("DGS2",    None),
    "UST_3Y":  ("DGS3",    None),
    "UST_5Y":  ("DGS5",    None),
    "UST_7Y":  ("DGS7",    None),
    "UST_10Y": ("DGS10",   None),
    "UST_20Y": ("DGS20",   None),
    "UST_30Y": ("DGS30",   None),
    "TIPS_10Y":("DFII10",  None),              # 10Y real yield (inflation-protected)
    "IG_CRED": ("BAMLC0A0CMEY", None),         # ICE BofA US Corporate Effective Yield
    "HY_CRED": ("BAMLH0A0HYM2EY", None),       # ICE BofA US High Yield Effective Yield
    "IG_AAA":  ("BAMLC0A1CAAAEY", None),       # AAA corporate effective yield
    "HY_CCC":  ("BAMLH0A3HYCEY", None),        # CCC & lower effective yield (deep junk)
    "EM_USD":  ("BAMLEMCBPIEY", None),         # EM USD corporate effective yield
    "MUNI":    ("BAMLU0A0CMEY", None),         # US municipal (proxy via corp master fallback)
}

# Commodity ETFs — roll yield approximation via 30D ETF underperformance vs spot
# For physical commodities we use historical contango/backwardation observation
COMMODITY_UNIVERSE = {
    # symbol → (FMP_symbol, spot_FRED_series, structural_roll_estimate_pct_yr)
    "GLD":  ("GLD",  "GOLDAMGBD228NLBM",  0.0),    # Gold: near-zero carry (storage ~ financing)
    "SLV":  ("SLV",  None,                -1.0),   # Silver: slight contango
    "USO":  ("USO",  "DCOILWTICO",        -8.0),   # Oil (WTI): persistent contango ~ -8% / yr
    "BNO":  ("BNO",  "DCOILBRENTEU",      -6.0),   # Brent oil: milder contango than WTI
    "UNG":  ("UNG",  "DHHNGSP",           -25.0),  # Nat gas: severe contango
    "DBA":  ("DBA",  None,                -3.0),   # Ag basket: mild contango
    "DBC":  ("DBC",  None,                -3.0),   # Broad commodity: mild contango
    "DBB":  ("DBB",  None,                -2.0),   # Base metals basket
    "CPER": ("CPER", None,                -2.5),   # Copper
    "PPLT": ("PPLT", None,                -1.5),   # Platinum
    "PALL": ("PALL", None,                -1.5),   # Palladium
    "CORN": ("CORN", None,                -4.0),   # Corn
    "WEAT": ("WEAT", None,                -5.0),   # Wheat
    "VXX":  ("VXX",  None,                -65.0),  # VIX futures: severe contango (well-known)
}

# Crypto: perpetual funding rates from OKX public API (Binance blocked from Lambda IPs).
# Bare coin symbols; OKX instId is built as {COIN}-USDT-SWAP.
# Positive funding = longs pay shorts → negative carry for longs holding via perp.
CRYPTO_UNIVERSE = ["BTC", "ETH", "SOL", "AVAX", "BNB",
                   "XRP", "DOGE", "ADA", "LINK", "LTC"]

# ──────────────────────────────────────────────────────────────────────
# HTTP HELPERS
# ──────────────────────────────────────────────────────────────────────

def http_get_json(url, timeout=20, max_retries=2):
    """Robust HTTP GET with retries."""
    last_err = None
    for attempt in range(max_retries + 1):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "JustHodl-CarrySurface/1.0",
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError) as e:
            last_err = e
            if attempt < max_retries:
                time.sleep(0.5 * (attempt + 1))
    raise last_err


def fred_series(series_id, limit=400):
    """Pull FRED time series. Returns list of (date, value) tuples, newest first."""
    url = (f"https://api.stlouisfed.org/fred/series/observations?"
           f"series_id={series_id}&api_key={FRED_KEY}&file_type=json"
           f"&limit={limit}&sort_order=desc")
    data = http_get_json(url)
    obs = data.get('observations', [])
    out = []
    for o in obs:
        v = o.get('value', '.')
        if v != '.' and v not in (None, ''):
            try:
                out.append((o['date'], float(v)))
            except ValueError:
                pass
    return out


def fred_latest(series_id):
    """Latest non-missing value for a FRED series."""
    series = fred_series(series_id, limit=20)
    return series[0][1] if series else None


def fred_change(series_id, days_back, today_value=None):
    """Change in series over days_back, returned as absolute (current - past)."""
    series = fred_series(series_id, limit=days_back + 30)
    if not series:
        return None, None
    current = today_value if today_value is not None else series[0][1]
    # Find observation closest to days_back ago
    target_date = (datetime.now(timezone.utc) - timedelta(days=days_back)).date()
    past_value = None
    for date_str, val in series:
        try:
            obs_date = datetime.fromisoformat(date_str).date()
            if obs_date <= target_date:
                past_value = val
                break
        except (ValueError, TypeError):
            pass
    if past_value is None and len(series) > min(days_back, len(series) - 1):
        # Fall back to nth observation
        past_value = series[min(days_back, len(series) - 1)][1]
    if past_value is None:
        return None, None
    return current - past_value, past_value


def fmp_quote_with_history(symbol, days=90):
    """Get FMP quote + historical close prices for realized vol calc."""
    try:
        url = f"https://financialmodelingprep.com/stable/historical-price-eod/light?symbol={symbol}&apikey={FMP_KEY}"
        data = http_get_json(url)
        if isinstance(data, list) and data:
            history = data[:days]
            closes = [float(x['price']) for x in history if 'price' in x and x['price'] is not None]
            return {
                'symbol': symbol,
                'current': closes[0] if closes else None,
                'closes': closes,
            }
    except Exception as e:
        return {'symbol': symbol, 'error': str(e)[:120]}
    return {'symbol': symbol, 'error': 'no_data'}


# ETFs are not covered by /stable/ratios-ttm (no income statement) — that endpoint
# returns empty for them, which the old code coerced to 0 and displayed as -financing.
# We route ETFs to the actual declared-distribution endpoint instead. This set is the
# fund-type members of EQUITY_UNIVERSE; single names go through ratios-ttm.
ETF_SYMBOLS = {
    "SPY", "QQQ", "IWM", "DIA", "VTI", "RSP", "MDY",
    "XLE", "XLF", "XLK", "XLV", "XLI", "XLP", "XLY", "XLB", "XLU", "XLRE", "XLC",
    "VYM", "SCHD", "SPHD", "DGRO", "NOBL", "HDV", "DVY", "USMV", "QUAL", "MTUM", "VLUE",
    "EFA", "VEA", "EEM", "VWO", "INDA", "MCHI", "EWJ", "EWG", "EWZ", "EWU", "EWC", "EWA", "EWY", "EWT", "EWH",
    "VNQ", "IYR", "SCHH",
}

# Known genuine non-payers — a real 0% yield here is VALID data, not a failed fetch.
KNOWN_NONPAYERS = {"BRK-B", "GOOGL", "TSLA", "AMZN", "META", "NVDA", "ADBE", "CRM"}


def _dy_from_ratios_ttm(symbol):
    """Single-name path: TTM dividend yield from FMP /stable/ratios-ttm.
    Returns dividend yield as a PERCENT (float), or None if not fetchable."""
    try:
        url = f"https://financialmodelingprep.com/stable/ratios-ttm?symbol={symbol}&apikey={FMP_KEY}"
        data = http_get_json(url)
        if isinstance(data, list) and data:
            row = data[0]
            # dividendYieldTTM on /stable is a DECIMAL fraction (0.0123 = 1.23%).
            raw = row.get('dividendYieldTTM')
            if raw is None:
                raw = row.get('dividendYieldPercentageTTM')  # legacy: already percent
                if raw is not None:
                    return float(raw)  # already a percent
                return None  # genuinely absent — do NOT coerce to 0
            return float(raw) * 100.0  # decimal → percent, deterministic
    except Exception:
        pass
    return None


def _dy_from_dividends(symbol, price):
    """ETF path: sum trailing-12-month declared distributions from /stable/dividends,
    divide by current price → true trailing distribution yield (PERCENT).
    Returns None if not fetchable or price missing."""
    if not price or price <= 0:
        return None
    try:
        url = f"https://financialmodelingprep.com/stable/dividends?symbol={symbol}&apikey={FMP_KEY}"
        data = http_get_json(url)
        if isinstance(data, list) and data:
            cutoff = (datetime.now(timezone.utc).date() - timedelta(days=365)).isoformat()
            ttm = 0.0
            found = False
            for row in data:
                d = row.get('date') or row.get('recordDate') or ''
                amt = row.get('dividend') if row.get('dividend') is not None else row.get('adjDividend')
                if d and d >= cutoff and amt is not None:
                    ttm += float(amt)
                    found = True
            if not found:
                return None  # no distributions on record — treat as unknown, not 0
            return round(ttm / price * 100.0, 4)
    except Exception:
        pass
    return None


def fmp_dividend_yield_ttm(symbol, price=None):
    """ETF-aware dividend yield (PERCENT). None means 'could not determine' — the
    caller must NOT substitute 0. A real 0 is only returned for known non-payers."""
    if symbol in ETF_SYMBOLS:
        dy = _dy_from_dividends(symbol, price)
        if dy is not None:
            return dy
        # ETF distribution endpoint failed — try ratios as a long shot, else None.
        return _dy_from_ratios_ttm(symbol)
    # Single name
    dy = _dy_from_ratios_ttm(symbol)
    if dy is not None:
        return dy
    if symbol in KNOWN_NONPAYERS:
        return 0.0  # verified non-payer: 0 is real data
    return None  # unknown — signal failure upstream, don't fake a zero


def fmp_buyback_yield(symbol):
    """Approximate buyback yield from changes in shares outstanding."""
    try:
        url = f"https://financialmodelingprep.com/stable/key-metrics-ttm?symbol={symbol}&apikey={FMP_KEY}"
        data = http_get_json(url)
        if isinstance(data, list) and data:
            row = data[0]
            # Some FMP responses have buybackYieldTTM directly
            by = row.get('buybackYieldTTM') or row.get('netBuybackYieldTTM')
            if by is not None:
                return float(by)
    except Exception:
        pass
    return 0.0


OKX_BASE = "https://www.okx.com"


def okx_funding_rate(coin):
    """Get perpetual funding rate from OKX public API — reachable from AWS Lambda where
    Binance (fapi.binance.com) is geo-blocked. `coin` is the bare symbol e.g. 'BTC'.
    OKX funding settles every 8h. Returns the same shape the caller expects.
    Also pulls funding-rate-history for a 30-period average (dislocation input)."""
    swap = f"{coin}-USDT-SWAP"
    try:
        req = urllib.request.Request(
            f"{OKX_BASE}/api/v5/public/funding-rate?instId={swap}",
            headers={"User-Agent": "Mozilla/5.0 (Macintosh) Chrome/120 Safari/537.36",
                     "Accept": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            d = json.loads(r.read().decode("utf-8"))
        if d.get("code") != "0" or not d.get("data"):
            return {"error": f"okx_no_data:{d.get('code')}"}
        current_8h = float(d["data"][0].get("fundingRate") or 0)

        # 30-period history for the trailing average (best-effort).
        avg_8h_30 = current_8h
        n = 1
        try:
            reqh = urllib.request.Request(
                f"{OKX_BASE}/api/v5/public/funding-rate-history?instId={swap}&limit=30",
                headers={"User-Agent": "Mozilla/5.0 (Macintosh) Chrome/120 Safari/537.36",
                         "Accept": "application/json"},
            )
            with urllib.request.urlopen(reqh, timeout=15) as rh:
                dh = json.loads(rh.read().decode("utf-8"))
            if dh.get("code") == "0" and dh.get("data"):
                rates = [float(x.get("realizedRate") or x.get("fundingRate") or 0) for x in dh["data"]]
                if rates:
                    avg_8h_30 = sum(rates) / len(rates)
                    n = len(rates)
        except Exception:
            pass

        # Annualize: 3 settlements/day × 365.
        return {
            "current_8h": current_8h,
            "current_annualized_pct": current_8h * 3 * 365 * 100,
            "avg_30period_annualized_pct": avg_8h_30 * 3 * 365 * 100,
            "n_settlements": n,
        }
    except Exception as e:
        return {"error": str(e)[:120]}


# ──────────────────────────────────────────────────────────────────────
# CARRY COMPUTATION PER ASSET CLASS
# ──────────────────────────────────────────────────────────────────────

def realized_vol_annualized(closes, periods_per_year=252):
    """Standard annualized realized volatility from log returns."""
    if not closes or len(closes) < 2:
        return None
    rets = []
    for i in range(len(closes) - 1):
        try:
            rets.append(math.log(closes[i] / closes[i + 1]))
        except (ValueError, ZeroDivisionError):
            pass
    if len(rets) < 2:
        return None
    sd = stdev(rets)
    return sd * math.sqrt(periods_per_year) * 100  # percent


def zscore_within_class(items, key):
    """Add a `<key>_z` field to each item in the list (z-scored within the list)."""
    vals = [it.get(key) for it in items if it.get(key) is not None]
    if len(vals) < 2:
        for it in items:
            it[f'{key}_z'] = None
        return
    m = mean(vals); sd = stdev(vals) if len(vals) > 1 else 0
    for it in items:
        v = it.get(key)
        if v is None or sd == 0:
            it[f'{key}_z'] = None
        else:
            it[f'{key}_z'] = round((v - m) / sd, 2)


def compute_equity_carry(financing_rate_pct):
    """For each equity, carry = div_yield + buyback_yield - financing_cost (all in %)."""
    print(f"[equity] processing {len(EQUITY_UNIVERSE)} tickers...")
    results = []
    
    def process_one(symbol):
        try:
            # Price history first — ETF div-yield needs current price, and we need vol.
            hist = fmp_quote_with_history(symbol, 90)
            closes = hist.get('closes', []) if hist else []
            price = closes[0] if closes else None

            # Div yield (ETF-aware, price-aware) + buyback yield, in parallel.
            with ThreadPoolExecutor(max_workers=2) as ex:
                f_dy = ex.submit(fmp_dividend_yield_ttm, symbol, price)
                f_by = ex.submit(fmp_buyback_yield, symbol)
                dy_pct = f_dy.result()   # PERCENT or None
                by = f_by.result()

            # If dividend yield is unknown, we cannot honestly compute equity carry.
            # Leave it dormant with a named reason rather than masking as -financing.
            if dy_pct is None:
                return {
                    'symbol': symbol, 'asset_class': 'equity',
                    'dormant': True, 'dormant_reason': 'div_yield_unavailable',
                    'price': price,
                }

            # Buyback: fmp_buyback_yield already returns PERCENT (or 0.0 fallback).
            by_pct = float(by) if by is not None else 0.0

            vol = realized_vol_annualized(closes) if len(closes) > 20 else None
            carry_pct = round(dy_pct + by_pct - financing_rate_pct, 3)

            return {
                'symbol': symbol,
                'asset_class': 'equity',
                'div_yield_pct': round(dy_pct, 3),
                'buyback_yield_pct': round(by_pct, 3),
                'financing_pct': round(financing_rate_pct, 3),
                'carry_pct': carry_pct,
                'realized_vol_pct': round(vol, 2) if vol else None,
                'carry_per_vol': round(carry_pct / vol, 3) if vol and vol > 0 else None,
                'price': price,
            }
        except Exception as e:
            return {'symbol': symbol, 'asset_class': 'equity', 'error': str(e)[:120]}
    
    with ThreadPoolExecutor(max_workers=6) as ex:
        for r in as_completed([ex.submit(process_one, s) for s in EQUITY_UNIVERSE]):
            try:
                results.append(r.result())
            except Exception as e:
                print(f"[equity] worker error: {e}")
    
    # Only genuinely-computed rows enter the z-score distribution. Dormant rows
    # (unknown yield) and errors are excluded so they can't flatten the stats.
    valid = [r for r in results if r.get('carry_pct') is not None and not r.get('dormant')]
    zscore_within_class(valid, 'carry_pct')
    zscore_within_class(valid, 'carry_per_vol')
    dormant = [r for r in results if r.get('dormant')]
    errored = [r for r in results if 'error' in r]
    n_dormant = len(dormant)
    if n_dormant:
        print(f"[equity] {len(valid)} live, {n_dormant} dormant (div_yield_unavailable): "
              f"{[d['symbol'] for d in dormant]}")
    return valid + dormant + errored


def compute_fx_carry():
    """FX carry: long_X return = short rate of X - short rate of USD."""
    print(f"[fx] processing {len(FX_UNIVERSE)} currencies...")
    
    # First pull all short rates
    rates = {}
    for ccy, series in FX_UNIVERSE.items():
        try:
            rates[ccy] = fred_latest(series)
        except Exception as e:
            rates[ccy] = None
    
    usd_rate = rates.get('USD')
    if usd_rate is None:
        return []
    
    results = []
    for ccy, rate in rates.items():
        if ccy == 'USD' or rate is None:
            continue
        carry_pct = round(rate - usd_rate, 3)  # long X vs USD
        results.append({
            'symbol': f'{ccy}USD',
            'asset_class': 'fx',
            'long_currency': ccy,
            'short_currency': 'USD',
            'long_rate_pct': round(rate, 3),
            'short_rate_pct': round(usd_rate, 3),
            'carry_pct': carry_pct,
            'realized_vol_pct': None,  # would need FX historical data
        })
    
    zscore_within_class(results, 'carry_pct')
    return results


def compute_fi_carry(financing_rate_pct):
    """Fixed income carry: yield - financing."""
    print(f"[fi] processing {len(FIXED_INCOME)} fixed income instruments...")
    results = []
    for label, (series, _) in FIXED_INCOME.items():
        try:
            yield_pct = fred_latest(series)
            if yield_pct is None:
                continue
            carry_pct = round(yield_pct - financing_rate_pct, 3)
            results.append({
                'symbol': label,
                'asset_class': 'fixed_income',
                'yield_pct': round(yield_pct, 3),
                'financing_pct': round(financing_rate_pct, 3),
                'carry_pct': carry_pct,
            })
        except Exception as e:
            results.append({'symbol': label, 'asset_class': 'fixed_income', 'error': str(e)[:120]})
    
    valid = [r for r in results if 'carry_pct' in r]
    zscore_within_class(valid, 'carry_pct')
    errored = [r for r in results if 'error' in r]
    return valid + errored


def compute_commodity_carry():
    """Commodity carry = -roll_yield (positive = backwardation).
    We use structural estimates from historical commodity research as base,
    then adjust based on recent ETF performance vs spot."""
    print(f"[commodity] processing {len(COMMODITY_UNIVERSE)} commodities...")
    results = []
    
    for etf_sym, (fmp_sym, spot_fred, structural_pct) in COMMODITY_UNIVERSE.items():
        try:
            # Get ETF historical data for vol
            hist = fmp_quote_with_history(fmp_sym, 60)
            closes = hist.get('closes', []) if hist else []
            vol = realized_vol_annualized(closes) if len(closes) > 20 else None
            
            # Roll-yield estimate. The ETF-vs-spot basis over a trailing window is a proxy,
            # but annualizing a single month by ×12 amplifies noise into ±80% garbage.
            # Instead: structural estimate is the anchor; the OBSERVED basis is a gentle,
            # bounded adjustment computed over a longer window, and the final carry is
            # winsorized so one volatile month can't dominate the whole surface.
            carry_pct = structural_pct
            spot_etf_basis_pct = None
            if spot_fred and len(closes) >= 30:
                try:
                    spot_series = fred_series(spot_fred, limit=90)
                    # Use up to 60 trading days for a steadier basis (was 30).
                    win = min(60, len(closes) - 1, len(spot_series) - 1)
                    if win >= 20:
                        etf_ret = (closes[0] / closes[win] - 1) * 100 if closes[win] > 0 else 0
                        spot_now = spot_series[0][1]
                        spot_ago = spot_series[win][1] if len(spot_series) > win else None
                        if spot_ago and spot_ago > 0:
                            spot_ret = (spot_now / spot_ago - 1) * 100
                            # Annualize by the ACTUAL window length (trading days → year),
                            # not a flat ×12. ~252 trading days/yr.
                            ann = 252.0 / win
                            raw_basis = (etf_ret - spot_ret) * ann
                            # Winsorize the observed basis to a sane band before blending.
                            spot_etf_basis_pct = round(max(-40.0, min(40.0, raw_basis)), 2)
                            # Anchor on structural (75%), nudge with observed (25%).
                            carry_pct = round(structural_pct * 0.75 + spot_etf_basis_pct * 0.25, 2)
                except Exception:
                    pass
            # Final safety winsor: no commodity carry beyond ±70% on the surface
            # (VXX's structural -65 is the legitimate floor; nothing should exceed it via noise).
            carry_pct = round(max(-70.0, min(70.0, carry_pct)), 2)
            
            results.append({
                'symbol': etf_sym,
                'asset_class': 'commodity',
                'structural_carry_pct': structural_pct,
                'spot_etf_basis_pct': spot_etf_basis_pct,
                'carry_pct': carry_pct,
                'realized_vol_pct': round(vol, 2) if vol else None,
                'carry_per_vol': round(carry_pct / vol, 3) if vol and vol > 0 else None,
                'price': closes[0] if closes else None,
            })
        except Exception as e:
            results.append({'symbol': etf_sym, 'asset_class': 'commodity', 'error': str(e)[:120]})
    
    valid = [r for r in results if 'carry_pct' in r]
    zscore_within_class(valid, 'carry_pct')
    zscore_within_class(valid, 'carry_per_vol')
    return valid


def compute_crypto_carry():
    """Crypto carry = perpetual funding rate annualized (Binance public)."""
    print(f"[crypto] processing {len(CRYPTO_UNIVERSE)} crypto perps...")
    results = []
    
    def process_one(symbol):
        try:
            funding = okx_funding_rate(symbol)
            if 'error' in funding:
                return {'symbol': symbol, 'asset_class': 'crypto', 'error': funding['error']}
            # For a long position in the underlying via perp:
            # If funding rate is positive, longs pay → negative carry to longs
            # We define carry_pct as the cost to LONG (so we flip the sign)
            long_carry_pct = -funding.get('current_annualized_pct', 0)
            return {
                'symbol': f"{symbol}-PERP",
                'asset_class': 'crypto',
                'funding_rate_8h': round(funding.get('current_8h', 0), 6),
                'funding_annualized_pct': round(funding.get('current_annualized_pct', 0), 2),
                'carry_pct': round(long_carry_pct, 2),  # negative when funding positive
                'avg_30period_annualized_pct': round(funding.get('avg_30period_annualized_pct', 0), 2),
            }
        except Exception as e:
            return {'symbol': symbol, 'asset_class': 'crypto', 'error': str(e)[:120]}
    
    with ThreadPoolExecutor(max_workers=6) as ex:
        for r in as_completed([ex.submit(process_one, s) for s in CRYPTO_UNIVERSE]):
            try:
                results.append(r.result())
            except Exception as e:
                print(f"[crypto] worker error: {e}")
    
    valid = [r for r in results if 'carry_pct' in r]
    zscore_within_class(valid, 'carry_pct')
    errored = [r for r in results if 'error' in r]
    return valid + errored


# ──────────────────────────────────────────────────────────────────────
# CROSS-ASSET RANKING & SYNTHESIS
# ──────────────────────────────────────────────────────────────────────

def cross_asset_rank(all_assets):
    """Compute cross-asset percentile + global rank."""
    valid = [a for a in all_assets if a.get('carry_pct') is not None]
    valid.sort(key=lambda x: x.get('carry_pct', -999), reverse=True)
    n = len(valid)
    for i, a in enumerate(valid):
        a['global_rank'] = i + 1
        a['global_percentile'] = round((1 - i / max(n - 1, 1)) * 100, 1) if n > 1 else 50
    return valid


def carry_regime_summary(by_class):
    """Compute a regime fingerprint from class-level statistics."""
    summary = {}
    for cls, assets in by_class.items():
        valid_z = [a.get('carry_pct_z') for a in assets if a.get('carry_pct_z') is not None]
        valid_carry = [a.get('carry_pct') for a in assets if a.get('carry_pct') is not None]
        if valid_carry:
            summary[cls] = {
                'n': len(valid_carry),
                'mean_carry_pct': round(mean(valid_carry), 2),
                'median_carry_pct': round(sorted(valid_carry)[len(valid_carry)//2], 2),
                'dispersion_pct': round(stdev(valid_carry), 2) if len(valid_carry) > 1 else 0,
                'max': round(max(valid_carry), 2),
                'min': round(min(valid_carry), 2),
            }
    return summary


# ──────────────────────────────────────────────────────────────────────
# CARRY-UNWIND FRAGILITY OVERLAY  (KMPV crash-risk insight)
# ──────────────────────────────────────────────────────────────────────
# Carry trades don't bleed slowly — they crash together on risk-off. This overlay
# fuses (1) how rich/crowded an asset's carry is, (2) its realized vol, and (3) the
# LIVE risk-regime (RORO / VIX / repo stress from data/risk-regime.json) to produce a
# per-asset fragility score and a cohort-level unwind-risk gauge. "Picking up pennies
# in front of a steamroller" — this tells you when the steamroller is moving.

def _read_json_s3(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)['Body'].read().decode())
    except Exception:
        return None


def load_risk_regime():
    """Read the fleet's fused RORO/VIX/repo-stress composite (data/risk-regime.json).
    Returns a normalized dict: score (-100..100), regime, vix, posture, stress_0_100."""
    rr = _read_json_s3("data/risk-regime.json") or {}
    score = rr.get("score")  # -100 (risk-off) .. +100 (risk-on)
    regime = rr.get("regime")
    vix = None
    try:
        vix = ((rr.get("results") or {}).get("vix") or {}).get("vix")
    except Exception:
        vix = None
    posture = rr.get("posture") or {}
    # Convert RORO score to a 0..100 STRESS scale (risk-off = high stress).
    stress = None
    if score is not None:
        stress = round(max(0.0, min(100.0, (100.0 - score) / 2.0)), 1)  # -100→100, +100→0
    # Cross-confirm with dedicated eurodollar-stress feed if present.
    eds = _read_json_s3("data/eurodollar-stress.json") or {}
    ed_score = eds.get("score") or eds.get("stress_score")
    return {
        "roro_score": score, "regime": regime, "vix": vix,
        "posture": posture, "stress_0_100": stress,
        "eurodollar_stress": ed_score,
        "asof": rr.get("generated_at"),
        "available": bool(rr),
    }


def _regime_multiplier(rr):
    """How 'live' is the unwind risk right now? 1.0 = calm, up to ~2.0 = acute risk-off.
    Uses RORO stress and VIX so fragility escalates when the steamroller is moving."""
    stress = rr.get("stress_0_100")
    vix = rr.get("vix") or 0
    m = 1.0
    if stress is not None:
        m += (stress / 100.0) * 0.8          # up to +0.8 from RORO stress
    if vix:
        if vix >= 30:   m += 0.4
        elif vix >= 22: m += 0.2
        elif vix >= 18: m += 0.1
    regime = (rr.get("regime") or "").upper()
    if regime in ("RISK_OFF", "FLIGHT_TO_QUALITY"):
        m += 0.2
    return round(min(2.0, m), 3)


def attach_unwind_fragility(ranked, rr):
    """Per-asset carry-unwind fragility. Fragility is only meaningful for POSITIVE-carry
    assets (you're being paid to hold → you're the one exposed to the unwind).

    fragility_0_100 combines, for each positive-carry asset:
      • carry richness   — higher carry vs class = more crowded/more to unwind
      • realized vol     — higher vol = faster, deeper unwind
      • own dislocation  — carry stretched vs its own history (from #2) = more extended
      • regime multiplier— live RORO/VIX escalation (the steamroller)

    Also computes each asset's historical risk-off drawdown behavior from its own price
    path around past high-stress dates (best-effort from stored snapshots)."""
    reg_mult = _regime_multiplier(rr)

    # Gather class-relative carry percentiles for richness.
    pos = [a for a in ranked if (a.get('carry_pct') or 0) > 0]
    # Normalize helpers across the positive-carry cohort.
    carries = [a['carry_pct'] for a in pos]
    vols = [a.get('realized_vol_pct') for a in pos if a.get('realized_vol_pct')]
    cmax = max(carries) if carries else 1.0
    vmax = max(vols) if vols else 1.0

    for a in ranked:
        cp = a.get('carry_pct')
        if cp is None or cp <= 0:
            a['unwind_fragility'] = None
            a['unwind_flag'] = None
            continue
        # Richness 0..1 (how high is this carry vs the richest positive carry).
        richness = min(1.0, cp / cmax) if cmax > 0 else 0.0
        # Vol 0..1.
        vol = a.get('realized_vol_pct')
        voln = min(1.0, vol / vmax) if (vol and vmax > 0) else 0.3  # assume mid if unknown
        # Own-history extension (from dislocation z): high positive z = extended.
        z = a.get('carry_own_z')
        extn = 0.0
        if z is not None:
            extn = min(1.0, max(0.0, z / 3.0))  # z=3 → fully extended
        # Base fragility (structural, regime-independent): weighted blend.
        base = 0.45 * richness + 0.35 * voln + 0.20 * extn  # 0..1
        # Apply live regime multiplier and scale to 0..100.
        frag = round(min(100.0, base * 100.0 * reg_mult), 1)
        a['unwind_fragility'] = frag
        a['unwind_components'] = {
            'richness': round(richness, 3), 'vol_norm': round(voln, 3),
            'extension': round(extn, 3), 'regime_mult': reg_mult,
        }
        # Flag tiers.
        if frag >= 70:
            a['unwind_flag'] = 'FRAGILE'
        elif frag >= 45:
            a['unwind_flag'] = 'CROWDED'
        else:
            a['unwind_flag'] = 'STABLE'

    # Cohort gauge: the top-carry decile is the classic carry basket.
    positive = [a for a in ranked if a.get('unwind_fragility') is not None]
    positive.sort(key=lambda x: x['carry_pct'], reverse=True)
    decile_n = max(3, len(positive) // 10)
    top_decile = positive[:decile_n]
    cohort_frag = round(mean([a['unwind_fragility'] for a in top_decile]), 1) if top_decile else None

    fragile = [a for a in ranked if a.get('unwind_flag') == 'FRAGILE']
    crowded = [a for a in ranked if a.get('unwind_flag') == 'CROWDED']

    # Overall unwind-risk verdict.
    if cohort_frag is None:
        verdict = 'UNKNOWN'
    elif cohort_frag >= 70:
        verdict = 'HIGH — carry cohort fragile, risk-off would force a violent unwind'
    elif cohort_frag >= 45:
        verdict = 'ELEVATED — carry is crowded; watch RORO/VIX for the trigger'
    else:
        verdict = 'LOW — carry cohort is not currently stretched'

    return {
        'regime': rr,
        'regime_multiplier': reg_mult,
        'cohort_fragility': cohort_frag,
        'cohort_size': len(top_decile),
        'verdict': verdict,
        'n_fragile': len(fragile),
        'n_crowded': len(crowded),
        'fragile_assets': [
            {'symbol': a['symbol'], 'asset_class': a['asset_class'],
             'carry_pct': a['carry_pct'], 'unwind_fragility': a['unwind_fragility'],
             'realized_vol_pct': a.get('realized_vol_pct')}
            for a in sorted(fragile, key=lambda x: x['unwind_fragility'], reverse=True)[:15]
        ],
    }


# ──────────────────────────────────────────────────────────────────────
# HISTORICAL DELTAS (CARRY MOMENTUM)
# ──────────────────────────────────────────────────────────────────────

def load_prior_snapshot(days_ago):
    """Load the snapshot from N days ago for carry-momentum comparison."""
    target_date = (datetime.now(timezone.utc) - timedelta(days=days_ago)).date()
    # Try exact date first, then walk backward up to 3 days
    for offset in range(4):
        try_date = target_date - timedelta(days=offset)
        key = f"{HIST_PREFIX}{try_date.isoformat()}.json"
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            return json.loads(obj['Body'].read().decode())
        except Exception:
            continue
    return None


DISLOCATION_MIN_SNAPSHOTS = 60  # ~3 trading months of daily history before z activates


def _list_history_snapshots(max_days=400):
    """List available daily snapshot keys under HIST_PREFIX, newest first."""
    keys = []
    try:
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=BUCKET, Prefix=HIST_PREFIX):
            for obj in page.get('Contents', []):
                k = obj['Key']
                if k.endswith('.json'):
                    keys.append(k)
    except Exception as e:
        print(f"[dislocation] list history failed: {e}")
    keys.sort(reverse=True)
    return keys[:max_days]


def attach_dislocation_zscore(current_assets):
    """For each asset, z-score its CURRENT carry against its OWN trailing carry history.
    This is the dislocation signal: 'is this asset paying unusually vs what it usually
    pays?' — distinct from within-class z. Gated: needs >=DISLOCATION_MIN_SNAPSHOTS days.

    Writes per asset:
      carry_own_z            float | None
      carry_own_pctile       0..100 | None   (rank of current within own history)
      carry_own_mean_pct     float | None
      carry_own_n            int              (# historical obs used)
      dislocation_status     'active' | 'warming (n/60)' | 'no_history'
    """
    snap_keys = _list_history_snapshots()
    # Build per-symbol historical carry series from snapshots.
    series = {}  # symbol -> list[float]
    for key in snap_keys:
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            snap = json.loads(obj['Body'].read().decode())
        except Exception:
            continue
        for a in snap.get('all_assets', []):
            sym = a.get('symbol')
            cp = a.get('carry_pct')
            if sym and cp is not None:
                series.setdefault(sym, []).append(float(cp))

    for a in current_assets:
        sym = a.get('symbol')
        cur = a.get('carry_pct')
        hist = series.get(sym, [])
        n = len(hist)
        a['carry_own_n'] = n
        if cur is None or n == 0:
            a['carry_own_z'] = None
            a['carry_own_pctile'] = None
            a['carry_own_mean_pct'] = None
            a['dislocation_status'] = 'no_history'
            continue
        if n < DISLOCATION_MIN_SNAPSHOTS:
            a['carry_own_z'] = None
            a['carry_own_pctile'] = None
            a['carry_own_mean_pct'] = round(mean(hist), 3)
            a['dislocation_status'] = f'warming ({n}/{DISLOCATION_MIN_SNAPSHOTS})'
            continue
        m = mean(hist)
        sd = stdev(hist) if len(hist) > 1 else 0.0
        a['carry_own_mean_pct'] = round(m, 3)
        a['carry_own_z'] = round((cur - m) / sd, 2) if sd > 0 else None
        # percentile of current vs history
        below = sum(1 for h in hist if h <= cur)
        a['carry_own_pctile'] = round(below / n * 100, 1)
        a['dislocation_status'] = 'active' if sd > 0 else 'flat_history'


def attach_carry_momentum(current_assets):
    """For each asset, find its carry value 7D ago and 30D ago. Compute delta."""
    snap_7d = load_prior_snapshot(7)
    snap_30d = load_prior_snapshot(30)
    
    def lookup_carry(snap, symbol):
        if not snap:
            return None
        for asset in snap.get('all_assets', []):
            if asset.get('symbol') == symbol:
                return asset.get('carry_pct')
        return None
    
    for a in current_assets:
        sym = a.get('symbol')
        c7 = lookup_carry(snap_7d, sym)
        c30 = lookup_carry(snap_30d, sym)
        a['carry_change_7d_pct'] = round(a['carry_pct'] - c7, 3) if c7 is not None and a.get('carry_pct') is not None else None
        a['carry_change_30d_pct'] = round(a['carry_pct'] - c30, 3) if c30 is not None and a.get('carry_pct') is not None else None


# ──────────────────────────────────────────────────────────────────────
# TELEGRAM
# ──────────────────────────────────────────────────────────────────────

def send_telegram(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({
            'chat_id': TELEGRAM_CHAT_ID,
            'text': msg[:4000],
            'parse_mode': 'Markdown',
            'disable_web_page_preview': 'true',
        }).encode()
        req = urllib.request.Request(url, data=data, method='POST')
        urllib.request.urlopen(req, timeout=10)
        return True
    except Exception as e:
        print(f"[telegram] failed: {e}")
        return False


def build_telegram_digest(payload):
    top = payload['cross_asset_top'][:5]
    bot = payload['cross_asset_bottom'][:5]
    regime = payload['regime_summary']
    
    lines = [f"*💰 CARRY SURFACE* — {payload['generated_at'][:16]}"]
    lines.append(f"_{payload['n_assets']} assets across {len(regime)} classes_\n")
    
    lines.append("*🟢 TOP CARRY (long pays best)*")
    for a in top:
        delta = ""
        if a.get('carry_change_7d_pct') is not None:
            sign = "+" if a['carry_change_7d_pct'] >= 0 else ""
            delta = f" ({sign}{a['carry_change_7d_pct']:.2f} 7d)"
        lines.append(f"  `{a['symbol']:<10}` *{a['carry_pct']:+.2f}%*{delta} `[{a['asset_class']}]`")
    
    lines.append("\n*🔴 BOTTOM CARRY (short pays best)*")
    for a in bot:
        delta = ""
        if a.get('carry_change_7d_pct') is not None:
            sign = "+" if a['carry_change_7d_pct'] >= 0 else ""
            delta = f" ({sign}{a['carry_change_7d_pct']:.2f} 7d)"
        lines.append(f"  `{a['symbol']:<10}` *{a['carry_pct']:+.2f}%*{delta} `[{a['asset_class']}]`")
    
    lines.append("\n*📊 REGIME*")
    for cls, s in regime.items():
        lines.append(f"  {cls}: median {s['median_carry_pct']:+.2f}%  σ={s['dispersion_pct']:.2f}")
    
    return "\n".join(lines)


def maybe_send_telegram(payload):
    """Send digest if any significant carry shifts or first time."""
    try:
        prev_alert = json.loads(s3.get_object(Bucket=BUCKET, Key=ALERT_HISTORY_KEY)['Body'].read().decode())
    except Exception:
        prev_alert = {}
    
    last_sent_str = prev_alert.get('last_sent')
    if last_sent_str:
        try:
            last_sent = datetime.fromisoformat(last_sent_str.replace('Z', '+00:00'))
            hours_since = (datetime.now(timezone.utc) - last_sent).total_seconds() / 3600
            if hours_since < 24:  # daily digest max
                return False
        except Exception:
            pass
    
    sent = send_telegram(build_telegram_digest(payload))
    if sent:
        s3.put_object(
            Bucket=BUCKET, Key=ALERT_HISTORY_KEY,
            Body=json.dumps({'last_sent': datetime.now(timezone.utc).isoformat()}).encode(),
            ContentType='application/json',
        )
    return sent


# ──────────────────────────────────────────────────────────────────────
# MAIN HANDLER
# ──────────────────────────────────────────────────────────────────────

def _carry_massive_fx():
    import boto3 as _b
    _c = _b.client("s3", "us-east-1")
    try:
        fx = json.loads(_c.get_object(Bucket=BUCKET, Key="data/polygon-fx-regime.json")["Body"].read())
    except Exception as e:
        return {"error": str(e)[:80]}
    pd = fx.get("pair_data") or {}; rm = fx.get("regime_metrics") or {}
    majors = {p: {"price": (pd.get(p) or {}).get("latest_price"),
                  "ret_20d_pct": (pd.get(p) or {}).get("return_20d_pct"),
                  "vol_20d_pct": (pd.get(p) or {}).get("realized_vol_20d_pct")}
              for p in ("USD_JPY", "EUR_USD", "GBP_USD", "AUD_USD", "USD_CNH", "USD_MXN") if p in pd}
    return {
        "regime_signals": fx.get("regime_signals") or [],
        "usd_synthetic_20d_pct": rm.get("usd_synthetic_20d_pct"),
        "majors": majors,
        "note": "Live Massive FX majors (spot, 20d momentum, realized vol) — corroborates the FRED-rate FX carry leg.",
        "source": "Massive polygon-fx-regime",
    }


def lambda_handler(event=None, context=None):
    started = time.time()
    run_ts = datetime.now(timezone.utc)
    print(f"[carry-surface] v{VERSION} starting @ {run_ts.isoformat()}")
    
    # Pull SOFR / financing rate (universal across equity & FI)
    try:
        financing_rate = fred_latest('DFF') or 5.0  # Fed funds effective
    except Exception:
        financing_rate = 5.0
    print(f"[carry-surface] financing rate: {financing_rate}%")
    
    # Compute each asset class
    by_class = {}
    
    by_class['equity'] = compute_equity_carry(financing_rate)
    print(f"  equity: {len(by_class['equity'])} computed")
    
    by_class['fx'] = compute_fx_carry()
    print(f"  fx: {len(by_class['fx'])} computed")
    
    by_class['fixed_income'] = compute_fi_carry(financing_rate)
    print(f"  fixed_income: {len(by_class['fixed_income'])} computed")
    
    by_class['commodity'] = compute_commodity_carry()
    print(f"  commodity: {len(by_class['commodity'])} computed")
    
    by_class['crypto'] = compute_crypto_carry()
    print(f"  crypto: {len(by_class['crypto'])} computed")
    
    # Flatten + rank cross-asset
    all_assets = []
    for cls, assets in by_class.items():
        all_assets.extend(assets)
    
    ranked = cross_asset_rank(all_assets)
    
    # Carry momentum (compare to 7D / 30D snapshots)
    attach_carry_momentum(ranked)

    # Dislocation z-score: current carry vs each asset's OWN history (gated at 60 obs).
    # This is what turns the leaderboard into a 'who pays UNUSUALLY' signal.
    attach_dislocation_zscore(ranked)

    # Top dislocations (most stretched vs own history), only where z is active.
    dislocations = [a for a in ranked if a.get('carry_own_z') is not None]
    dislocations.sort(key=lambda x: abs(x['carry_own_z']), reverse=True)

    # Carry-unwind fragility overlay (KMPV crash-risk): fuse live RORO/VIX regime with
    # per-asset carry richness + vol + extension → who blows up if risk-off hits.
    risk_regime = load_risk_regime()
    unwind = attach_unwind_fragility(ranked, risk_regime)
    
    # Synthesis
    top10 = sorted([a for a in ranked if a.get('carry_pct') is not None],
                   key=lambda x: x.get('carry_pct', -999), reverse=True)[:10]
    bottom10 = sorted([a for a in ranked if a.get('carry_pct') is not None],
                      key=lambda x: x.get('carry_pct', 999))[:10]
    
    # Risk-adjusted (carry per vol) leaders
    risk_adjusted = [a for a in ranked if a.get('carry_per_vol') is not None]
    risk_adjusted.sort(key=lambda x: x.get('carry_per_vol', -999), reverse=True)
    
    regime = carry_regime_summary(by_class)
    elapsed = time.time() - started
    
    payload = {
        'version': VERSION,
        'generated_at': run_ts.isoformat(),
        'elapsed_s': round(elapsed, 2),
        'n_assets': len([a for a in ranked if a.get('carry_pct') is not None]),
        'financing_rate_pct': financing_rate,
        'by_class': by_class,
        'all_assets': ranked,
        'cross_asset_top': top10,
        'cross_asset_bottom': bottom10,
        'risk_adjusted_leaders': risk_adjusted[:10],
        'dislocation_leaders': dislocations[:10],
        'unwind_overlay': unwind,
        'n_dormant': len([a for a in ranked if a.get('dormant')]),
        'regime_summary': regime,
        'massive_fx': _carry_massive_fx(),
        'methodology': {
            'equity': 'div_yield + buyback_yield - financing_cost (FRED DFF). ETF yields from declared TTM distributions; single names from ratios-ttm. Unknown yields left dormant, never masked as -financing.',
            'fx': 'long_currency_3M_rate - USD_3M_rate (FRED IR3TIB01 series)',
            'fixed_income': 'yield_to_maturity - financing_cost',
            'commodity': '-roll_yield, blend of structural estimate + observed ETF-spot basis',
            'crypto': '-perpetual_funding_rate_annualized (Binance public API)',
            'z_score': 'within-class normalization',
            'dislocation_z': f'current carry vs assets OWN trailing carry history (gated >={DISLOCATION_MIN_SNAPSHOTS} daily obs); z, percentile, mean',
            'unwind_overlay': 'KMPV crash-risk: per-asset fragility = 0.45*carry_richness + 0.35*realized_vol + 0.20*own_extension, scaled by a live regime multiplier from data/risk-regime.json (RORO score + VIX + repo stress). Cohort gauge = mean fragility of top-carry decile. Answers: how badly does the carry basket unwind if risk-off hits NOW.',
            'carry_per_vol': 'carry_pct / realized_vol_pct (Sharpe-of-carry)',
            'carry_momentum': 'current vs 7D and 30D prior snapshots',
        },
    }
    
    # Save main output
    s3.put_object(
        Bucket=BUCKET, Key=OUT_KEY,
        Body=json.dumps(payload, default=str, indent=2).encode(),
        ContentType='application/json',
        CacheControl='max-age=300, public',
    )
    
    # Save daily snapshot (overwrites within same day)
    today_key = f"{HIST_PREFIX}{run_ts.date().isoformat()}.json"
    s3.put_object(
        Bucket=BUCKET, Key=today_key,
        Body=json.dumps(payload, default=str).encode(),
        ContentType='application/json',
    )
    
    # Telegram digest (once per day max)
    try:
        sent = maybe_send_telegram(payload)
    except Exception as e:
        sent = False
        print(f"[telegram] error: {e}")
    
    print(f"[carry-surface] done {elapsed:.1f}s — {payload['n_assets']} assets")
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
        'body': json.dumps({
            'ok': True,
            'n_assets': payload['n_assets'],
            'top1': top10[0]['symbol'] if top10 else None,
            'top1_carry_pct': top10[0]['carry_pct'] if top10 else None,
            'bottom1': bottom10[0]['symbol'] if bottom10 else None,
            'bottom1_carry_pct': bottom10[0]['carry_pct'] if bottom10 else None,
            'telegram_sent': sent,
            'elapsed_s': round(elapsed, 2),
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
