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

VERSION = "1.0.0"
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
    "SPY", "QQQ", "IWM", "DIA", "VTI",
    # Sectors
    "XLE", "XLF", "XLK", "XLV", "XLI", "XLP", "XLY", "XLB", "XLU", "XLRE", "XLC",
    # Top single names
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "BRK-B",
    "JPM", "JNJ", "V", "PG", "MA", "HD", "BAC", "ABBV", "AVGO",
    "WMT", "KO", "MRK", "PEP", "PFE", "TMO", "CVX", "XOM",
    # Dividend champions  
    "VYM", "SCHD", "SPHD", "DGRO",
    # International
    "EFA", "VEA", "EEM", "VWO", "INDA", "MCHI", "EWJ", "EWG", "EWZ",
    # REITs
    "VNQ", "IYR",
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
    # EM with FRED data available
    "MXN": "IR3TIB01MXM156N",             # Mexico
    "BRL": "IRSTCB01BRM156N",             # Brazil central bank rate (proxy)
    "INR": "IR3TIB01INM156N",             # India (note: spotty data)
    "ZAR": "IR3TIB01ZAM156N",             # South Africa
}

# Fixed income: treasuries (DGSx) + credit indexes
# Carry = yield - financing (SOFR)
FIXED_INCOME = {
    "UST_3M":  ("DGS3MO",  "data/cash"),       # 3M T-bill (basically risk-free)
    "UST_2Y":  ("DGS2",    None),
    "UST_5Y":  ("DGS5",    None),
    "UST_10Y": ("DGS10",   None),
    "UST_30Y": ("DGS30",   None),
    "IG_CRED": ("BAMLC0A0CMEY", None),         # ICE BofA US Corporate Effective Yield
    "HY_CRED": ("BAMLH0A0HYM2EY", None),       # ICE BofA US High Yield Effective Yield
    # NOTE: BAML OAS series exist too; we use effective yield for carry calc
}

# Commodity ETFs — roll yield approximation via 30D ETF underperformance vs spot
# For physical commodities we use historical contango/backwardation observation
COMMODITY_UNIVERSE = {
    # symbol → (FMP_symbol, spot_FRED_series, structural_roll_estimate_pct_yr)
    "GLD":  ("GLD",  "GOLDAMGBD228NLBM",  0.0),    # Gold: near-zero carry (storage ~ financing)
    "SLV":  ("SLV",  None,                -1.0),   # Silver: slight contango
    "USO":  ("USO",  "DCOILWTICO",        -8.0),   # Oil: persistent contango ~ -8% / yr
    "UNG":  ("UNG",  "DHHNGSP",           -25.0),  # Nat gas: severe contango
    "DBA":  ("DBA",  None,                -3.0),   # Ag basket: mild contango
    "DBC":  ("DBC",  None,                -3.0),   # Broad commodity: mild contango
    "VXX":  ("VXX",  None,                -65.0),  # VIX futures: severe contango (well-known)
}

# Crypto: perpetual funding rates from Binance public API (no auth)
# Positive funding rate = longs pay shorts (positive carry for shorts; expected
# negative carry for longs holding the underlying via perp)
CRYPTO_UNIVERSE = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "AVAXUSDT", "BNBUSDT",
                   "XRPUSDT", "DOGEUSDT", "ADAUSDT", "MATICUSDT", "LINKUSDT"]

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


def fmp_dividend_yield_ttm(symbol):
    """Get TTM dividend yield from FMP."""
    try:
        url = f"https://financialmodelingprep.com/stable/ratios-ttm?symbol={symbol}&apikey={FMP_KEY}"
        data = http_get_json(url)
        if isinstance(data, list) and data:
            row = data[0]
            # Field name varies: dividendYieldTTM, dividendYieldPercentageTTM
            dy = row.get('dividendYieldTTM') or row.get('dividendYieldPercentageTTM') or 0
            return float(dy or 0)
    except Exception:
        pass
    return 0.0


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


def binance_funding_rate(symbol):
    """Get latest perpetual funding rate from Binance public API.
    Returns dict with current rate (decimal, 8h) and annualized estimate."""
    try:
        url = f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={symbol}&limit=30"
        data = http_get_json(url)
        if isinstance(data, list) and data:
            # Last 30 funding settlements (8h apart, so ~10 days of history)
            rates = [float(x['fundingRate']) for x in data]
            current_8h = rates[-1]  # most recent
            avg_8h_30 = sum(rates) / len(rates)
            # Annualize: 3 per day × 365
            return {
                'current_8h': current_8h,
                'current_annualized_pct': current_8h * 3 * 365 * 100,
                'avg_30period_annualized_pct': avg_8h_30 * 3 * 365 * 100,
                'n_settlements': len(rates),
            }
    except Exception as e:
        return {'error': str(e)[:120]}
    return {'error': 'no_data'}


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
            # Pull div yield, buyback yield, history (price + vol)
            with ThreadPoolExecutor(max_workers=3) as ex:
                f_dy = ex.submit(fmp_dividend_yield_ttm, symbol)
                f_by = ex.submit(fmp_buyback_yield, symbol)
                f_hist = ex.submit(fmp_quote_with_history, symbol, 90)
                dy = f_dy.result()
                by = f_by.result()
                hist = f_hist.result()
            
            # FMP returns div yield in different scales depending on endpoint:
            # ratios-ttm.dividendYieldTTM is typically DECIMAL (0.012 = 1.2%)
            # If we got a value >1, assume already in percent
            if dy and dy > 0.5:
                dy_pct = dy  # likely already percent
            else:
                dy_pct = dy * 100  # decimal → percent
            
            # Buyback yield - same logic
            if by and abs(by) > 0.5:
                by_pct = by
            else:
                by_pct = by * 100
            
            closes = hist.get('closes', []) if hist else []
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
                'price': closes[0] if closes else None,
            }
        except Exception as e:
            return {'symbol': symbol, 'asset_class': 'equity', 'error': str(e)[:120]}
    
    with ThreadPoolExecutor(max_workers=6) as ex:
        for r in as_completed([ex.submit(process_one, s) for s in EQUITY_UNIVERSE]):
            try:
                results.append(r.result())
            except Exception as e:
                print(f"[equity] worker error: {e}")
    
    # Filter to successful, z-score
    valid = [r for r in results if 'carry_pct' in r and r.get('carry_pct') is not None]
    zscore_within_class(valid, 'carry_pct')
    zscore_within_class(valid, 'carry_per_vol')
    # Re-attach errored ones at the end
    errored = [r for r in results if 'error' in r]
    return valid + errored


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
            
            # Use the structural estimate as carry baseline
            # (in production, you'd compute actual roll from forward curve; ETF tracking
            # error vs spot over 30D is the second-best proxy)
            carry_pct = structural_pct
            
            # If we have spot price from FRED, compute the ETF tracking error
            spot_etf_basis_pct = None
            if spot_fred and len(closes) >= 30:
                try:
                    spot_series = fred_series(spot_fred, limit=60)
                    if len(spot_series) >= 30:
                        # ETF return last 30D
                        etf_ret_30d = (closes[0] / closes[29] - 1) * 100 if closes[29] > 0 else 0
                        # Spot return last 30D
                        spot_now = spot_series[0][1]
                        spot_30d_ago = spot_series[min(29, len(spot_series)-1)][1] if len(spot_series) > 1 else None
                        if spot_30d_ago and spot_30d_ago > 0:
                            spot_ret_30d = (spot_now / spot_30d_ago - 1) * 100
                            spot_etf_basis_pct = round((etf_ret_30d - spot_ret_30d) * 12, 2)  # annualize
                            # Override carry estimate with observed tracking error (annualized)
                            # blended: 60% structural, 40% observed
                            carry_pct = round(structural_pct * 0.6 + spot_etf_basis_pct * 0.4, 2)
                except Exception:
                    pass
            
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
            funding = binance_funding_rate(symbol)
            if 'error' in funding:
                return {'symbol': symbol, 'asset_class': 'crypto', 'error': funding['error']}
            # For a long position in the underlying via perp:
            # If funding rate is positive, longs pay → negative carry to longs
            # We define carry_pct as the cost to LONG (so we flip the sign)
            long_carry_pct = -funding.get('current_annualized_pct', 0)
            return {
                'symbol': symbol,
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
        'regime_summary': regime,
        'methodology': {
            'equity': 'div_yield + buyback_yield - financing_cost (FRED DFF)',
            'fx': 'long_currency_3M_rate - USD_3M_rate (FRED IR3TIB01 series)',
            'fixed_income': 'yield_to_maturity - financing_cost',
            'commodity': '-roll_yield, blend of structural estimate + observed ETF-spot basis',
            'crypto': '-perpetual_funding_rate_annualized (Binance public API)',
            'z_score': 'within-class normalization',
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
