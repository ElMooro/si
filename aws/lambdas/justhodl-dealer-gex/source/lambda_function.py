"""
justhodl-dealer-gex — DEALER GAMMA EXPOSURE & POSITIONING ENGINE
═════════════════════════════════════════════════════════════════════════════
Models market-maker (dealer) options positioning. The dealer's delta hedging
flow IS the intraday market — this Lambda computes it.

DATA SOURCES
────────────
  • Options chains: Yahoo Finance (free, no subscription required)
                     /v7/finance/options/{symbol} endpoint
                     Provides: strike, OI, volume, IV, bid/ask per contract
  • Spot prices: Polygon /v2/aggs/prev (fallback to Yahoo quote)
  • Greeks: computed by us via Black-Scholes (more transparent than vendor)

THE INSTITUTIONAL CONCEPT
─────────────────────────
Market makers are net SHORT options (they sell to retail and hedge with stock).
Their gamma exposure determines hedging behavior:

  POSITIVE GEX (above flip) → dealers BUY dips, SELL rips → mean-reverting tape
                                Low realized vol regime, low VIX, range-bound

  NEGATIVE GEX (below flip) → dealers SELL dips, BUY rips → momentum/explosive
                                High realized vol, gap risk, trend persistence

  ZERO GAMMA "FLIP LEVEL"  → the SPX/SPY price at which sign changes
                                Most-traded level; trades around it = whipsaw

═════════════════════════════════════════════════════════════════════════════
METRICS COMPUTED PER UNDERLYING (SPY, QQQ, IWM, plus top names)
───────────────────────────────────────────────────────────────

  1. GEX by strike & expiry
       GEX = OI × contract_multiplier × spot² × gamma × 0.01 × call_or_put_sign
       Signed: call OI positive (dealers short gamma), put OI negative

  2. Total Dealer GEX (sum across all strikes/expiries)
       In $-per-1%-move terms (institutional standard)

  3. Zero-Gamma Flip Level
       Iterate spot scenarios → find where cumulative GEX crosses zero
       This is the regime-shift price

  4. Major Strike Walls
       Top 5 call walls (resistance — dealers buy as price approaches)
       Top 5 put walls (support — dealers sell as price approaches)

  5. Max Pain (per expiry)
       Strike minimizing total intrinsic value of OI
       Market gravitates toward this at expiry (pinning)

  6. Vanna Exposure
       Vanna = ∂Δ/∂σ ≈ -d₁ × φ(d₁) / σ × √T
       Drives the OPEX vanna squeeze: IV crush forces dealer buying

  7. Charm Exposure
       Charm = ∂Δ/∂t (per day decay of delta)
       Drives pinning toward expiry as gamma collapses

  8. Put/Call Ratio (volume + OI)
       Total volume P/C and total OI P/C
       Extreme readings = sentiment + hedging info

  9. IV Skew (25-delta put IV − 25-delta call IV)
       Higher skew = expensive crash protection = stress in market

  10. 0DTE Concentration
       % of total OI/volume in zero-day-to-expiry contracts
       Currently 45-50% of SPX = dominant intraday driver

═════════════════════════════════════════════════════════════════════════════
TRADING REGIME CLASSIFICATION (output as actionable signal)
───────────────────────────────────────────────────────────

  Spot vs Flip   Total GEX     Regime              Trading Bias
  ───────────────────────────────────────────────────────────────
  > Flip +1%     > +$5B        STRONG POSITIVE     Fade rallies, buy dips, sell vol
  > Flip          > 0          POSITIVE            Mean revert, low vol
  Near Flip      ~0            UNSTABLE            Whipsaw zone, reduce size
  < Flip          < 0          NEGATIVE            Trend follow, buy rips
  < Flip -1%     < -$3B        STRONG NEGATIVE     Momentum/explosive, gap risk

═════════════════════════════════════════════════════════════════════════════
"""
import json
import math
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

import boto3

VERSION = "1.0.0"

S3_BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/dealer-gex.json"
HISTORY_KEY = "data/dealer-gex-history.json"

POLY_KEY = os.environ.get("POLY_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# ─── Underlyings to model ───
# SPY, QQQ, IWM = market-wide regimes (most important)
# Top names = single-stock gamma squeezes
UNDERLYINGS = ["SPY", "QQQ", "IWM", "NVDA", "TSLA", "AAPL", "META", "AMZN", "MSFT", "GOOGL"]

# Calculation parameters
RISK_FREE_RATE = 0.0425  # 10y treasury proxy
EXPIRY_HORIZON_DAYS = 60  # only model expiries within this window
CONTRACT_MULTIPLIER = 100  # standard equity options
HTTP_TIMEOUT = 12

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


# ═══════════════════════════════════════════════════════════════════════════
# BLACK-SCHOLES GREEKS (pure Python — no scipy in Lambda)
# ═══════════════════════════════════════════════════════════════════════════

def norm_pdf(x):
    """Standard normal PDF."""
    return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)

def norm_cdf(x):
    """Standard normal CDF using error function."""
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))

def d1_d2(S, K, T, r, sigma):
    """Black-Scholes d1 and d2."""
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return None, None
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    return d1, d2

def bs_gamma(S, K, T, r, sigma):
    """Black-Scholes gamma. Same for calls and puts."""
    d1, _ = d1_d2(S, K, T, r, sigma)
    if d1 is None: return 0.0
    return norm_pdf(d1) / (S * sigma * math.sqrt(T))

def bs_delta(S, K, T, r, sigma, is_call):
    """Black-Scholes delta."""
    d1, _ = d1_d2(S, K, T, r, sigma)
    if d1 is None: return 0.0
    n_d1 = norm_cdf(d1)
    return n_d1 if is_call else n_d1 - 1.0

def bs_vanna(S, K, T, r, sigma):
    """Vanna = ∂Δ/∂σ. Same magnitude for calls and puts (sign matters for hedging)."""
    d1, d2 = d1_d2(S, K, T, r, sigma)
    if d1 is None: return 0.0
    return -d2 * norm_pdf(d1) / sigma

def bs_charm(S, K, T, r, sigma, is_call):
    """Charm = ∂Δ/∂t (per year, negate for per-day decay)."""
    d1, d2 = d1_d2(S, K, T, r, sigma)
    if d1 is None: return 0.0
    pdf_d1 = norm_pdf(d1)
    common = -pdf_d1 * (2 * r * T - d2 * sigma * math.sqrt(T)) / (2 * T * sigma * math.sqrt(T))
    if is_call:
        return common
    else:
        return common  # symmetric structure; difference absorbed in delta sign


# ═══════════════════════════════════════════════════════════════════════════
# POLYGON OPTIONS DATA
# ═══════════════════════════════════════════════════════════════════════════

def fetch_spot_price(symbol):
    """Latest close from Polygon previous day aggregate, fall back to Yahoo."""
    if POLY_KEY:
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/prev?adjusted=true&apiKey={POLY_KEY}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-GEX/1.0"})
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
                data = json.loads(r.read().decode("utf-8"))
            results = data.get("results") or []
            if results: return float(results[0]["c"])
        except Exception as e:
            print(f"  polygon spot {symbol} err: {str(e)[:80]} — falling back to Yahoo")

    # Yahoo fallback (also where we get the quote.regularMarketPrice from the options chain)
    try:
        url = f"https://query2.finance.yahoo.com/v7/finance/options/{symbol}"
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                                   "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            data = json.loads(r.read().decode("utf-8"))
        result = ((data.get("optionChain") or {}).get("result") or [])
        if result:
            quote = result[0].get("quote") or {}
            price = quote.get("regularMarketPrice") or quote.get("postMarketPrice") or quote.get("preMarketPrice")
            if price: return float(price)
    except Exception as e:
        print(f"  yahoo spot {symbol} err: {str(e)[:80]}")
    return None


def fetch_options_chain_snapshot(underlying):
    """
    Fetch full options chain from Yahoo Finance (free, no auth required).
    Returns list of contract dicts in the schema our analyzer expects.

    Two-step fetch:
      1. /v7/finance/options/{sym} — returns expirations list + first expiry's contracts
      2. /v7/finance/options/{sym}?date={unix} — per-additional-expiry call

    Yahoo provides: strike, openInterest, volume, impliedVolatility, lastPrice,
                     bid, ask, expiration (unix ts), inTheMoney
    Greeks (delta/gamma/vanna/charm) are computed ourselves via Black-Scholes.
    """
    base_url = f"https://query2.finance.yahoo.com/v7/finance/options/{underlying}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
    }

    today = date.today()
    cutoff_unix = int((today + timedelta(days=EXPIRY_HORIZON_DAYS)).strftime("%s") if False
                       else int(time.mktime((today + timedelta(days=EXPIRY_HORIZON_DAYS)).timetuple())))

    # Step 1: initial call
    try:
        req = urllib.request.Request(base_url, headers=headers)
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            data = json.loads(r.read().decode("utf-8"))
    except Exception as e:
        print(f"  yahoo chain {underlying} init err: {str(e)[:120]}")
        return []

    result = ((data.get("optionChain") or {}).get("result") or [])
    if not result:
        return []
    chain_data = result[0]
    expirations = chain_data.get("expirationDates") or []
    expirations_in_horizon = [e for e in expirations if e <= cutoff_unix]

    contracts = []
    # First expiry's data is in the initial response
    options_block = chain_data.get("options") or []
    seen_expiries = set()

    def parse_block(block):
        exp_ts = block.get("expirationDate")
        if exp_ts in seen_expiries:
            return
        seen_expiries.add(exp_ts)
        try:
            exp_str = datetime.fromtimestamp(exp_ts, tz=timezone.utc).date().isoformat()
        except Exception:
            return
        for c in (block.get("calls") or []):
            contracts.append({
                "ticker": c.get("contractSymbol"),
                "type": "call",
                "strike": c.get("strike"),
                "expiry": exp_str,
                "open_interest": c.get("openInterest") or 0,
                "volume": c.get("volume") or 0,
                "iv": c.get("impliedVolatility"),
                "gamma_polygon": None,  # we'll compute ourselves
                "delta_polygon": None,
                "vega_polygon": None,
                "theta_polygon": None,
                "last_quote_bid": c.get("bid"),
                "last_quote_ask": c.get("ask"),
                "last_price": c.get("lastPrice"),
                "in_the_money": c.get("inTheMoney"),
            })
        for p in (block.get("puts") or []):
            contracts.append({
                "ticker": p.get("contractSymbol"),
                "type": "put",
                "strike": p.get("strike"),
                "expiry": exp_str,
                "open_interest": p.get("openInterest") or 0,
                "volume": p.get("volume") or 0,
                "iv": p.get("impliedVolatility"),
                "gamma_polygon": None,
                "delta_polygon": None,
                "vega_polygon": None,
                "theta_polygon": None,
                "last_quote_bid": p.get("bid"),
                "last_quote_ask": p.get("ask"),
                "last_price": p.get("lastPrice"),
                "in_the_money": p.get("inTheMoney"),
            })

    for block in options_block:
        parse_block(block)

    # Step 2: fetch each remaining expiry in horizon (skip the one already loaded)
    for exp_ts in expirations_in_horizon:
        if exp_ts in seen_expiries:
            continue
        url = f"{base_url}?date={exp_ts}"
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
                d = json.loads(r.read().decode("utf-8"))
            for block in (((d.get("optionChain") or {}).get("result") or [{}])[0].get("options") or []):
                parse_block(block)
        except Exception as e:
            print(f"  yahoo {underlying} exp {exp_ts} err: {str(e)[:80]}")
            continue

    return contracts


# ═══════════════════════════════════════════════════════════════════════════
# GEX CALCULATION
# ═══════════════════════════════════════════════════════════════════════════

def calculate_gex_per_contract(contract, spot, today):
    """
    Compute dealer gamma exposure for one contract.
    Sign convention: dealer is short calls (sold to retail) → calls have negative dealer gamma
                     dealer is long puts (sold to retail, equivalent of short put) → also short gamma
    But for VISUAL/regime purposes: convention is to SUM all GEX as if dealer is short everything,
    which means: GEX positive when calls dominate (dealer needs to BUY underlying as price rises =
    suppresses volatility = the "positive gamma" stabilizing regime).

    Standard institutional convention (SpotGamma, Tier1Alpha):
      call_GEX = +OI × 100 × spot² × γ × 0.01
      put_GEX  = -OI × 100 × spot² × γ × 0.01
      Total Dealer GEX = sum

    Returns: (gex_per_1pct, gamma_per_oi, vanna_value, charm_value)
    """
    K = contract.get("strike")
    expiry_str = contract.get("expiry")
    oi = contract.get("open_interest") or 0
    iv = contract.get("iv")
    gamma_poly = contract.get("gamma_polygon")
    if not K or not expiry_str or oi <= 0 or not iv or iv <= 0:
        return 0.0, 0.0, 0.0, 0.0

    try:
        exp_date = date.fromisoformat(expiry_str)
    except Exception:
        return 0.0, 0.0, 0.0, 0.0
    days_to_exp = max(1, (exp_date - today).days)
    T = days_to_exp / 365.0

    # Use Polygon gamma if available, else compute Black-Scholes
    if gamma_poly is not None and gamma_poly > 0:
        gamma = gamma_poly
    else:
        gamma = bs_gamma(spot, K, T, RISK_FREE_RATE, iv)

    is_call = contract.get("type") == "call"
    sign = 1 if is_call else -1

    # GEX per 1% move in spot (institutional convention)
    gex_per_1pct = sign * oi * CONTRACT_MULTIPLIER * (spot ** 2) * gamma * 0.01

    vanna = bs_vanna(spot, K, T, RISK_FREE_RATE, iv)
    vanna_value = sign * oi * CONTRACT_MULTIPLIER * vanna

    charm = bs_charm(spot, K, T, RISK_FREE_RATE, iv, is_call)
    charm_value = sign * oi * CONTRACT_MULTIPLIER * charm / 365.0  # per-day

    return gex_per_1pct, gamma * oi, vanna_value, charm_value


def find_zero_gamma_flip(contracts, base_spot, today):
    """
    Find the price level where total dealer GEX crosses zero.
    Iterate spot scenarios in 0.25% increments from -5% to +5%.
    """
    best_flip = None
    prev_gex = None
    for pct in range(-500, 501, 25):  # -5% to +5% in 0.25% steps
        test_spot = base_spot * (1 + pct / 10000)
        total_gex = 0
        for c in contracts:
            g, _, _, _ = calculate_gex_per_contract(c, test_spot, today)
            total_gex += g
        if prev_gex is not None and (prev_gex * total_gex < 0):
            # Sign change — interpolate
            try:
                ratio = prev_gex / (prev_gex - total_gex)
                prev_spot = base_spot * (1 + (pct - 25) / 10000)
                best_flip = prev_spot + ratio * (test_spot - prev_spot)
                break
            except Exception:
                best_flip = test_spot
                break
        prev_gex = total_gex
    return best_flip


def calculate_max_pain(contracts, expiry):
    """
    Max pain = strike that minimizes total dollar value of in-the-money OI.
    Convention: writers of options (dealers) collect premium; pain is to OI holders.
    """
    by_strike = defaultdict(lambda: {"call_oi": 0, "put_oi": 0})
    for c in contracts:
        if c.get("expiry") != expiry: continue
        K = c.get("strike")
        if not K: continue
        oi = c.get("open_interest") or 0
        if c.get("type") == "call":
            by_strike[K]["call_oi"] += oi
        else:
            by_strike[K]["put_oi"] += oi
    if not by_strike: return None

    strikes = sorted(by_strike.keys())
    best_pain = float("inf")
    best_strike = None
    for trial in strikes:
        total_pain = 0
        for K, d in by_strike.items():
            if trial > K:
                # ITM calls
                total_pain += (trial - K) * d["call_oi"] * CONTRACT_MULTIPLIER
            if trial < K:
                # ITM puts
                total_pain += (K - trial) * d["put_oi"] * CONTRACT_MULTIPLIER
        if total_pain < best_pain:
            best_pain = total_pain
            best_strike = trial
    return best_strike


def calculate_iv_skew(contracts, spot):
    """
    25-delta put IV minus 25-delta call IV (institutional skew measure).
    Higher = expensive crash protection = market stress.
    """
    near_term = [c for c in contracts if c.get("expiry") and c.get("delta_polygon")
                  and c.get("iv")]
    if not near_term: return None
    # Find expiry closest to 30 days
    today = date.today()
    def days_to(c):
        try: return abs((date.fromisoformat(c["expiry"]) - today).days - 30)
        except Exception: return 999
    near_term.sort(key=days_to)
    if not near_term: return None
    target_exp = near_term[0].get("expiry")
    target_chain = [c for c in contracts if c.get("expiry") == target_exp]

    # 25-delta put: delta closest to -0.25
    puts = [c for c in target_chain if c.get("type") == "put" and c.get("delta_polygon") is not None]
    calls = [c for c in target_chain if c.get("type") == "call" and c.get("delta_polygon") is not None]
    if not puts or not calls: return None
    p25 = min(puts, key=lambda c: abs(c["delta_polygon"] + 0.25))
    c25 = min(calls, key=lambda c: abs(c["delta_polygon"] - 0.25))
    if not p25.get("iv") or not c25.get("iv"): return None
    return {
        "expiry": target_exp,
        "p25_iv": round(p25["iv"], 4),
        "c25_iv": round(c25["iv"], 4),
        "skew": round(p25["iv"] - c25["iv"], 4),
        "skew_pct": round((p25["iv"] / c25["iv"] - 1) * 100, 1) if c25["iv"] else None,
    }


# ═══════════════════════════════════════════════════════════════════════════
# PER-UNDERLYING ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════

def analyze_underlying(symbol):
    print(f"  analyzing {symbol}...")
    spot = fetch_spot_price(symbol)
    if not spot:
        return {"symbol": symbol, "err": "spot price unavailable"}

    contracts = fetch_options_chain_snapshot(symbol)
    if not contracts:
        return {"symbol": symbol, "spot": spot, "err": "no options chain"}

    today = date.today()
    # Filter to contracts with OI > 0 and expiry within horizon
    contracts = [c for c in contracts
                  if c.get("open_interest", 0) > 0
                  and c.get("expiry")
                  and c.get("iv")]
    contracts_in_horizon = []
    for c in contracts:
        try:
            exp = date.fromisoformat(c["expiry"])
            if 0 <= (exp - today).days <= EXPIRY_HORIZON_DAYS:
                contracts_in_horizon.append(c)
        except Exception: continue

    if not contracts_in_horizon:
        return {"symbol": symbol, "spot": spot, "err": "no contracts in horizon"}

    n_contracts = len(contracts_in_horizon)
    print(f"    {n_contracts} contracts in {EXPIRY_HORIZON_DAYS}d horizon")

    # ─── Aggregate GEX, vanna, charm ───
    total_gex = 0.0
    total_vanna = 0.0
    total_charm = 0.0
    by_strike_gex = defaultdict(float)
    by_strike_oi = defaultdict(lambda: {"call_oi": 0, "put_oi": 0, "call_vol": 0, "put_vol": 0})
    by_expiry = defaultdict(lambda: {"gex": 0, "oi_call": 0, "oi_put": 0, "vol_call": 0, "vol_put": 0})
    total_call_oi = 0
    total_put_oi = 0
    total_call_vol = 0
    total_put_vol = 0
    zero_dte_oi = 0
    zero_dte_vol = 0

    for c in contracts_in_horizon:
        gex, _, vanna, charm = calculate_gex_per_contract(c, spot, today)
        total_gex += gex
        total_vanna += vanna
        total_charm += charm
        K = c["strike"]
        by_strike_gex[K] += gex
        is_call = c.get("type") == "call"
        oi = c.get("open_interest") or 0
        vol = c.get("volume") or 0
        if is_call:
            by_strike_oi[K]["call_oi"] += oi
            by_strike_oi[K]["call_vol"] += vol
            by_expiry[c["expiry"]]["oi_call"] += oi
            by_expiry[c["expiry"]]["vol_call"] += vol
            total_call_oi += oi
            total_call_vol += vol
        else:
            by_strike_oi[K]["put_oi"] += oi
            by_strike_oi[K]["put_vol"] += vol
            by_expiry[c["expiry"]]["oi_put"] += oi
            by_expiry[c["expiry"]]["vol_put"] += vol
            total_put_oi += oi
            total_put_vol += vol
        by_expiry[c["expiry"]]["gex"] += gex
        # 0DTE
        try:
            exp = date.fromisoformat(c["expiry"])
            if (exp - today).days == 0:
                zero_dte_oi += oi
                zero_dte_vol += vol
        except Exception: pass

    # ─── Zero gamma flip ───
    flip = find_zero_gamma_flip(contracts_in_horizon, spot, today)

    # ─── Regime classification ───
    above_flip = (flip is not None and spot > flip)
    pct_to_flip = round((spot / flip - 1) * 100, 2) if flip else None

    # Standardize total GEX in $-billions
    gex_b = total_gex / 1e9
    if gex_b > 5 and above_flip:
        regime = "STRONG_POSITIVE_GAMMA"
        bias = "Fade rallies · buy dips · sell volatility"
    elif gex_b > 0 and above_flip:
        regime = "POSITIVE_GAMMA"
        bias = "Mean-revert · low realized vol · range-bound"
    elif abs(gex_b) < 0.5:
        regime = "NEAR_FLIP"
        bias = "Whipsaw zone · reduce position size · directional unstable"
    elif gex_b < -3:
        regime = "STRONG_NEGATIVE_GAMMA"
        bias = "Momentum/explosive · gap risk · trend persistence"
    else:
        regime = "NEGATIVE_GAMMA"
        bias = "Trend follow · buy rips on confirmation · expect vol expansion"

    # ─── Strike walls (top 5 calls, top 5 puts) ───
    call_walls = sorted(by_strike_oi.items(), key=lambda kv: -kv[1]["call_oi"])[:5]
    put_walls = sorted(by_strike_oi.items(), key=lambda kv: -kv[1]["put_oi"])[:5]

    # ─── Max pain per next 3 expiries ───
    expiries = sorted(set(c["expiry"] for c in contracts_in_horizon))[:4]
    max_pain_by_expiry = {}
    for exp in expiries:
        mp = calculate_max_pain(contracts_in_horizon, exp)
        if mp: max_pain_by_expiry[exp] = mp

    # ─── IV skew ───
    skew = calculate_iv_skew(contracts_in_horizon, spot)

    # ─── 0DTE concentration ───
    zero_dte_pct = {
        "oi_pct": round(100 * zero_dte_oi / (total_call_oi + total_put_oi), 1) if (total_call_oi + total_put_oi) > 0 else 0,
        "vol_pct": round(100 * zero_dte_vol / (total_call_vol + total_put_vol), 1) if (total_call_vol + total_put_vol) > 0 else 0,
        "oi": zero_dte_oi,
        "vol": zero_dte_vol,
    }

    # ─── Put/Call ratios ───
    pcr_oi = round(total_put_oi / total_call_oi, 3) if total_call_oi > 0 else None
    pcr_vol = round(total_put_vol / total_call_vol, 3) if total_call_vol > 0 else None

    # ─── Top-gamma strikes (where the biggest positions sit) ───
    top_gamma_strikes = sorted(by_strike_gex.items(), key=lambda kv: -abs(kv[1]))[:10]

    return {
        "symbol": symbol,
        "spot": round(spot, 2),
        "n_contracts_modeled": n_contracts,
        "total_call_oi": total_call_oi,
        "total_put_oi": total_put_oi,
        "total_call_volume": total_call_vol,
        "total_put_volume": total_put_vol,
        "pcr_oi": pcr_oi,
        "pcr_volume": pcr_vol,
        # Core GEX
        "total_dealer_gex_dollars": round(total_gex, 0),
        "total_dealer_gex_billions": round(gex_b, 3),
        "zero_gamma_flip_level": round(flip, 2) if flip else None,
        "spot_pct_to_flip": pct_to_flip,
        "spot_above_flip": above_flip,
        "regime": regime,
        "trading_bias": bias,
        # Vanna / charm
        "total_vanna_dollars": round(total_vanna, 0),
        "total_charm_dollars_per_day": round(total_charm, 0),
        # Walls
        "call_walls_top5": [{"strike": k, "call_oi": v["call_oi"]} for k, v in call_walls],
        "put_walls_top5": [{"strike": k, "put_oi": v["put_oi"]} for k, v in put_walls],
        # GEX concentration
        "top_gamma_strikes": [{"strike": k, "gex_dollars": round(v, 0),
                                "gex_billions": round(v / 1e9, 3)}
                                for k, v in top_gamma_strikes],
        # Per-expiry breakdown
        "by_expiry": {
            exp: {"gex_billions": round(d["gex"] / 1e9, 3),
                   "call_oi": d["oi_call"], "put_oi": d["oi_put"],
                   "call_vol": d["vol_call"], "put_vol": d["vol_put"]}
            for exp, d in by_expiry.items()
        },
        # Max pain
        "max_pain_by_expiry": max_pain_by_expiry,
        # Skew
        "iv_skew_30d": skew,
        # 0DTE
        "zero_dte": zero_dte_pct,
    }


# ═══════════════════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════════════════

def get_chat_id():
    if TELEGRAM_CHAT_ID: return TELEGRAM_CHAT_ID
    try:
        return ssm.get_parameter(Name="/justhodl/telegram/chat_id",
                                  WithDecryption=True)["Parameter"]["Value"]
    except Exception: return None

def send_telegram(text, chat_id):
    if not TELEGRAM_TOKEN or not chat_id: return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    body = urllib.parse.urlencode({
        "chat_id": chat_id, "text": text[:4000],
        "parse_mode": "Markdown", "disable_web_page_preview": "true",
    }).encode("utf-8")
    try:
        req = urllib.request.Request(url, data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"})
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read().decode("utf-8")).get("ok", False)
    except Exception as e:
        print(f"  telegram err: {str(e)[:200]}")
        return False


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    started = time.time()
    print(f"=== DEALER-GEX v{VERSION} · {datetime.now(timezone.utc).isoformat()} ===")

    # Analyze each underlying in parallel
    results = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(analyze_underlying, sym): sym for sym in UNDERLYINGS}
        for f in as_completed(futures):
            sym = futures[f]
            try:
                results[sym] = f.result()
            except Exception as e:
                print(f"  {sym} err: {str(e)[:200]}")
                results[sym] = {"symbol": sym, "err": str(e)[:200]}

    # ─── Market-wide composite (SPY-centric) ───
    spy = results.get("SPY") or {}
    market_composite = {}
    if spy and not spy.get("err"):
        market_composite = {
            "spy_regime": spy.get("regime"),
            "spy_gex_billions": spy.get("total_dealer_gex_billions"),
            "spy_flip_level": spy.get("zero_gamma_flip_level"),
            "spy_spot": spy.get("spot"),
            "spy_pct_to_flip": spy.get("spot_pct_to_flip"),
            "spy_trading_bias": spy.get("trading_bias"),
            "qqq_regime": (results.get("QQQ") or {}).get("regime"),
            "iwm_regime": (results.get("IWM") or {}).get("regime"),
        }
        # Risk-on/off composite from gamma regimes
        regimes = [r for r in [spy.get("regime"),
                                  (results.get("QQQ") or {}).get("regime"),
                                  (results.get("IWM") or {}).get("regime")] if r]
        positive_count = sum(1 for r in regimes if "POSITIVE" in r)
        negative_count = sum(1 for r in regimes if "NEGATIVE" in r)
        if positive_count == 3:
            market_composite["composite_regime"] = "ALL_POSITIVE_GAMMA"
            market_composite["composite_signal"] = "Low vol, range-bound — sell vol, fade extremes"
        elif negative_count >= 2:
            market_composite["composite_regime"] = "NEGATIVE_GAMMA_DOMINANT"
            market_composite["composite_signal"] = "Volatility expanding — trend follow, reduce size"
        elif positive_count >= 2:
            market_composite["composite_regime"] = "MOSTLY_POSITIVE_GAMMA"
            market_composite["composite_signal"] = "Mean-reverting — buy dips at support, fade resistance"
        else:
            market_composite["composite_regime"] = "MIXED_GAMMA"
            market_composite["composite_signal"] = "Cross-index disagreement — neutral, watch SPY flip"

    # ─── Single-name squeeze candidates ───
    # Stocks with: high P/C ratio, large negative gamma, dealers short
    squeeze_candidates = []
    for sym in UNDERLYINGS[3:]:  # skip SPY/QQQ/IWM
        r = results.get(sym)
        if not r or r.get("err"): continue
        gex_b = r.get("total_dealer_gex_billions") or 0
        pcr = r.get("pcr_oi") or 0
        skew = (r.get("iv_skew_30d") or {}).get("skew", 0) or 0
        # High negative GEX + low P/C ratio = dealers short calls = squeeze setup
        squeeze_score = 0
        if gex_b < -0.05: squeeze_score += 30  # negative gamma
        if pcr and pcr < 0.7: squeeze_score += 25  # call-heavy
        if skew and skew < 0.02: squeeze_score += 20  # flat skew = complacency
        if r.get("zero_dte", {}).get("vol_pct", 0) > 30: squeeze_score += 15  # 0DTE speculation
        if squeeze_score >= 50:
            squeeze_candidates.append({
                "symbol": sym, "score": squeeze_score,
                "gex_billions": gex_b, "pcr_oi": pcr,
                "spot": r.get("spot"),
                "regime": r.get("regime"),
            })
    squeeze_candidates.sort(key=lambda x: -x["score"])

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "version": VERSION,
        "elapsed_seconds": round(time.time() - started, 2),
        "calculation_config": {
            "risk_free_rate": RISK_FREE_RATE,
            "expiry_horizon_days": EXPIRY_HORIZON_DAYS,
            "contract_multiplier": CONTRACT_MULTIPLIER,
            "n_underlyings": len(UNDERLYINGS),
        },
        "market_composite": market_composite,
        "squeeze_candidates": squeeze_candidates,
        "underlyings": results,
    }

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
            Body=json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8"),
            ContentType="application/json", CacheControl="public, max-age=600")
        size_kb = len(json.dumps(payload, default=str)) / 1024
        print(f"  ✓ dealer-gex.json written ({size_kb:.1f} KB)")
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"err": f"put: {e}"})}

    # ─── Append to history (last 30 days, sample 1/hour) ───
    try:
        history = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=HISTORY_KEY)["Body"].read())
    except Exception:
        history = {"history": []}
    history["history"].append({
        "ts": int(time.time()),
        "spy_gex_b": spy.get("total_dealer_gex_billions") if spy else None,
        "spy_flip": spy.get("zero_gamma_flip_level") if spy else None,
        "spy_spot": spy.get("spot") if spy else None,
        "spy_regime": spy.get("regime") if spy else None,
        "qqq_gex_b": (results.get("QQQ") or {}).get("total_dealer_gex_billions"),
        "iwm_gex_b": (results.get("IWM") or {}).get("total_dealer_gex_billions"),
    })
    history["history"] = history["history"][-720:]  # ~30 days hourly
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=HISTORY_KEY,
            Body=json.dumps(history, separators=(",", ":")).encode("utf-8"),
            ContentType="application/json", CacheControl="public, max-age=600")
    except Exception as e:
        print(f"  history err: {e}")

    # ─── Telegram alert on regime shift ───
    alert_sent = False
    if market_composite.get("composite_regime") in ("NEGATIVE_GAMMA_DOMINANT", "ALL_POSITIVE_GAMMA"):
        chat_id = get_chat_id()
        if chat_id:
            lines = [f"📐 *Dealer GEX Regime — {market_composite['composite_regime']}*",
                      f"_{datetime.now(timezone.utc).strftime('%b %d %H:%M UTC')}_\n"]
            for sym in ["SPY", "QQQ", "IWM"]:
                r = results.get(sym, {})
                if r.get("err"): continue
                lines.append(f"*{sym}* {r.get('spot')} → flip {r.get('zero_gamma_flip_level')}")
                lines.append(f"  GEX: {r.get('total_dealer_gex_billions')}B · {r.get('regime')}")
                lines.append(f"  Bias: _{r.get('trading_bias')}_")
            if squeeze_candidates:
                lines.append("\n🎯 *Squeeze candidates:*")
                for sc in squeeze_candidates[:3]:
                    lines.append(f"  • {sc['symbol']} score {sc['score']} · GEX {sc['gex_billions']}B")
            lines.append(f"\n_{market_composite.get('composite_signal','')}_")
            lines.append("\n[GEX Dashboard](https://justhodl.ai/gex/)")
            try: alert_sent = send_telegram("\n".join(lines), chat_id)
            except Exception as e: print(f"  alert err: {e}")

    return {"statusCode": 200, "body": json.dumps({
        "success": True, "version": VERSION,
        "n_underlyings": len([r for r in results.values() if not r.get("err")]),
        "spy_regime": spy.get("regime") if spy else None,
        "spy_gex_b": spy.get("total_dealer_gex_billions") if spy else None,
        "spy_flip": spy.get("zero_gamma_flip_level") if spy else None,
        "composite_regime": market_composite.get("composite_regime"),
        "n_squeeze_candidates": len(squeeze_candidates),
        "alert_sent": alert_sent,
        "elapsed_seconds": round(time.time() - started, 2),
    })}
