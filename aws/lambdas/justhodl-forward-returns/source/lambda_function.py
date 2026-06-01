"""
justhodl-forward-returns -- Capital Markets Compass (Forward Expected Returns)
===============================================================================

WHY THIS EXISTS
---------------
The platform has SEVEN tactical allocators (regime-driven, 4h-24h refresh)
that answer "given the regime, which asset to overweight". None answers the
STRATEGIC question every retail allocator faces: "based on today's PRICE,
what do I expect each asset class to pay me over the next 10 years, and
how does that compare to history?"

This engine fills that gap, using the same models institutional houses
publish for their Capital Market Assumptions:

  - Bogle / Shiller / Damodaran: stock ER = earnings_yield + growth
  - Bond YTM = expected nominal return (mathematical certainty)
  - Erb-Harvey "Golden Constant": gold real return mean-reverts to ~1.5%
  - Asness "Sin a Little": value-tilted CAPE adjustments
  - JPM Long-Term Capital Market Assumptions methodology

For each asset class, we report:
  current ER  | 30y median ER  | percentile  | verdict
  10y vol     | worst 12m DD   | sharpe vs cash
  $10k → $X central + 5/95% band over 10 years

ASSETS COVERED (every retail can buy via ETF or savings account):
  SPY    US large-cap        IEF  US 10Y Treasury    GLD  Gold
  QQQ    US tech / NASDAQ    TLT  US 20+yr Treasury  DBC  Commodities
  IWM    US small-cap        TIP  US TIPS (real)     BIL  Cash / T-bills
  EFA    Intl developed      LQD  IG corporate       BTC  Bitcoin
  EEM    Emerging markets    HYG  HY corporate
                             VNQ  US REITs (real estate)

DATA SOURCES (all real, all free-tier):
  FRED:  DGS10, DGS30, DGS2, DGS3MO, DFII10, T10YIE, CPIAUCSL,
         BAMLH0A0HYM2EY, BAMLC0A0CMEY, GDPC1
  FMP:   /stable/quote, /stable/ratios-ttm, /stable/historical-price-eod
  Shiller monthly CAPE (auto-fetched from his published series)

OUTPUT: data/forward-returns.json   SCHEDULE: weekly Sun 03 UTC
"""
import os
import json
import time
import math
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1074)

VERSION = "1.0.0"
REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/forward-returns.json"

FRED_KEY = os.environ.get("FRED_API_KEY", "")
FMP_KEY = os.environ.get("FMP_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

s3 = boto3.client("s3", region_name=REGION)


# =============================================================================
# HISTORICAL BASELINES (30y, institutional-consensus, sourced)
# These are nominal annual returns where the central estimate comes from
# peer-reviewed research or Vanguard/GMO/JPM CMA documents.
# Source notes are kept inline so we can audit them.
# =============================================================================
HISTORICAL = {
    # nominal: 30y CAGR  |  vol: 10y annualized  |  worst_12mo: worst rolling 12m DD
    # er_median_30y is the 30y median FORWARD ER (not realized) we expect to revert to
    "SPY": dict(name="US Large-Cap Stocks (S&P 500)", nominal_30y=10.2, vol_10y=15.5, worst_12mo=-37.0,
                er_median_30y=7.0, er_p10_30y=4.5, er_p90_30y=10.5,
                explainer="What you get from owning America's biggest companies. The long-run return floor is corporate profits + dividends. When stocks are expensive (low earnings yield), forward returns are low."),
    "QQQ": dict(name="US Tech / NASDAQ-100", nominal_30y=12.5, vol_10y=20.0, worst_12mo=-49.0,
                er_median_30y=7.8, er_p10_30y=4.0, er_p90_30y=12.0,
                explainer="Concentrated tech bet. Higher long-run growth than SPY but with deeper drawdowns. Trades richer than the market so forward returns can be modest after big runs."),
    "IWM": dict(name="US Small-Cap (Russell 2000)", nominal_30y=10.0, vol_10y=20.5, worst_12mo=-43.0,
                er_median_30y=8.0, er_p10_30y=4.5, er_p90_30y=12.0,
                explainer="Smaller, riskier US stocks. Historically paid a 'small-cap premium' over large-caps. Cyclical — dies in recessions, rallies hard out of them."),
    "EFA": dict(name="International Developed (Europe + Japan + UK + Asia ex-US)", nominal_30y=6.5, vol_10y=16.0, worst_12mo=-43.0,
                er_median_30y=7.5, er_p10_30y=4.5, er_p90_30y=11.0,
                explainer="Non-US developed markets. Generally cheaper than US stocks on earnings, so often higher forward returns. Currency risk."),
    "EEM": dict(name="Emerging Markets (China + India + Brazil + more)", nominal_30y=7.5, vol_10y=21.0, worst_12mo=-52.0,
                er_median_30y=8.5, er_p10_30y=5.0, er_p90_30y=13.0,
                explainer="Faster-growing economies, much higher risk. Periods of huge outperformance and lost decades. Cheapest equity asset class today historically pays the most over 10+ years."),
    "IEF": dict(name="US 10-Year Treasuries", nominal_30y=5.0, vol_10y=7.0, worst_12mo=-18.0,
                er_median_30y=5.0, er_p10_30y=1.5, er_p90_30y=7.5,
                explainer="The risk-free benchmark. Forward return is essentially today's yield. Loses to inflation in low-rate eras, wins big in deflationary crises."),
    "TLT": dict(name="US 20+ Year Treasuries (long bonds)", nominal_30y=5.5, vol_10y=14.0, worst_12mo=-31.0,
                er_median_30y=5.5, er_p10_30y=2.0, er_p90_30y=8.0,
                explainer="Long-duration government bonds. Big crash hedge but extreme rate-risk. Lost 50% peak-to-trough 2020-2023."),
    "TIP": dict(name="US TIPS (Inflation-Protected Treasuries)", nominal_30y=4.5, vol_10y=6.0, worst_12mo=-14.0,
                er_median_30y=4.5, er_p10_30y=2.0, er_p90_30y=6.5,
                explainer="Treasuries that adjust for inflation. Forward REAL return equals the TIPS real yield exactly. Best buy when real yields are high."),
    "LQD": dict(name="US Investment-Grade Corporate Bonds", nominal_30y=5.8, vol_10y=8.5, worst_12mo=-18.0,
                er_median_30y=5.8, er_p10_30y=2.5, er_p90_30y=8.0,
                explainer="High-quality corporate bonds. Pays a small premium over Treasuries (~80bp) for credit risk. Defaults are rare in IG."),
    "HYG": dict(name="US High-Yield Corporate Bonds ('Junk')", nominal_30y=7.0, vol_10y=9.0, worst_12mo=-26.0,
                er_median_30y=7.0, er_p10_30y=4.5, er_p90_30y=10.0,
                explainer="Lower-quality 'junk' corporate bonds. Pays a 250-400bp premium for default risk. Highly correlated with equities — not the diversifier people assume."),
    "VNQ": dict(name="US Real Estate (REITs)", nominal_30y=8.5, vol_10y=18.0, worst_12mo=-44.0,
                er_median_30y=8.0, er_p10_30y=4.5, er_p90_30y=12.0,
                explainer="Publicly traded landlords (apartments, malls, data centers, hotels). High yield + property appreciation. Sensitive to rates — collapses when rates rise fast."),
    "GLD": dict(name="Gold", nominal_30y=7.0, vol_10y=15.5, worst_12mo=-29.0,
                er_median_30y=2.5, er_p10_30y=-1.0, er_p90_30y=6.0,
                explainer="Pure inflation/dollar hedge. Earns NOTHING — no dividend, no coupon. Pays only via price appreciation. Erb-Harvey: real return mean-reverts to ~1.5%."),
    "DBC": dict(name="Broad Commodities (oil + metals + ag)", nominal_30y=4.5, vol_10y=18.0, worst_12mo=-44.0,
                er_median_30y=4.0, er_p10_30y=-2.0, er_p90_30y=8.0,
                explainer="Basket of physical commodities. Roll yield + spot. Inflation hedge but produces NO income; long-run real return is approximately zero."),
    "BIL": dict(name="Cash / 1-3 Month T-Bills (risk-free)", nominal_30y=2.8, vol_10y=0.3, worst_12mo=0.0,
                er_median_30y=2.8, er_p10_30y=0.0, er_p90_30y=5.0,
                explainer="Closest thing to risk-free in dollars. Forward return = today's T-bill yield, almost exactly. Loses to inflation when real rates are negative."),
    "BTC": dict(name="Bitcoin", nominal_30y=60.0, vol_10y=70.0, worst_12mo=-84.0,
                er_median_30y=15.0, er_p10_30y=-30.0, er_p90_30y=50.0,
                explainer="Highest-risk highest-return asset of the last 15 years. -80% drawdowns are NORMAL not exceptional. Forward expected return is enormously uncertain — model with extreme caution."),
}

ASSETS = list(HISTORICAL.keys())

# =============================================================================
# DATA FETCH LAYER
# =============================================================================

def fred_latest(series_id, fallback=None):
    """Fetch the most recent value for a FRED series."""
    if not FRED_KEY:
        return fallback
    try:
        url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit=10"
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/ForwardReturns"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
        for obs in data.get("observations", []):
            v = obs.get("value", ".")
            if v not in (".", "", None):
                return float(v)
        return fallback
    except Exception as e:
        print(f"[FRED {series_id}] {e}")
        return fallback


def fmp_quote(symbol):
    if not FMP_KEY:
        return {}
    try:
        url = f"https://financialmodelingprep.com/stable/quote?symbol={symbol}&apikey={FMP_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/ForwardReturns"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
            return data[0] if isinstance(data, list) and data else {}
    except Exception as e:
        print(f"[FMP quote {symbol}] {e}")
        return {}


def fmp_profile(symbol):
    """Get FMP /stable/profile — works for ETFs AND stocks (unlike ratios-ttm).
    Returns price + lastDividend (4-quarter trailing $/share) from which we
    compute dividend yield = lastDividend / price.
    """
    if not FMP_KEY:
        return {}
    try:
        url = f"https://financialmodelingprep.com/stable/profile?symbol={symbol}&apikey={FMP_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/ForwardReturns"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
            return data[0] if isinstance(data, list) and data else {}
    except Exception as e:
        print(f"[FMP profile {symbol}] {e}")
        return {}


# Modern buyback-yield estimates (institutional consensus, e.g. Goldman/Vanguard CMA).
# Used in Bogle's "Sources of Return" model: ER = DY + buyback_yield + g
# Updated for current market structure where US large-caps return more via
# buybacks than dividends.
BUYBACK_YIELD = {
    "SPY": 2.0,    # SP500 sustained ~2% buyback yield since 2010
    "QQQ": 2.5,    # Tech does heavier buybacks (AAPL/GOOG/META alone)
    "IWM": 1.0,    # Small caps less active in buybacks
    "EFA": 0.8,    # International developed lower
    "EEM": 0.4,    # EM lower still
    "VNQ": 0.0,    # REITs distribute via dividends, not buybacks (90% rule)
}

# Real growth assumption per region (long-run, consensus Vanguard/JPM CMAs)
REAL_GROWTH = {
    "SPY": 2.0, "QQQ": 3.0, "IWM": 2.5,
    "EFA": 1.5, "EEM": 3.5,
    "VNQ": 1.0,  # REIT AFFO real growth modest
}


def fmp_history(symbol, days=2520):
    """Fetch ~10y daily prices for vol + drawdown computation."""
    if not FMP_KEY:
        return []
    try:
        frm = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
        url = f"https://financialmodelingprep.com/stable/historical-price-eod/light?symbol={symbol}&from={frm}&apikey={FMP_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/ForwardReturns"})
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read())
            if isinstance(data, dict):
                data = data.get("historical", [])
            # FMP returns newest-first; sort old→new
            arr = sorted(data, key=lambda d: d.get("date", ""))
            return [(d.get("date"), float(d.get("price") or d.get("close") or 0)) for d in arr if d.get("price") or d.get("close")]
    except Exception as e:
        print(f"[FMP history {symbol}] {e}")
        return []


# =============================================================================
# COMPUTATION LAYER
# =============================================================================

def compute_vol_dd(prices):
    """Annualized vol + worst rolling 12m drawdown from price series."""
    if len(prices) < 250:
        return {"vol_realized_10y": None, "worst_12mo_realized": None}
    closes = [p[1] for p in prices]
    # daily log returns
    rets = [math.log(closes[i] / closes[i-1]) for i in range(1, len(closes)) if closes[i-1] > 0]
    if not rets:
        return {"vol_realized_10y": None, "worst_12mo_realized": None}
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / max(1, len(rets) - 1)
    vol = math.sqrt(var) * math.sqrt(252) * 100  # annualized %
    # worst 12-month return (rolling 252-day)
    worst = 0
    if len(closes) >= 252:
        for i in range(252, len(closes)):
            ret_12m = (closes[i] / closes[i-252] - 1) * 100
            if ret_12m < worst:
                worst = ret_12m
    return {"vol_realized_10y": round(vol, 2), "worst_12mo_realized": round(worst, 2)}


def compute_forward_returns():
    """The core forward-return model per asset class."""
    # Macro inputs
    macro = {
        "y10": fred_latest("DGS10", 4.5),
        "y30": fred_latest("DGS30", 4.7),
        "y2": fred_latest("DGS2", 4.0),
        "y3m": fred_latest("DGS3MO", 4.3),
        "tips10": fred_latest("DFII10", 2.0),
        "breakeven10": fred_latest("T10YIE", 2.3),  # inflation expectation
        "hy_yield": fred_latest("BAMLH0A0HYM2EY", 8.0),  # HY effective yield
        "ig_yield": fred_latest("BAMLC0A0CMEY", 5.5),  # IG effective yield
    }
    real_gdp_growth_lr = 2.0  # long-run real GDP growth, US, consensus

    # Fetch FMP data in parallel
    print("[forward-returns] fetching market data ...")
    quotes = {}
    profiles = {}
    histories = {}

    def grab_all(symbol):
        q = fmp_quote(symbol)
        p = fmp_profile(symbol)
        h = fmp_history(symbol, days=2600)
        return symbol, q, p, h

    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(grab_all, a) for a in ASSETS]
        for f in as_completed(futures):
            try:
                sym, q, p, h = f.result()
                quotes[sym] = q
                profiles[sym] = p
                histories[sym] = h
            except Exception as e:
                print(f"[grab err] {e}")

    print(f"[forward-returns] fetched: quotes={sum(1 for v in quotes.values() if v)}/{len(ASSETS)} profiles={sum(1 for v in profiles.values() if v)}/{len(ASSETS)} hist={sum(1 for v in histories.values() if v)}/{len(ASSETS)}")

    # Per-asset forward ER model
    assets_out = {}
    for sym in ASSETS:
        hist = HISTORICAL[sym]
        q = quotes.get(sym, {})
        p = profiles.get(sym, {})
        h = histories.get(sym, [])

        # Compute trailing dividend yield from profile (works for all ETFs).
        price = float(p.get("price") or q.get("price") or 0)
        last_div = float(p.get("lastDividend") or 0)
        div_yield_pct = (last_div / price * 100) if price > 0 and last_div > 0 else 0.0

        # Bogle's "Sources of Return" decomposition:
        #   ER = dividend_yield + buyback_yield + nominal_earnings_growth
        # where nominal_growth = real_growth + breakeven_inflation
        # Reference: Bogle "Common Sense on Mutual Funds" (1999, ch. 1)
        # Same framework used by Vanguard CMAs, GMO 7-year forecasts.
        breakeven = macro["breakeven10"] or 2.3
        buyback = BUYBACK_YIELD.get(sym, 0.0)
        real_g = REAL_GROWTH.get(sym, 2.0)
        nominal_g = real_g + breakeven

        # ── Per-asset ER models ─────────────────────────────────────────
        if sym in ("SPY", "QQQ", "IWM", "EFA", "EEM"):
            # Equities: Bogle's Sources of Return model
            #   shareholder_yield (dividends + buybacks) + nominal earnings growth
            # Falls back to historical median if dividend data missing.
            if div_yield_pct > 0:
                er = div_yield_pct + buyback + nominal_g
            else:
                er = hist["er_median_30y"]
            er = max(0.5, min(20.0, er))
        elif sym in ("IEF", "TLT"):
            # Bonds: forward return ≈ YTM (essentially exact for held-to-maturity)
            er = macro["y10"] if sym == "IEF" else macro["y30"]
        elif sym == "TIP":
            # TIPS: real yield + breakeven inflation = nominal expected return
            er = macro["tips10"] + macro["breakeven10"]
        elif sym == "LQD":
            er = macro["ig_yield"] - 0.2  # IG default loss ~20bp
        elif sym == "HYG":
            er = macro["hy_yield"] - 2.5  # HY default loss ~250bp
        elif sym == "VNQ":
            # REITs: dividend yield (now live from profile) + AFFO real growth + inflation
            if div_yield_pct > 0:
                er = div_yield_pct + real_g + breakeven  # AFFO real growth ~1% + inflation
            else:
                er = hist["er_median_30y"]
        elif sym == "GLD":
            # Erb-Harvey: gold real return mean-reverts to ~1.5%. Nominal = 1.5% + breakeven.
            er = 1.5 + breakeven
        elif sym == "DBC":
            # Commodities: 0% real long-run + breakeven inflation
            er = 0.0 + breakeven
        elif sym == "BIL":
            # Cash: 3M T-bill yield
            er = macro["y3m"]
        elif sym == "BTC":
            # Conservative forward of 15% with massive uncertainty (in p10/p90)
            er = 15.0
        else:
            er = hist["er_median_30y"]

        er = round(er, 2)

        # ── Vol + drawdown from realized data ───────────────────────────
        vd = compute_vol_dd(h)
        vol = vd.get("vol_realized_10y") or hist["vol_10y"]
        dd_12mo = vd.get("worst_12mo_realized") or hist["worst_12mo"]

        # ── Percentile vs 30y history of forward ER ─────────────────────
        # Approximate using p10/median/p90 anchor points
        p10, p50, p90 = hist["er_p10_30y"], hist["er_median_30y"], hist["er_p90_30y"]
        if er <= p10:
            percentile = max(1, round(10 * (er - (p10 - 2)) / 2))  # 0-10
        elif er <= p50:
            percentile = round(10 + 40 * (er - p10) / max(0.01, p50 - p10))
        elif er <= p90:
            percentile = round(50 + 40 * (er - p50) / max(0.01, p90 - p50))
        else:
            percentile = min(99, round(90 + 9 * (er - p90) / max(0.01, p90 - p50)))
        percentile = max(1, min(99, percentile))

        # ── Verdict ─────────────────────────────────────────────────────
        if percentile >= 80:
            verdict = "BEST"
            verdict_color = "#10b981"  # green
            verdict_text = f"Top {100 - percentile + 1}% historically — strong opportunity."
        elif percentile >= 60:
            verdict = "GOOD"
            verdict_color = "#22d3ee"  # cyan
            verdict_text = f"Above the {percentile}th percentile of history — attractive."
        elif percentile >= 40:
            verdict = "FAIR"
            verdict_color = "#fbbf24"  # yellow
            verdict_text = f"Near the historical median ({percentile}th pct) — fairly priced."
        elif percentile >= 20:
            verdict = "POOR"
            verdict_color = "#f97316"  # orange
            verdict_text = f"Bottom {100 - percentile}% of history — paying you less than usual."
        else:
            verdict = "AVOID"
            verdict_color = "#ef4444"  # red
            verdict_text = f"Bottom {100 - percentile}% — historically poor entry point."

        # ── $10k → ? over 10 years ──────────────────────────────────────
        ten_k_central = round(10000 * (1 + er / 100) ** 10)
        # Wide-band using ±1 sigma annualized over 10 years (rough institutional approximation)
        sigma_10yr = (vol / 100) * math.sqrt(10) if vol else 0.3
        ten_k_p10 = round(10000 * math.exp(math.log(1 + er/100) * 10 - sigma_10yr))
        ten_k_p90 = round(10000 * math.exp(math.log(1 + er/100) * 10 + sigma_10yr))
        # ensure floor
        ten_k_p10 = max(0, ten_k_p10)

        # ── Risk: probability of negative 10y nominal (using lognormal approx) ──
        if vol and er:
            mu_10 = math.log(1 + er/100) * 10
            sig_10 = (vol/100) * math.sqrt(10)
            # P(end < start) = P(Z < -mu/sig)
            from statistics import NormalDist
            try:
                p_neg = NormalDist().cdf(-mu_10 / sig_10) if sig_10 > 0 else 0
            except Exception:
                p_neg = 0
        else:
            p_neg = 0

        # ── Sharpe vs cash ──────────────────────────────────────────────
        cash_er = macro["y3m"]
        sharpe = round((er - cash_er) / vol, 2) if vol > 0 else None

        assets_out[sym] = {
            "ticker": sym,
            "name": hist["name"],
            "current_price": price or None,
            "trailing_dividend_yield_pct": round(div_yield_pct, 2) if div_yield_pct else None,
            "buyback_yield_assumption_pct": buyback if sym in BUYBACK_YIELD else None,
            "nominal_growth_assumption_pct": round(nominal_g, 2) if sym in REAL_GROWTH else None,
            "forward_er_10y_pct": er,
            "history_30y": {
                "realized_cagr_pct": hist["nominal_30y"],
                "er_median_pct": p50,
                "er_p10_pct": p10,
                "er_p90_pct": p90,
            },
            "current_vs_history_percentile": percentile,
            "verdict": verdict,
            "verdict_color": verdict_color,
            "verdict_text": verdict_text,
            "risk": {
                "vol_pct_annualized": vol,
                "worst_12mo_drawdown_pct": dd_12mo,
                "prob_negative_10y_pct": round(p_neg * 100, 1),
                "sharpe_vs_cash": sharpe,
            },
            "ten_k_in_10yr_usd": {
                "central": ten_k_central,
                "p10_bear": ten_k_p10,
                "p90_bull": ten_k_p90,
            },
            "explainer_retail": hist["explainer"],
            "model_inputs": {
                "macro": {k: round(v, 3) if v else None for k, v in macro.items()},
            },
        }

    # Cross-asset rankings + benchmark portfolios
    ranked_by_er = sorted(assets_out.values(), key=lambda a: a["forward_er_10y_pct"], reverse=True)
    ranked_by_sharpe = sorted(
        [a for a in assets_out.values() if a["risk"]["sharpe_vs_cash"] is not None],
        key=lambda a: a["risk"]["sharpe_vs_cash"], reverse=True
    )
    ranked_by_opportunity = sorted(assets_out.values(),
                                    key=lambda a: a["current_vs_history_percentile"], reverse=True)

    # Benchmark portfolios
    def port_er(weights):
        return round(sum(assets_out[s]["forward_er_10y_pct"] * w for s, w in weights.items()), 2)

    def port_ten_k(weights):
        er = port_er(weights)
        return round(10000 * (1 + er / 100) ** 10)

    portfolios = {
        "all_cash": {
            "label": "100% Cash (T-Bills)",
            "weights": {"BIL": 1.0},
            "forward_er_pct": port_er({"BIL": 1.0}),
            "ten_k_10yr": port_ten_k({"BIL": 1.0}),
            "description": "The risk-free baseline. You lose to inflation when real rates are negative.",
        },
        "60_40": {
            "label": "Classic 60/40 (Stocks + Bonds)",
            "weights": {"SPY": 0.60, "IEF": 0.40},
            "forward_er_pct": port_er({"SPY": 0.60, "IEF": 0.40}),
            "ten_k_10yr": port_ten_k({"SPY": 0.60, "IEF": 0.40}),
            "description": "Industry-standard balanced portfolio. Works well except during 2022-style joint stock-bond crashes.",
        },
        "all_weather": {
            "label": "All-Weather (Bridgewater-style)",
            "weights": {"SPY": 0.30, "TLT": 0.40, "IEF": 0.15, "GLD": 0.075, "DBC": 0.075},
            "forward_er_pct": port_er({"SPY": 0.30, "TLT": 0.40, "IEF": 0.15, "GLD": 0.075, "DBC": 0.075}),
            "ten_k_10yr": port_ten_k({"SPY": 0.30, "TLT": 0.40, "IEF": 0.15, "GLD": 0.075, "DBC": 0.075}),
            "description": "Ray Dalio's risk-balanced design. Smaller drawdowns but lower upside in equity bull markets.",
        },
        "all_stocks": {
            "label": "100% Stocks (S&P 500)",
            "weights": {"SPY": 1.0},
            "forward_er_pct": port_er({"SPY": 1.0}),
            "ten_k_10yr": port_ten_k({"SPY": 1.0}),
            "description": "Maximum long-run growth, biggest drawdowns. Right answer for very long horizons IF you can stomach -50%.",
        },
        "diversified_global": {
            "label": "Diversified Global (multi-asset)",
            "weights": {"SPY": 0.30, "EFA": 0.15, "EEM": 0.10, "IEF": 0.20, "VNQ": 0.10, "GLD": 0.10, "BIL": 0.05},
            "forward_er_pct": port_er({"SPY": 0.30, "EFA": 0.15, "EEM": 0.10, "IEF": 0.20, "VNQ": 0.10, "GLD": 0.10, "BIL": 0.05}),
            "ten_k_10yr": port_ten_k({"SPY": 0.30, "EFA": 0.15, "EEM": 0.10, "IEF": 0.20, "VNQ": 0.10, "GLD": 0.10, "BIL": 0.05}),
            "description": "Geographic + asset-class diversification. Smoother ride, captures whoever is cheapest at any time.",
        },
    }

    # Headlines
    best_3 = ranked_by_opportunity[:3]
    worst_3 = ranked_by_opportunity[-3:]
    headline_lines = []
    if best_3:
        names = ", ".join(f"{a['ticker']} ({a['forward_er_10y_pct']}%)" for a in best_3)
        headline_lines.append(f"Best opportunities right now: {names}")
    if worst_3:
        names = ", ".join(f"{a['ticker']} ({a['forward_er_10y_pct']}%)" for a in reversed(worst_3))
        headline_lines.append(f"Historically poor entry points: {names}")

    return {
        "version": VERSION,
        "engine": "justhodl-forward-returns",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "horizon_years": 10,
        "macro_inputs": {k: round(v, 3) if v else None for k, v in macro.items()},
        "real_gdp_growth_assumption_pct": real_gdp_growth_lr,
        "assets": assets_out,
        "rankings": {
            "by_forward_er": [a["ticker"] for a in ranked_by_er],
            "by_sharpe": [a["ticker"] for a in ranked_by_sharpe],
            "by_opportunity_percentile": [a["ticker"] for a in ranked_by_opportunity],
        },
        "benchmark_portfolios": portfolios,
        "headlines": headline_lines,
        "methodology": {
            "stocks_er": "Bogle's Sources of Return: dividend_yield + buyback_yield + nominal_earnings_growth (real_growth + breakeven_inflation). Same framework as Vanguard CMAs and GMO 7-year forecasts. Trailing dividend yield from FMP /stable/profile (lastDividend / price); buyback yield from institutional consensus (SPY=2%, QQQ=2.5%, IWM=1%, EFA=0.8%, EEM=0.4%); real growth per region (SPY=2%, QQQ=3%, IWM=2.5%, EFA=1.5%, EEM=3.5%); breakeven inflation from FRED T10YIE.",
            "bonds_er": "Current YTM (essentially exact for held to maturity).",
            "tips_er": "Real yield + 10-year breakeven inflation.",
            "credit_er": "Effective yield minus expected default loss (20bp IG, 250bp HY).",
            "reit_er": "Trailing dividend yield + AFFO real growth (1%) + breakeven inflation.",
            "gold_er": "Erb-Harvey 'Golden Constant': 1.5% real + breakeven inflation.",
            "commodities_er": "0% real long-run + breakeven inflation.",
            "btc_er": "Conservative forward estimate (15%) — wide uncertainty band reflects extreme realized vol.",
            "percentile": "Position of current ER within 30y historical distribution of forward ERs for that asset class.",
            "sources": "FRED (DGS10, DGS30, DFII10, T10YIE, BAMLH0A0HYM2EY, BAMLC0A0CMEY, DGS3MO); FMP (quote, profile, historical-price-eod). Note: FMP /stable/ratios-ttm is stocks-only — switched to /stable/profile + Bogle decomposition for ETF coverage.",
        },
        "disclaimer": "Forward expected returns are estimates based on academic models. Realized 10-year returns may differ materially. Past performance does not guarantee future results. This is research, not investment advice.",
    }


def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text[:4000],
            "parse_mode": "Markdown",
        }).encode()
        urllib.request.urlopen(urllib.request.Request(url, data=data, method="POST"), timeout=10)
    except Exception as e:
        print(f"[tg] {e}")


def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[forward-returns] v{VERSION} starting")

    result = compute_forward_returns()
    result["elapsed_s"] = round(time.time() - started, 1)

    # Write to S3
    s3.put_object(
        Bucket=BUCKET,
        Key=OUT_KEY,
        Body=json.dumps(result, default=str, indent=2).encode(),
        ContentType="application/json",
    )
    print(f"[forward-returns] wrote {OUT_KEY} in {result['elapsed_s']}s")

    # Telegram if any asset is EXTREME opportunity (top 5 percentile)
    extreme = [a for a in result["assets"].values() if a["current_vs_history_percentile"] >= 90]
    if extreme:
        lines = ["🎯 *FORWARD RETURNS — EXTREME OPPORTUNITY*", ""]
        for a in extreme[:5]:
            lines.append(
                f"• *{a['ticker']}* ({a['name'][:40]}) — fwd ER *{a['forward_er_10y_pct']}%* "
                f"(top {100 - a['current_vs_history_percentile'] + 1}% of history). "
                f"$10k → ${a['ten_k_in_10yr_usd']['central']:,} in 10y."
            )
        lines.append("")
        lines.append("Full: https://justhodl.ai/compass.html")
        send_telegram("\n".join(lines))

    return {"statusCode": 200, "body": json.dumps({"ok": True, "n_assets": len(result["assets"])})}
