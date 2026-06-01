"""
justhodl-pump-positioning
═════════════════════════
Adds the actual TRADE FRAMEWORK to each pump candidate from
convergence-radar. This is what a hedge-fund analyst produces per ticker:

  • ATR(14)-based stop-loss
  • Take-profit ladder (1/2/3x ATR)
  • Vol-targeted position size (% of portfolio per name)
  • Kelly fraction (capped at 0.25)
  • Risk-reward ratio
  • Liquidity tier (S/M/L/XL by ADV)
  • Sector + sector ETF context (RS vs SPY/sector)
  • Days-to-catalyst (next earnings from earnings-tracker)
  • Squeeze score proxy (uses options-flow + momentum tier as available)
  • Macroeconomic regime overlay (from ai-website-synthesis)

INPUT
═════
data/convergence-radar.json   →  pump_candidates[]
data/earnings-tracker.json    →  upcoming_14d[]
data/ai-website-synthesis.json→  global_posture for macro overlay

OUTPUT
══════
data/pump-positioning.json
{
  "schema_version": "1.0",
  "generated_at":   "...",
  "n_candidates":   10,
  "macro_regime":   "RISK_ON | NEUTRAL | DEFENSIVE | EXTREME",
  "candidates": [
    {
      "ticker":              "PLTR",
      "convergence_summary": {...},

      "price_data": {
        "current":            85.30,
        "atr_14":             3.45,
        "atr_pct":            4.04,
        "hv_30":              42.5,
        "volume_avg_20d":     45000000,
        "beta_spy":           1.85
      },

      "trade_framework": {
        "entry_zone":         {"low": 84.20, "high": 86.40},
        "stop_loss":          78.40,           # entry - 2*ATR
        "stop_loss_pct":      -8.1,
        "tp_ladder": [
          {"level": "TP1", "price": 88.75, "rr": 1.0, "size_pct": 33},
          {"level": "TP2", "price": 92.20, "rr": 2.0, "size_pct": 33},
          {"level": "TP3", "price": 95.65, "rr": 3.0, "size_pct": 34}
        ],
        "position_size_pct":  2.3,             # % of portfolio per name
        "kelly_fraction":     0.18,            # raw Kelly, capped at 0.25
        "rr_ratio":           "1:2.0",
        "risk_per_share":     6.90,
        "max_dollar_risk_pct": 0.5             # % of portfolio at risk
      },

      "context": {
        "sector":              "Technology",
        "sector_etf":          "XLK",
        "sector_5d_pct":       2.1,
        "ticker_5d_pct":       8.4,
        "rs_vs_sector_5d":     6.3,           # outperforming sector by 6.3%
        "liquidity_tier":      "L",            # S=small, M=mid, L=large, XL=mega
        "adv_dollars":         3.8e9,

        "days_to_earnings":    14,
        "next_earnings_date":  "2026-06-15",
        "next_earnings_time":  "after_market",

        "macro_regime":        "NEUTRAL",
        "regime_supports_long": true            # does the macro view favor longs?
      },

      "warnings": [
        "Earnings within 14d — IV crush risk on TP1+",
        "Below-avg liquidity for this size",
        ...
      ],

      "actionable_summary": "..."
    },
    ...
  ]
}

SCHEDULE
════════
cron(10 * * * ? *) — hourly at :10. Runs after radar at :00/:30 and
research at :05.
"""
import json
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

import boto3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

S3_BUCKET    = "justhodl-dashboard-live"
RADAR_KEY    = "data/convergence-radar.json"
EARNINGS_KEY = "data/earnings-tracker.json"
SYNTHESIS_KEY = "data/ai-website-synthesis.json"
OUTPUT_KEY   = "data/pump-positioning.json"

FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")

# Portfolio assumptions for sizing (these are PARAMETERS, displayed in UI)
TARGET_VOL_PER_NAME = 0.02   # target 2% portfolio vol per single name
KELLY_CAP           = 0.25   # never size above 25% Kelly
MAX_POSITION_PCT    = 5.0    # cap any single name at 5% of portfolio
MIN_POSITION_PCT    = 0.5    # min meaningful position
ATR_STOP_MULTIPLIER = 2.0    # stop = entry - 2*ATR

# Sector ETF mapping (rough)
SECTOR_ETF_MAP = {
    "Technology":              "XLK",
    "Communication Services":  "XLC",
    "Consumer Cyclical":       "XLY",
    "Consumer Defensive":      "XLP",
    "Energy":                  "XLE",
    "Financial Services":      "XLF",
    "Healthcare":              "XLV",
    "Industrials":             "XLI",
    "Basic Materials":         "XLB",
    "Real Estate":             "XLRE",
    "Utilities":               "XLU",
}

s3 = boto3.client("s3", region_name="us-east-1")


# ═════════════════════════════════════════════════════════════════════
# FMP price-history fetch + ATR calculation
# ═════════════════════════════════════════════════════════════════════

def fetch_price_history(ticker: str, days: int = 90) -> Optional[List[dict]]:
    """Pull daily OHLC for the ticker from FMP /stable endpoint."""
    try:
        end = datetime.now(timezone.utc).date()
        start = end - timedelta(days=days)
        url = (f"https://financialmodelingprep.com/stable/historical-price-eod/full"
                f"?symbol={ticker}&from={start.isoformat()}&to={end.isoformat()}&apikey={FMP_KEY}")
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl/positioning"})
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read())
        # FMP /stable returns a list of OHLC rows directly
        rows = data if isinstance(data, list) else data.get("historical", [])
        if not rows:
            return None
        # Sort ascending by date (FMP returns desc)
        rows = sorted(rows, key=lambda x: x.get("date", ""))
        return rows
    except Exception as e:
        print(f"[fmp] {ticker} err: {str(e)[:120]}")
        return None


def fetch_quote(ticker: str) -> Optional[dict]:
    """Real-time quote with sector + market cap."""
    try:
        url = f"https://financialmodelingprep.com/stable/quote?symbol={ticker}&apikey={FMP_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl/positioning"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
        if isinstance(data, list) and data:
            return data[0]
        return None
    except Exception as e:
        print(f"[fmp-quote] {ticker} err: {str(e)[:120]}")
        return None


def fetch_profile(ticker: str) -> Optional[dict]:
    """Company profile for sector classification + beta."""
    try:
        url = f"https://financialmodelingprep.com/stable/profile?symbol={ticker}&apikey={FMP_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl/positioning"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
        if isinstance(data, list) and data:
            return data[0]
        return None
    except Exception as e:
        print(f"[fmp-profile] {ticker} err: {str(e)[:120]}")
        return None


def compute_atr(rows: List[dict], period: int = 14) -> Optional[float]:
    """Average True Range over the last `period` rows.

    True Range = max(high-low, abs(high-prev_close), abs(low-prev_close))
    """
    if not rows or len(rows) < period + 1:
        return None
    trs = []
    for i in range(1, len(rows)):
        h = rows[i].get("high")
        l = rows[i].get("low")
        pc = rows[i-1].get("close")
        if None in (h, l, pc):
            continue
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    if len(trs) < period:
        return None
    # Use last `period` TRs for ATR
    recent = trs[-period:]
    return sum(recent) / len(recent)


def compute_hv(rows: List[dict], period: int = 30) -> Optional[float]:
    """Historical volatility (annualized %) from close-to-close log returns."""
    import math
    if not rows or len(rows) < period + 1:
        return None
    closes = [r.get("close") for r in rows[-(period+1):] if r.get("close")]
    if len(closes) < 5:
        return None
    rets = []
    for i in range(1, len(closes)):
        if closes[i] > 0 and closes[i-1] > 0:
            rets.append(math.log(closes[i] / closes[i-1]))
    if len(rets) < 5:
        return None
    mean_r = sum(rets) / len(rets)
    var = sum((r - mean_r) ** 2 for r in rets) / max(1, (len(rets) - 1))
    std_daily = math.sqrt(var)
    return round(std_daily * math.sqrt(252) * 100, 2)


def compute_perf(rows: List[dict], days: int) -> Optional[float]:
    """Total return over the last `days` trading days, in %."""
    if not rows or len(rows) < days + 1:
        return None
    end_close = rows[-1].get("close")
    start_close = rows[-(days+1)].get("close")
    if not end_close or not start_close or start_close <= 0:
        return None
    return round((end_close / start_close - 1) * 100, 2)


def compute_volume_avg(rows: List[dict], days: int = 20) -> Optional[float]:
    """Average daily volume over the last `days`."""
    if not rows:
        return None
    vols = [r.get("volume") for r in rows[-days:] if r.get("volume")]
    if not vols:
        return None
    return sum(vols) / len(vols)


# ═════════════════════════════════════════════════════════════════════
# Trade framework computation
# ═════════════════════════════════════════════════════════════════════

def liquidity_tier(adv_dollars: float) -> str:
    """ADV in $ → liquidity classification."""
    if adv_dollars is None:
        return "?"
    if adv_dollars >= 5e9:   return "XL"  # mega-cap (AAPL, MSFT)
    if adv_dollars >= 1e9:   return "L"   # large
    if adv_dollars >= 2e8:   return "M"   # mid
    if adv_dollars >= 5e7:   return "S"   # small (still liquid)
    return "XS"                              # micro (caution)


def compute_trade_framework(
    ticker: str,
    pump_likelihood: float,
    current_price: float,
    atr_14: float,
    hv_30: Optional[float],
    adv_dollars: Optional[float],
    days_to_earnings: Optional[int],
) -> dict:
    """Produce stop, TP ladder, position size, R:R."""
    if not all([current_price, atr_14]) or current_price <= 0 or atr_14 <= 0:
        return {"err": "missing price/ATR"}

    entry = current_price
    # Entry zone: -0.7 to +0.5 of ATR around current (small pullback to slight extension)
    entry_low  = round(entry - 0.7 * atr_14, 2)
    entry_high = round(entry + 0.5 * atr_14, 2)

    # Stop = entry - 2*ATR (default), tighter if earnings nearby (limit IV crush exposure)
    stop_mult = ATR_STOP_MULTIPLIER
    if days_to_earnings is not None and days_to_earnings <= 7:
        stop_mult = 1.5  # tighter stop when earnings is imminent
    stop_price    = round(entry - stop_mult * atr_14, 2)
    stop_loss_pct = round((stop_price / entry - 1) * 100, 2)
    risk_per_share = round(entry - stop_price, 2)

    # TP ladder: 1x, 2x, 3x ATR above entry (R:R = 0.5x, 1.0x, 1.5x stop distance)
    rr_unit = risk_per_share  # one R = risk distance
    tp1_price = round(entry + 1.0 * atr_14, 2)
    tp2_price = round(entry + 2.0 * atr_14, 2)
    tp3_price = round(entry + 3.0 * atr_14, 2)
    tp_ladder = [
        {"level": "TP1", "price": tp1_price,
         "rr": round((tp1_price - entry) / max(rr_unit, 0.01), 2),
         "gain_pct": round((tp1_price / entry - 1) * 100, 2),
         "size_pct": 33},
        {"level": "TP2", "price": tp2_price,
         "rr": round((tp2_price - entry) / max(rr_unit, 0.01), 2),
         "gain_pct": round((tp2_price / entry - 1) * 100, 2),
         "size_pct": 33},
        {"level": "TP3", "price": tp3_price,
         "rr": round((tp3_price - entry) / max(rr_unit, 0.01), 2),
         "gain_pct": round((tp3_price / entry - 1) * 100, 2),
         "size_pct": 34},
    ]
    # Overall R:R using equal-weighted TPs
    avg_tp_gain = sum(t["rr"] for t in tp_ladder) / 3
    rr_ratio_str = f"1:{round(avg_tp_gain, 1)}"

    # Position sizing — vol-targeted
    # size_pct = TARGET_VOL_PER_NAME / (HV/100 * sqrt(holding_period_days/252))
    # Assume holding period = 5 trading days for pump candidates
    import math
    if hv_30 and hv_30 > 0:
        # daily vol from annual HV
        vol_horizon = (hv_30 / 100) * math.sqrt(5 / 252)  # 5-day vol
        size_pct_vol = (TARGET_VOL_PER_NAME / vol_horizon) * 100 if vol_horizon > 0 else 2.0
    else:
        size_pct_vol = 2.0

    # Kelly fraction: pump_likelihood/100 = p(win); avg_tp_gain = win amount in R; 1 = loss in R
    # Kelly = (p*b - q) / b where b = win/loss ratio
    p_win = max(0.1, min(0.9, pump_likelihood / 100))
    b = avg_tp_gain  # avg payoff if win, in R units
    q = 1 - p_win
    kelly = (p_win * b - q) / max(b, 0.01)
    kelly_capped = max(0, min(KELLY_CAP, kelly))
    kelly_pct = kelly_capped * 100  # % of portfolio per Kelly

    # Take the MIN of vol-targeted and Kelly (whichever is more conservative)
    position_size_pct = max(MIN_POSITION_PCT, min(MAX_POSITION_PCT, min(size_pct_vol, kelly_pct)))

    # Max dollar risk = position_size * stop_loss_pct
    max_dollar_risk_pct = round(position_size_pct * abs(stop_loss_pct) / 100, 2)

    return {
        "entry_zone":          {"low": entry_low, "high": entry_high, "current": entry},
        "stop_loss":           stop_price,
        "stop_loss_pct":       stop_loss_pct,
        "stop_multiplier":     stop_mult,
        "tp_ladder":           tp_ladder,
        "position_size_pct":   round(position_size_pct, 2),
        "kelly_fraction":      round(kelly_capped, 3),
        "vol_target_size_pct": round(size_pct_vol, 2),
        "rr_ratio":            rr_ratio_str,
        "avg_tp_rr":           round(avg_tp_gain, 2),
        "risk_per_share":      risk_per_share,
        "max_dollar_risk_pct": max_dollar_risk_pct,
    }


def build_warnings(
    days_to_earnings: Optional[int],
    adv_dollars: Optional[float],
    hv_30: Optional[float],
    pump_likelihood: float,
    macro_regime: str,
    bearish_engines: List[dict],
) -> List[str]:
    """Surface execution-relevant warnings."""
    w = []
    if days_to_earnings is not None:
        if days_to_earnings <= 3:
            w.append(f"⚠ Earnings in {days_to_earnings}d — high IV crush + binary risk")
        elif days_to_earnings <= 7:
            w.append(f"⚠ Earnings in {days_to_earnings}d — tighten stops, watch IV")
        elif days_to_earnings <= 14:
            w.append(f"📅 Earnings in {days_to_earnings}d — TP1 before report ideally")

    if adv_dollars is not None:
        if adv_dollars < 5e7:
            w.append(f"⚠ Low liquidity (ADV ${adv_dollars/1e6:.0f}M) — slippage risk on size")
        elif adv_dollars < 2e8:
            w.append(f"📊 Mid liquidity (ADV ${adv_dollars/1e6:.0f}M)")

    if hv_30 and hv_30 > 80:
        w.append(f"⚠ High historical vol ({hv_30:.0f}%) — size accordingly")

    if pump_likelihood < 55:
        w.append("⚠ Pump score <55 — speculative tier; use smallest size")

    if macro_regime in ("DEFENSIVE", "EXTREME", "RISK_OFF"):
        w.append(f"⚠ Macro regime is {macro_regime} — adverse for longs broadly")

    if bearish_engines and len(bearish_engines) >= 2:
        engines = ", ".join(b.get("engine", "") for b in bearish_engines[:3])
        w.append(f"⚠ Multiple bearish signals present: {engines}")

    return w


# ═════════════════════════════════════════════════════════════════════
# Catalyst proximity from earnings-tracker
# ═════════════════════════════════════════════════════════════════════

def build_catalyst_map() -> Dict[str, dict]:
    """Map ticker → upcoming earnings info from earnings-tracker."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=EARNINGS_KEY)
        d = json.loads(obj["Body"].read())
        upcoming = d.get("upcoming_14d", [])
        out = {}
        today = datetime.now(timezone.utc).date()
        for u in upcoming:
            t = u.get("ticker")
            ed = u.get("earnings_date")
            if not t or not ed:
                continue
            try:
                ed_date = datetime.strptime(ed[:10], "%Y-%m-%d").date()
                days_to = (ed_date - today).days
            except Exception:
                days_to = None
            out[t] = {
                "next_earnings_date": ed,
                "days_to_earnings":   days_to,
                "earnings_time":      u.get("time"),
                "eps_consensus":      u.get("eps_consensus"),
            }
        return out
    except Exception as e:
        print(f"[catalyst] err: {e}")
        return {}


def get_macro_regime() -> str:
    """Pull current macro regime from ai-website-synthesis."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=SYNTHESIS_KEY)
        d = json.loads(obj["Body"].read())
        return (d.get("synthesis") or {}).get("global_posture", "NEUTRAL")
    except Exception:
        return "NEUTRAL"


# ═════════════════════════════════════════════════════════════════════
# Per-ticker enrichment (called in parallel)
# ═════════════════════════════════════════════════════════════════════

def enrich_candidate(cand: dict, catalyst_map: Dict[str, dict],
                       macro_regime: str) -> dict:
    """Fetch price + profile, compute framework, return enriched record."""
    ticker = cand["ticker"]
    pump_likelihood = cand.get("pump_likelihood", 50)

    # Pull price history + profile in parallel
    with ThreadPoolExecutor(max_workers=3) as ex:
        f_rows = ex.submit(fetch_price_history, ticker, 90)
        f_quote = ex.submit(fetch_quote, ticker)
        f_profile = ex.submit(fetch_profile, ticker)
        rows = f_rows.result()
        quote = f_quote.result()
        profile = f_profile.result()

    enriched = {
        **cand,
        "ticker": ticker,
    }

    if not rows or not quote:
        enriched["err"] = "price data unavailable"
        return enriched

    # Price metrics
    current_price = quote.get("price") or rows[-1].get("close")
    atr_14 = compute_atr(rows, 14)
    hv_30  = compute_hv(rows, 30)
    perf_5d  = compute_perf(rows, 5)
    perf_20d = compute_perf(rows, 20)
    perf_60d = compute_perf(rows, 60)
    vol_avg_20 = compute_volume_avg(rows, 20)
    adv_dollars = vol_avg_20 * current_price if vol_avg_20 and current_price else None

    enriched["price_data"] = {
        "current":          current_price,
        "atr_14":           round(atr_14, 2) if atr_14 else None,
        "atr_pct":          round(atr_14 / current_price * 100, 2) if atr_14 and current_price else None,
        "hv_30":            hv_30,
        "perf_5d_pct":      perf_5d,
        "perf_20d_pct":     perf_20d,
        "perf_60d_pct":     perf_60d,
        "volume_avg_20d":   int(vol_avg_20) if vol_avg_20 else None,
        "adv_dollars":      int(adv_dollars) if adv_dollars else None,
        "beta_spy":         profile.get("beta") if profile else None,
    }

    # Catalyst
    cat = catalyst_map.get(ticker, {})
    days_to_earnings = cat.get("days_to_earnings")

    # Sector context
    sector = (profile or {}).get("sector", "Unknown")
    sector_etf = SECTOR_ETF_MAP.get(sector, "")

    enriched["context"] = {
        "sector":               sector,
        "industry":             (profile or {}).get("industry", ""),
        "sector_etf":           sector_etf,
        "market_cap":           (profile or {}).get("marketCap"),
        "liquidity_tier":       liquidity_tier(adv_dollars),
        "adv_dollars":          adv_dollars,
        "days_to_earnings":     days_to_earnings,
        "next_earnings_date":   cat.get("next_earnings_date"),
        "next_earnings_time":   cat.get("earnings_time"),
        "macro_regime":         macro_regime,
        "regime_supports_long": macro_regime in ("RISK_ON", "NEUTRAL"),
    }

    # Trade framework
    if current_price and atr_14:
        framework = compute_trade_framework(
            ticker=ticker, pump_likelihood=pump_likelihood,
            current_price=current_price, atr_14=atr_14,
            hv_30=hv_30, adv_dollars=adv_dollars,
            days_to_earnings=days_to_earnings,
        )
        enriched["trade_framework"] = framework
    else:
        enriched["trade_framework"] = {"err": "missing price or ATR"}

    # Warnings
    enriched["warnings"] = build_warnings(
        days_to_earnings=days_to_earnings,
        adv_dollars=adv_dollars,
        hv_30=hv_30,
        pump_likelihood=pump_likelihood,
        macro_regime=macro_regime,
        bearish_engines=cand.get("bearish_engines", []),
    )

    return enriched


# ═════════════════════════════════════════════════════════════════════
# Portfolio basket construction
# ═════════════════════════════════════════════════════════════════════

def build_portfolio_basket(candidates: List[dict]) -> dict:
    """Construct a sector-aware basket from the enriched candidates.

    Rules:
    - Take top candidates with valid trade_framework
    - Cap max position at MAX_POSITION_PCT
    - Skip if regime is DEFENSIVE/EXTREME
    - Skip if pump_likelihood < 45
    - Sector neutralization: max 30% in any one sector
    - Target total exposure 80-100% (room for cash)
    """
    eligible = [
        c for c in candidates
        if isinstance(c.get("trade_framework"), dict) and "err" not in c["trade_framework"]
        and c.get("pump_likelihood", 0) >= 45
    ]

    # Initial allocations from trade_framework.position_size_pct
    allocations = []
    sector_totals: Dict[str, float] = {}
    total_pct = 0.0

    for c in eligible[:20]:  # consider top 20
        fw = c["trade_framework"]
        ctx = c.get("context") or {}
        sector = ctx.get("sector", "Unknown")
        sec_so_far = sector_totals.get(sector, 0)
        proposed = min(MAX_POSITION_PCT, fw.get("position_size_pct", 1.0))

        # Sector cap: 30%
        if sec_so_far + proposed > 30:
            proposed = max(0, 30 - sec_so_far)
        if proposed < MIN_POSITION_PCT:
            continue
        # Total cap: 100%
        if total_pct + proposed > 100:
            proposed = max(0, 100 - total_pct)
        if proposed < MIN_POSITION_PCT:
            break

        allocations.append({
            "ticker":       c["ticker"],
            "position_pct": round(proposed, 2),
            "pump_score":   c.get("pump_likelihood"),
            "sector":       sector,
            "stop":         fw.get("stop_loss"),
            "tp1":          fw["tp_ladder"][0]["price"] if fw.get("tp_ladder") else None,
            "tp2":          fw["tp_ladder"][1]["price"] if fw.get("tp_ladder") and len(fw["tp_ladder"]) > 1 else None,
            "tp3":          fw["tp_ladder"][2]["price"] if fw.get("tp_ladder") and len(fw["tp_ladder"]) > 2 else None,
            "entry":        (fw.get("entry_zone") or {}).get("current"),
            "rr_ratio":     fw.get("rr_ratio"),
            "horizon":      "1-2w",
        })
        sector_totals[sector] = sec_so_far + proposed
        total_pct += proposed

    # Basket-level metrics
    risk_at_stops = sum(
        a["position_pct"] * 0.1  # rough: avg stop is ~8-10%
        for a in allocations
    )

    return {
        "n_positions":      len(allocations),
        "total_exposure":   round(total_pct, 2),
        "cash_pct":         round(100 - total_pct, 2),
        "max_risk_at_stops_pct": round(risk_at_stops, 2),
        "sector_breakdown": {s: round(v, 2) for s, v in sector_totals.items()},
        "positions":        allocations,
        "construction_rules": {
            "max_per_position":   MAX_POSITION_PCT,
            "min_per_position":   MIN_POSITION_PCT,
            "max_per_sector":     30,
            "min_pump_score":     45,
            "target_vol_per_name": TARGET_VOL_PER_NAME,
            "kelly_cap":          KELLY_CAP,
        },
    }


# ═════════════════════════════════════════════════════════════════════
# Lambda handler
# ═════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    t0 = time.time()
    print(f"[positioning] start {datetime.now(timezone.utc).isoformat()}")

    # Load inputs
    try:
        radar = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=RADAR_KEY)["Body"].read())
    except Exception as e:
        return _write_error(f"Failed to load radar: {e}")
    candidates = (radar.get("pump_candidates") or [])[:12]  # 12 top, enrich all
    if not candidates:
        return _write_error("No pump candidates in radar")

    catalyst_map = build_catalyst_map()
    macro_regime = get_macro_regime()
    print(f"[positioning] {len(candidates)} candidates · macro={macro_regime} · {len(catalyst_map)} earnings dates")

    # Enrich in parallel (each ticker fires 3 FMP calls, so cap concurrency)
    enriched = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(enrich_candidate, c, catalyst_map, macro_regime): c
                    for c in candidates}
        for fut in as_completed(futures, timeout=120):
            try:
                enriched.append(fut.result())
            except Exception as e:
                cand = futures[fut]
                print(f"[enrich] {cand['ticker']} err: {e}")
                enriched.append({**cand, "err": str(e)[:120]})

    # Sort by pump_likelihood
    enriched.sort(key=lambda r: -r.get("pump_likelihood", 0))

    # Build portfolio basket
    basket = build_portfolio_basket(enriched)
    print(f"[positioning] basket: {basket['n_positions']} positions, total exposure {basket['total_exposure']}%")

    # Build output
    output = {
        "schema_version":  "1.0",
        "generated_at":    datetime.now(timezone.utc).isoformat(),
        "elapsed_sec":     round(time.time() - t0, 2),
        "n_candidates":    len(enriched),
        "macro_regime":    macro_regime,
        "candidates":      enriched,
        "portfolio_basket": basket,
        "sizing_assumptions": {
            "target_vol_per_name":  TARGET_VOL_PER_NAME,
            "kelly_cap":            KELLY_CAP,
            "max_position_pct":     MAX_POSITION_PCT,
            "min_position_pct":     MIN_POSITION_PCT,
            "atr_stop_multiplier":  ATR_STOP_MULTIPLIER,
            "horizon_days":         5,
        },
    }

    body = json.dumps(output, indent=2, default=str)
    s3.put_object(
        Bucket=S3_BUCKET, Key=OUTPUT_KEY, Body=body,
        ContentType="application/json", CacheControl="max-age=600",
    )
    archive_key = (f"data/archive/pump-positioning/"
                    f"{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')}.json")
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=archive_key, Body=body,
                        ContentType="application/json")
    except Exception:
        pass

    summary = {
        "status":          "ok",
        "elapsed_sec":     output["elapsed_sec"],
        "n_candidates":    output["n_candidates"],
        "macro_regime":    macro_regime,
        "basket_size":     basket["n_positions"],
        "total_exposure":  basket["total_exposure"],
    }
    print(f"[positioning] done: {summary}")
    return {"statusCode": 200, "body": json.dumps(summary)}


def _write_error(message: str, **extras) -> dict:
    payload = {
        "schema_version": "1.0",
        "generated_at":   datetime.now(timezone.utc).isoformat(),
        "status":         "error",
        "error":          message,
        **extras,
    }
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
                        Body=json.dumps(payload, default=str, indent=2),
                        ContentType="application/json", CacheControl="max-age=300")
    except Exception:
        pass
    print(f"[positioning] ERROR: {message}")
    return {"statusCode": 500, "body": json.dumps({"status": "error", "error": message})}
