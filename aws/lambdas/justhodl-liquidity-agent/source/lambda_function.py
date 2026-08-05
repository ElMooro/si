"""
justhodl-liquidity-agent
========================
TGA + Fed Liquidity Intelligence Agent for JustHodl.AI

Hedge-fund grade net liquidity computation:
    Net Liquidity = Fed Balance Sheet (WALCL) - TGA (WTREGEN) - RRP (RRPONTSYD)

Tracks 25+ FRED series across:
  - Treasury General Account (TGA)
  - Fed Balance Sheet / SOMA
  - Reverse Repo Facility (RRP)
  - Reserve Balances
  - M2 Money Supply
  - Dollar Index Components
  - Treasury Yield Curve
  - Credit & Funding Markets

Stores results to S3: liquidity-data.json
EventBridge: daily 7:30 AM ET (12:30 UTC)
"""

import json
import os
import math
import boto3
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple

# ─── CONFIG ────────────────────────────────────────────────────────────────
FRED_API_KEY  = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")
S3_BUCKET     = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY        = "liquidity-data.json"
FRED_BASE     = "https://api.stlouisfed.org/fred/series/observations"
REGION        = "us-east-1"

s3 = boto3.client("s3", region_name=REGION)

# ─── FRED SERIES CATALOG ───────────────────────────────────────────────────
# (series_id, label, category, unit, frequency)
FRED_SERIES = [
    # ── Core Liquidity Triad ──────────────────────────────────────────────
    ("WALCL",       "Fed Total Assets",                    "fed_balance_sheet", "B USD", "w"),
    ("WTREGEN",     "Treasury General Account (TGA)",      "tga",               "B USD", "w"),
    ("RRPONTSYD",   "Fed Reverse Repo (RRP)",              "rrp",               "B USD", "d"),

    # ── SOMA / Fed Holdings ───────────────────────────────────────────────
    ("WSHOSHO",     "SOMA Holdings Total",                 "soma",              "B USD", "w"),
    ("WSHOTSL",     "SOMA Treasury Securities",            "soma",              "B USD", "w"),
    ("WSHOMCB",     "SOMA MBS Holdings",                   "soma",              "B USD", "w"),

    # ── Reserve Balances ──────────────────────────────────────────────────
    ("WRESBAL",     "Reserve Balances at Fed",             "reserves",          "B USD", "w"),
    ("TOTRESNS",    "Total Reserves Depository Inst",      "reserves",          "B USD", "m"),
    ("EXCSRESNW",   "Excess Reserves",                     "reserves",          "B USD", "w"),

    # ── Money Supply ──────────────────────────────────────────────────────
    ("M2SL",        "M2 Money Supply",                     "money_supply",      "B USD", "m"),
    ("M1SL",        "M1 Money Supply",                     "money_supply",      "B USD", "m"),
    ("BOGMBASE",    "Monetary Base",                       "money_supply",      "B USD", "w"),

    # ── Dollar & FX ───────────────────────────────────────────────────────
    ("DTWEXBGS",    "Dollar Index Broad (DXY proxy)",      "dollar",            "Index", "d"),
    ("DTWEXAFEGS",  "Dollar vs Advanced Economies",        "dollar",            "Index", "d"),
    ("DTWEXEMEGS",  "Dollar vs Emerging Markets",          "dollar",            "Index", "d"),

    # ── Treasury Yields ───────────────────────────────────────────────────
    ("DGS2",        "2-Year Treasury Yield",               "yields",            "%",     "d"),
    ("DGS10",       "10-Year Treasury Yield",              "yields",            "%",     "d"),
    ("DGS30",       "30-Year Treasury Yield",              "yields",            "%",     "d"),
    ("T10Y2Y",      "10Y-2Y Yield Spread",                 "yields",            "%",     "d"),
    ("T10Y3M",      "10Y-3M Yield Spread",                 "yields",            "%",     "d"),
    ("DFII10",      "10Y TIPS (Real Yield)",                "yields",            "%",     "d"),

    # ── Credit & Funding Markets ─────────────────────────────────────────
    ("SOFR",        "SOFR Rate",                           "funding",           "%",     "d"),
    ("DFF",         "Fed Funds Effective Rate",            "funding",           "%",     "d"),
    ("DPCREDIT",    "Discount Window Primary Credit",      "funding",           "%",     "d"),
    ("WORAL",       "Fed Overnight RRP Awards",            "rrp",               "B USD", "d"),
    # ── ops 4409: Perplexity dimension-4 additions (Khalid: engine+page) ──
    ("TREAST", "US Treasury Securities Held (total)"      "soma", "B USD", "w"),
    ("WSHONBIILB", "SOMA TIPS Holdings"                       "soma", "B USD", "w"),
    ("WSHOBL", "SOMA Treasury Bills"                      "soma", "B USD", "w"),
    ("WSHOFADSL", "SOMA Federal Agency Debt"                 "soma", "B USD", "w"),
    ("WSHOMBLS", "SOMA MBS Holdings (total)"                "soma", "B USD", "w"),
    ("H41RESPPALDKNWA", "Fed Repo Agreements (H.4.1)"              "fed_balance_sheet", "B USD", "w"),
    ("WCBSL", "Central Bank Swap Lines"                  "fed_balance_sheet", "B USD", "w"),
    ("TRESEGUSM052N", "Foreign Official Treasury Holdings"       "fed_balance_sheet", "B USD", "m"),
    ("IORB", "Interest on Reserve Balances"             "funding", "%", "d"),
    ("EFFR", "Effective Fed Funds Rate"                 "funding", "%", "d"),
    ("OBFR", "Overnight Bank Funding Rate"              "funding", "%", "d"),
    ("M2V", "Velocity of M2"                           "money_supply", "ratio", "q"),
    ("CURRCIR", "Currency in Circulation"                  "money_supply", "B USD", "w"),
    ("DEXUSEU", "USD/EUR"                                  "dollar", "rate", "d"),
    ("DEXJPUS", "JPY/USD"                                  "dollar", "rate", "d"),
    ("DEXCHUS", "CNY/USD"                                  "dollar", "rate", "d"),
    ("DEXKOUS", "KRW/USD"                                  "dollar", "rate", "d"),
    ("DEXBZUS", "BRL/USD"                                  "dollar", "rate", "d"),
    ("DFII5", "5Y TIPS Real Yield"                       "yields", "%", "d"),
    ("DFII30", "30Y TIPS Real Yield"                      "yields", "%", "d"),
    ("T10YIE", "10Y Breakeven Inflation"                  "yields", "%", "d"),
    ("T5YIE", "5Y Breakeven Inflation"                   "yields", "%", "d"),
    ("T5YIFR", "5y5y Forward Breakeven"                   "yields", "%", "d"),
    ("THREEFYTP10", "ACM 10Y Term Premium"                     "yields", "%", "m"),
    ("DGS3MO", "3-Month Treasury Yield"                   "yields", "%", "d"),
    ("BAMLC0A0CM", "IG Corporate Master OAS"                  "credit", "bp", "d"),
    ("BAMLC0A1CAAA", "IG AAA OAS"                               "credit", "bp", "d"),
    ("BAMLC0A2CAA", "IG AA OAS"                                "credit", "bp", "d"),
    ("BAMLC0A3CA", "IG A OAS"                                 "credit", "bp", "d"),
    ("BAMLC0A4CBBB", "IG BBB OAS"                               "credit", "bp", "d"),
    ("BAMLC0A0CMFOAS", "IG Financial OAS"                         "credit", "bp", "d"),
    ("BAMLC0A0CMIOAS", "IG Industrial OAS"                        "credit", "bp", "d"),
    ("BAMLC0A0CMUOAS", "IG Utility OAS"                           "credit", "bp", "d"),
    ("BAMLH0A0HYM2", "HY Master II OAS"                         "credit", "bp", "d"),
    ("BAMLH0A1HYBB", "HY BB OAS"                                "credit", "bp", "d"),
    ("BAMLH0A2HYB", "HY Single-B OAS"                          "credit", "bp", "d"),
    ("BAMLH0A3HYC", "HY CCC & Lower OAS"                       "credit", "bp", "d"),
    ("BAMLEMCBPIOAS", "EM Corporate Plus OAS"                    "credit", "bp", "d"),
    ("BAMLEMHBHYCRPIOAS", "EM HY Corporate OAS"                      "credit", "bp", "d"),
    ("BAMLHE00EHYIOAS", "Euro HY OAS"                              "credit", "bp", "d"),
    ("DRTSCILM", "SLOOS: C&I Standards Tightening"          "stress", "%", "q"),
    ("DRTSCLCC", "SLOOS: Credit Card Tightening"            "stress", "%", "q"),
    ("DRTSCLM", "SLOOS: Mortgage Tightening"               "stress", "%", "q"),
    ("NFCI", "Chicago Fed NFCI"                         "stress", "index", "w"),
    ("ANFCI", "Adjusted NFCI"                            "stress", "index", "w"),
    ("STLFSI4", "St. Louis Fed Financial Stress"           "stress", "index", "w"),
    ("OFRFSI", "OFR Financial Stress Index"               "stress", "index", "d"),
]

# Series where value is already in billions (no conversion needed)
# ops 4411: FRED publishes H.4.1 / SOMA / money-stock series in MILLIONS.
# The prior set wrongly listed them as billions, so the page rendered
# "$6738190.0B" instead of "$6738.19B" (Khalid caught it on the live page).
ALREADY_BILLIONS = {
    "RRPONTSYD",   # FRED: Billions of USD
    "TOTRESNS",    # FRED: Billions of USD
}
# Series in millions -> divide by 1000 to get billions
IN_MILLIONS = {
    "M2SL", "M1SL", "WALCL", "WTREGEN", "WSHOSHO", "WSHOTSL", "WSHOMCB",
    "WRESBAL", "EXCSRESNW", "BOGMBASE", "WORAL", "CURRCIR", "WCURCIR",
    "TREAST", "WSHONBIILB", "WSHOBL", "WSHOFADSL", "WSHOMBLS", "WSHOFCDN",
    "H41RESPPALDKNWA", "WCBSL", "WLRRAL", "RESPPALGUONNWW",
    "RESPPNTEPNWW",
}


# ─── FRED FETCH ────────────────────────────────────────────────────────────
# CACHE-FIRST PATTERN (added 2026-05-31 ops 1071):
# data/fred-cache.json is maintained by justhodl-financial-secretary v2.2 with
# smart TTL per-series (88% hit rate). Reading it as primary source avoids
# the 429 rate-limit storm that occurs when 30+ Lambdas hit shared FRED key.
# Falls back to live FRED with exponential backoff on 429 only when cache miss.

import time as _time_mod

_FRED_CACHE = None  # lazy-loaded once per warm container


def _load_fred_cache():
    """Lazy-load data/fred-cache.json. Returns dict {series_id: [{date,value},...]}."""
    global _FRED_CACHE
    if _FRED_CACHE is not None:
        return _FRED_CACHE
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key="data/fred-cache.json")
        _FRED_CACHE = json.loads(obj["Body"].read().decode("utf-8"))
        print(f"[FRED-cache] loaded {len(_FRED_CACHE)} series from S3 cache")
    except Exception as e:
        print(f"[FRED-cache] load failed: {e}")
        _FRED_CACHE = {}
    return _FRED_CACHE


def _fred_live(series_id, limit, observation_start, max_retries=3):
    """Live FRED call with exponential backoff on 429. Returns list or None."""
    url = (
        f"{FRED_BASE}?series_id={series_id}"
        f"&api_key={FRED_API_KEY}"
        f"&file_type=json&limit={limit}&sort_order=desc"
        f"&observation_start={observation_start}"
    )
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-LiqAgent/2.1"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            observations = data.get("observations", [])
            results = []
            for obs in observations:
                val_str = obs.get("value", ".")
                if val_str not in (".", "", None):
                    try:
                        results.append({"date": obs["date"], "value": float(val_str)})
                    except (ValueError, TypeError):
                        pass
            return results if results else None
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < max_retries - 1:
                wait_s = 2 ** (attempt + 1)  # 2s, 4s, 8s
                print(f"[FRED-live] {series_id} HTTP 429, backoff {wait_s}s (attempt {attempt+1}/{max_retries})")
                _time_mod.sleep(wait_s)
                continue
            print(f"[FRED-live] {series_id} HTTP {e.code}")
            return None
        except Exception as e:
            print(f"[FRED-live] {series_id} {type(e).__name__}: {e}")
            return None
    return None


def fetch_fred(series_id: str, limit: int = 30, observation_start: str = "2020-01-01") -> Optional[List[Dict]]:
    """Fetch FRED series observations. CACHE-FIRST: tries S3 cache, falls back
    to live with 429-aware backoff. Returns list of {date, value} dicts."""
    cache = _load_fred_cache()
    cached_obs = cache.get(series_id)
    if isinstance(cached_obs, list) and len(cached_obs) > 0:
        # Cache hit. Return up to `limit` most recent entries.
        # Cache stores newest first per ops/1070 schema audit.
        sliced = cached_obs[:limit] if limit > 0 else cached_obs
        # Strip _meta field if present (cache adds it on first entry)
        clean = [{"date": o["date"], "value": o["value"]}
                  for o in sliced if "date" in o and "value" in o]
        if clean:
            return clean
    # Cache miss — go live with backoff
    return _fred_live(series_id, limit, observation_start)


def get_latest(series_id: str, limit: int = 10) -> Tuple[Optional[float], Optional[str]]:
    """Get most recent non-null value + date from FRED."""
    obs = fetch_fred(series_id, limit=limit)
    if not obs:
        return None, None
    latest = obs[0]
    val = latest["value"]
    # Unit conversion
    if series_id in IN_MILLIONS:
        val = val / 1000.0
    return round(val, 4), latest["date"]


def get_series_history(series_id: str, limit: int = 52) -> List[Dict]:
    """Get historical series for trend/momentum analysis."""
    obs = fetch_fred(series_id, limit=limit)
    if not obs:
        return []
    result = []
    for o in reversed(obs):  # chronological order
        val = o["value"]
        if series_id in IN_MILLIONS:
            val = val / 1000.0
        result.append({"date": o["date"], "value": round(val, 4)})
    return result


# ─── NET LIQUIDITY COMPUTATION ─────────────────────────────────────────────
def compute_net_liquidity(walcl: float, tga: float, rrp: float) -> float:
    """
    Hedge fund net liquidity formula:
    Net Liquidity = Fed Balance Sheet - TGA - RRP

    Interpretation:
    - When rising → money flowing INTO markets → bullish SPY (3-5 day lead)
    - When falling → money draining FROM markets → bearish SPY (3-5 day lead)
    """
    return round(walcl - tga - rrp, 2)


def compute_liquidity_momentum(history: List[float], periods: int = 4) -> Optional[float]:
    """Compute rate of change over N periods."""
    if len(history) < periods + 1:
        return None
    current = history[-1]
    prev = history[-periods - 1]
    if prev == 0:
        return None
    return round(((current - prev) / prev) * 100, 2)


def classify_liquidity_regime(
    net_liq: float,
    net_liq_4w_ago: float,
    net_liq_13w_ago: float
) -> Dict[str, str]:
    """
    Classify the liquidity regime for the market signal.
    Returns regime label, trend, and SPY signal.
    """
    delta_4w  = net_liq - net_liq_4w_ago
    delta_13w = net_liq - net_liq_13w_ago

    # Trend classification
    if delta_4w > 150:
        trend = "EXPANDING_FAST"
        spy_signal = "BULLISH"
        signal_strength = "STRONG"
    elif delta_4w > 50:
        trend = "EXPANDING"
        spy_signal = "BULLISH"
        signal_strength = "MODERATE"
    elif delta_4w > -50:
        trend = "NEUTRAL"
        spy_signal = "NEUTRAL"
        signal_strength = "WEAK"
    elif delta_4w > -150:
        trend = "CONTRACTING"
        spy_signal = "BEARISH"
        signal_strength = "MODERATE"
    else:
        trend = "CONTRACTING_FAST"
        spy_signal = "BEARISH"
        signal_strength = "STRONG"

    # Structural context (13w)
    if delta_13w > 300:
        structure = "SECULAR_EXPANSION"
    elif delta_13w > 0:
        structure = "CYCLICAL_EXPANSION"
    elif delta_13w > -300:
        structure = "CYCLICAL_CONTRACTION"
    else:
        structure = "SECULAR_CONTRACTION"

    return {
        "trend":           trend,
        "structure":       structure,
        "spy_signal":      spy_signal,
        "signal_strength": signal_strength,
        "delta_4w_bn":     round(delta_4w, 1),
        "delta_13w_bn":    round(delta_13w, 1),
    }


def compute_tga_drain_score(tga_history: List[float]) -> Dict[str, Any]:
    """
    TGA drain analysis — when Treasury draws down TGA, it injects
    liquidity into the system (bullish). When it rebuilds TGA, it drains.
    """
    if len(tga_history) < 4:
        return {"score": 0, "signal": "NEUTRAL", "note": "insufficient data"}

    recent = tga_history[-1]
    wk4    = tga_history[-5] if len(tga_history) >= 5 else tga_history[0]
    change = recent - wk4

    if change < -100:
        signal = "BULLISH_DRAIN"   # TGA falling fast → liquidity injected
        score = min(100, int(abs(change) / 5))
    elif change < -30:
        signal = "MILDLY_BULLISH"
        score = int(abs(change) / 5)
    elif change > 100:
        signal = "BEARISH_REBUILD" # TGA rising fast → liquidity absorbed
        score = -min(100, int(change / 5))
    elif change > 30:
        signal = "MILDLY_BEARISH"
        score = -int(change / 5)
    else:
        signal = "NEUTRAL"
        score = 0

    return {
        "current_bn":   round(recent, 1),
        "4w_ago_bn":    round(wk4, 1),
        "4w_change_bn": round(change, 1),
        "signal":       signal,
        "score":        score,
        "note": f"TGA {'drained' if change < 0 else 'rebuilt'} ${abs(round(change,1))}B in 4 weeks"
    }


def compute_rrp_signal(rrp_history: List[float]) -> Dict[str, Any]:
    """
    RRP dynamics: when institutions park cash at Fed, it drains market liquidity.
    Falling RRP → cash flooding markets → bullish.
    """
    if len(rrp_history) < 4:
        return {"signal": "NEUTRAL", "note": "insufficient data"}

    recent = rrp_history[-1]
    wk4    = rrp_history[-5] if len(rrp_history) >= 5 else rrp_history[0]
    peak   = max(rrp_history)
    change = recent - wk4
    from_peak = recent - peak

    if recent < 100 and change < 0:
        signal = "VERY_BULLISH"  # RRP near zero = all cash deployed
    elif change < -100:
        signal = "BULLISH"
    elif change < -30:
        signal = "MILDLY_BULLISH"
    elif change > 100:
        signal = "BEARISH"
    elif change > 30:
        signal = "MILDLY_BEARISH"
    else:
        signal = "NEUTRAL"

    return {
        "current_bn":     round(recent, 1),
        "4w_change_bn":   round(change, 1),
        "peak_bn":        round(peak, 1),
        "from_peak_bn":   round(from_peak, 1),
        "drain_pct":      round((from_peak / peak) * 100, 1) if peak > 0 else 0,
        "signal":         signal,
        "note": f"RRP at ${round(recent,1)}B ({'falling' if change < 0 else 'rising'} ${abs(round(change,1))}B/4w)"
    }


def compute_composite_liquidity_score(
    regime: Dict,
    tga: Dict,
    rrp: Dict,
    m2_mom: Optional[float],
    dollar_trend: Optional[float]
) -> int:
    """
    Composite 0–100 liquidity score for signal logger.
    >60 = bullish, 40-60 = neutral, <40 = bearish.
    """
    score = 50  # baseline

    # Net liquidity regime (heaviest weight: 40%)
    regime_scores = {
        "EXPANDING_FAST": +30,
        "EXPANDING":      +15,
        "NEUTRAL":          0,
        "CONTRACTING":    -15,
        "CONTRACTING_FAST": -30,
    }
    score += regime_scores.get(regime["trend"], 0)

    # TGA drain (25%)
    tga_score = tga.get("score", 0)
    score += min(20, max(-20, int(tga_score * 0.25)))

    # RRP signal (20%)
    rrp_map = {
        "VERY_BULLISH": +15,
        "BULLISH":      +10,
        "MILDLY_BULLISH": +5,
        "NEUTRAL": 0,
        "MILDLY_BEARISH": -5,
        "BEARISH": -10,
    }
    score += rrp_map.get(rrp.get("signal", "NEUTRAL"), 0)

    # M2 growth (10%)
    if m2_mom is not None:
        if m2_mom > 5:
            score += 8
        elif m2_mom > 2:
            score += 4
        elif m2_mom < -2:
            score -= 6

    # Dollar (5%) — inverse: strong dollar = tighter global liquidity
    if dollar_trend is not None:
        if dollar_trend > 2:
            score -= 5  # strong dollar = tight
        elif dollar_trend < -2:
            score += 5  # weak dollar = loose

    return min(100, max(0, score))


# ─── MAIN HANDLER ──────────────────────────────────────────────────────────


# ── ops 4421: Perplexity Part-4 structural additions for liquidity.html ──
# (A) Global vs US: Fed+ECB+BOJ+PBOC stack from global-liquidity/china feeds.
# (B) DXY first-class (brain: "DXY is the most important chart").
# (C) Credit-first sequencing (brain rule: "credit stress first, dollar spike
#     second, stock crash third") — HY OAS z -> DXY z -> SPX 60d, with the
#     current stage highlighted. All joins are honest: missing inputs are
#     reported, never faked.
def _sfeed(key):
    try:
        return json.loads(S3.get_object(Bucket=S3_BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        try:
            import boto3 as _b
            c = _b.client("s3", region_name="us-east-1")
            return json.loads(c.get_object(Bucket="justhodl-dashboard-live",
                                           Key=key)["Body"].read())
        except Exception as e:
            print(f"[part4] {key}: {type(e).__name__}")
            return None



def _find_cb_map(obj, _d=0):
    """ops 4423: locate a {CB-name: usd_bn} map anywhere in the feed."""
    CBS = {"fed", "ecb", "boj", "pboc", "boe", "snb"}
    if _d > 4 or obj is None:
        return None
    if isinstance(obj, dict):
        keys = {str(k).lower() for k in obj.keys()}
        if len(keys & CBS) >= 2 and any(
                isinstance(v, (int, float)) for v in obj.values()):
            return obj
        for v in obj.values():
            r = _find_cb_map(v, _d + 1)
            if r:
                return r
    elif isinstance(obj, list):
        for v in obj[:15]:
            r = _find_cb_map(v, _d + 1)
            if r:
                return r
    return None


def build_part4(catalog):
    out = {}
    # (A) global 4-CB stack
    gl = _sfeed("data/global-liquidity.json")
    cn = _sfeed("data/china-liquidity.json")
    stack, notes = {}, []
    if isinstance(gl, dict):
        # ops 4423: DISCOVER the component map rather than guessing its key —
        # the same technique that found treasury-noise:noise_bps.
        comp = None
        for k in ("components_usd_bn", "components", "cb_components",
                  "by_cb", "central_banks", "stack"):
            v = gl.get(k)
            if isinstance(v, dict) and any(
                    isinstance(x, (int, float)) for x in v.values()):
                comp = v
                break
        if comp is None:
            comp = _find_cb_map(gl)
        if isinstance(comp, dict):
            stack.update({str(k): float(v) for k, v in comp.items()
                          if isinstance(v, (int, float))})
        for k in ("total_usd_bn", "global_total_usd_bn", "total"):
            if isinstance(gl.get(k), (int, float)):
                out["global_total_usd_bn"] = gl[k]
                break
        for k in ("change_13w_pct", "change_52w_pct"):
            if isinstance(gl.get(k), (int, float)):
                out[k] = gl[k]
    else:
        notes.append("global-liquidity.json unavailable")
    if isinstance(cn, dict):
        for k in ("pboc_assets_usd_bn", "pboc_balance_sheet_usd_bn",
                  "china_m2_usd_bn"):
            if isinstance(cn.get(k), (int, float)):
                stack.setdefault("PBOC", cn[k])
                break
        ci = cn.get("credit_impulse")
        if isinstance(ci, (int, float)):
            out["china_credit_impulse"] = ci
        elif isinstance(cn.get("credit_impulse_flow_yoy_pct"), (int, float)):
            out["china_credit_impulse"] = cn["credit_impulse_flow_yoy_pct"]
    else:
        notes.append("china-liquidity.json unavailable")
    out["global_stack_usd_bn"] = stack
    out["global_stack_note"] = ("Fed+ECB+BOJ+PBOC balance sheets in USD; "
                               "page was US-only before (Perplexity Part-4 A)")

    # (B) DXY first-class
    dr = _sfeed("data/dollar-radar.json")
    dxy = {}
    if isinstance(dr, dict):
        for k in ("dxy", "broad_dollar", "index", "level"):
            v = dr.get(k)
            if isinstance(v, (int, float)):
                dxy["level"] = v
                break
            if isinstance(v, dict) and isinstance(v.get("level"),
                                                  (int, float)):
                dxy["level"] = v["level"]
                break
        for k in ("regime", "verdict", "breadth_verdict", "state"):
            if isinstance(dr.get(k), str):
                dxy["regime"] = dr[k]
                break
        if isinstance(dr.get("as_of"), str):
            dxy["as_of"] = dr["as_of"]
    cd = (catalog.get("dollar") or {}).get("DTWEXBGS") or {}
    if cd:
        dxy.setdefault("level", cd.get("value"))
        dxy["z"] = cd.get("z")
        dxy["pctile_5y"] = cd.get("pctile_5y")
    if not dxy:
        notes.append("dollar-radar.json unavailable and DTWEXBGS missing")
    dxy["note"] = "brain: DXY is the most important chart — promoted to hero"
    out["dxy"] = dxy

    # (C) credit-first sequencing — the brain's ordering rule
    # ops 4423: fetch HY OAS DIRECTLY. Depending on catalog["credit"] failed —
    # that category never materialised, so stage 1 read null.
    hy = (catalog.get("credit") or {}).get("BAMLH0A0HYM2") or {}
    hy_val, hy_z = hy.get("value"), hy.get("z")
    if hy_val is None:
        hh = get_series_history("BAMLH0A0HYM2", limit=1300) or []
        hv = [x["value"] for x in hh if x.get("value") is not None]
        if hv:
            hy_val = round(hv[-1], 3)
            m = sum(hv) / len(hv)
            sd = (sum((v - m) ** 2 for v in hv) / max(1, len(hv) - 1)) ** 0.5
            hy_z = round((hy_val - m) / sd, 2) if sd > 1e-9 else 0.0
    seq_stages = []
    seq_stages.append({"stage": 1, "name": "Credit stress (HY OAS)",
                       "value": hy_val, "z": hy_z, "unit": "%",
                       "fired": bool(hy_z is not None and hy_z > 1.0)})
    d_z = dxy.get("z")
    seq_stages.append({"stage": 2, "name": "Dollar spike (DXY)",
                       "value": dxy.get("level"), "z": d_z,
                       "fired": bool(d_z is not None and d_z > 1.0)})
    # ops 4423: compute SPX 60d from FRED SP500 directly — the fleet-feed
    # probe missed, so stage 3 read null.
    spx_60d = None
    try:
        sp = get_series_history("SP500", limit=200) or []
        sv = [x["value"] for x in sp if x.get("value") is not None]
        if len(sv) >= 61:
            spx_60d = round((sv[-1] / sv[-61] - 1) * 100, 2)
    except Exception as _e:
        print("[part4] SP500 60d:", type(_e).__name__)
    seq_stages.append({"stage": 3, "name": "Equity drawdown (SPX 60d)",
                       "value": spx_60d,
                       "fired": bool(spx_60d is not None and spx_60d < -5)})
    fired = [s["stage"] for s in seq_stages if s["fired"]]
    out["credit_first_sequence"] = {
        "stages": seq_stages, "stages_fired": fired,
        "current_stage": (max(fired) if fired else 0),
        "verdict": ("CLEAR — no stage firing" if not fired else
                    f"STAGE {max(fired)} firing"),
        "brain_rule": "credit stress first, dollar spike second, "
                      "stock crash third"}
    if notes:
        out["join_notes"] = notes
    return out


def _deep_num(obj, patterns, _d=0):
    if _d > 4 or obj is None:
        return None
    if isinstance(obj, dict):
        for k, v in obj.items():
            if any(p in str(k).lower() for p in patterns) and \
                    isinstance(v, (int, float)):
                return float(v)
            r = _deep_num(v, patterns, _d + 1)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for v in obj[:15]:
            r = _deep_num(v, patterns, _d + 1)
            if r is not None:
                return r
    return None


def lambda_handler(event: Dict, context: Any) -> Dict:
    print("[LiqAgent] Starting TGA + Fed Liquidity analysis...")
    ts_start = datetime.now(timezone.utc)

    # ── Fetch all core series ──────────────────────────────────────────────
    print("[LiqAgent] Fetching FRED series...")

    walcl_val, walcl_date     = get_latest("WALCL", 5)
    tga_val, tga_date         = get_latest("WTREGEN", 5)
    rrp_val, rrp_date         = get_latest("RRPONTSYD", 5)

    if any(v is None for v in [walcl_val, tga_val, rrp_val]):
        print(f"[LiqAgent] WARNING: Core series missing — WALCL={walcl_val}, TGA={tga_val}, RRP={rrp_val}")
        # Partial data — continue with whatever we have
        walcl_val = walcl_val or 0
        tga_val   = tga_val or 0
        rrp_val   = rrp_val or 0

    # Net Liquidity
    net_liquidity = compute_net_liquidity(walcl_val, tga_val, rrp_val)
    print(f"[LiqAgent] Net Liquidity: ${net_liquidity}B (WALCL={walcl_val}, TGA={tga_val}, RRP={rrp_val})")

    # ── Historical series for regime analysis ─────────────────────────────
    print("[LiqAgent] Fetching historical data for regime analysis...")
    walcl_hist = get_series_history("WALCL", limit=60)
    tga_hist   = get_series_history("WTREGEN", limit=60)
    rrp_hist   = get_series_history("RRPONTSYD", limit=60)
    m2_hist    = get_series_history("M2SL", limit=24)

    # Build net liquidity history
    net_liq_history = []
    for i, w in enumerate(walcl_hist):
        matching_tga = next((t["value"] for t in tga_hist if t["date"] <= w["date"]), None)
        matching_rrp = next((r["value"] for r in rrp_hist if r["date"] <= w["date"]), None)
        if matching_tga and matching_rrp:
            nl = compute_net_liquidity(w["value"], matching_tga, matching_rrp)
            net_liq_history.append({"date": w["date"], "value": nl})

    # Regime classification
    regime = {"trend": "NEUTRAL", "structure": "UNKNOWN", "spy_signal": "NEUTRAL",
              "signal_strength": "WEAK", "delta_4w_bn": 0, "delta_13w_bn": 0}
    if len(net_liq_history) >= 14:
        nl_vals = [x["value"] for x in net_liq_history]
        nl_4w   = nl_vals[-5] if len(nl_vals) >= 5 else nl_vals[0]
        nl_13w  = nl_vals[-14] if len(nl_vals) >= 14 else nl_vals[0]
        regime  = classify_liquidity_regime(net_liquidity, nl_4w, nl_13w)

    # Component analysis
    tga_analysis = compute_tga_drain_score([x["value"] for x in tga_hist])
    rrp_analysis = compute_rrp_signal([x["value"] for x in rrp_hist])

    # M2 momentum
    m2_mom = None
    if len(m2_hist) >= 13:
        m2_vals = [x["value"] for x in m2_hist]
        m2_mom = compute_liquidity_momentum(m2_vals, 12)

    # ── Fetch supplemental series ─────────────────────────────────────────
    print("[LiqAgent] Fetching supplemental FRED series...")
    soma_val,    soma_date    = get_latest("WSHOSHO", 5)
    soma_tsys,   _            = get_latest("WSHOTSL", 5)
    soma_mbs,    _            = get_latest("WSHOMCB", 5)
    reserves_val, res_date    = get_latest("WRESBAL", 5)
    excess_res,  _            = get_latest("EXCSRESNW", 5)
    m2_val,      m2_date      = get_latest("M2SL", 5)
    m1_val,      _            = get_latest("M1SL", 5)
    mon_base,    _            = get_latest("BOGMBASE", 5)
    dxy_val,     dxy_date     = get_latest("DTWEXBGS", 5)
    dxy_adv,     _            = get_latest("DTWEXAFEGS", 5)
    dxy_em,      _            = get_latest("DTWEXEMEGS", 5)
    y2_val,      y2_date      = get_latest("DGS2", 5)
    y10_val,     y10_date     = get_latest("DGS10", 5)
    y30_val,     _            = get_latest("DGS30", 5)
    spread_10_2, _            = get_latest("T10Y2Y", 5)
    spread_10_3m,_            = get_latest("T10Y3M", 5)
    tips_val,    _            = get_latest("DFII10", 5)
    sofr_val,    sofr_date    = get_latest("SOFR", 5)
    dff_val,     dff_date     = get_latest("DFF", 5)

    # Dollar trend (4-week momentum)
    dxy_history  = get_series_history("DTWEXBGS", limit=20)
    dollar_trend = None
    if len(dxy_history) >= 20:
        dxy_vals     = [x["value"] for x in dxy_history]
        dollar_trend = compute_liquidity_momentum(dxy_vals, 20)

    # ── Composite Score ───────────────────────────────────────────────────
    composite_score = compute_composite_liquidity_score(
        regime, tga_analysis, rrp_analysis, m2_mom, dollar_trend
    )

    # Score interpretation
    if composite_score >= 70:
        composite_label  = "VERY_BULLISH"
        composite_color  = "#00ff88"
    elif composite_score >= 60:
        composite_label  = "BULLISH"
        composite_color  = "#4caf50"
    elif composite_score >= 45:
        composite_label  = "NEUTRAL"
        composite_color  = "#ffd700"
    elif composite_score >= 35:
        composite_label  = "BEARISH"
        composite_color  = "#ff6b35"
    else:
        composite_label  = "VERY_BEARISH"
        composite_color  = "#f44336"

    # ── SPY Leading Indicator ─────────────────────────────────────────────
    spy_signal = {
        "direction":  regime["spy_signal"],
        "strength":   regime["signal_strength"],
        "lead_days":  "3-5",
        "basis":      f"Net liquidity {regime['trend'].lower().replace('_', ' ')} by ${abs(regime['delta_4w_bn'])}B over 4 weeks",
        "confidence": min(95, max(30, composite_score if regime["spy_signal"] == "BULLISH" else 100 - composite_score)),
        "components": {
            "fed_bs_trend":    "EXPANDING" if (walcl_hist[-1]["value"] > walcl_hist[-5]["value"] if len(walcl_hist) >= 5 else False) else "CONTRACTING",
            "tga_drain":       tga_analysis["signal"],
            "rrp_signal":      rrp_analysis["signal"],
            "m2_momentum":     f"{m2_mom:+.1f}%" if m2_mom else "N/A",
            "dollar_pressure": "STRONG" if (dollar_trend or 0) > 1 else ("WEAK" if (dollar_trend or 0) < -1 else "NEUTRAL"),
        }
    }

    # ── Chart-ready history (last 52 weeks) ───────────────────────────────
    chart_data = {
        "net_liquidity": net_liq_history[-52:],
        "fed_bs":   [{"date": x["date"], "value": x["value"]} for x in walcl_hist[-52:]],
        "tga":      [{"date": x["date"], "value": x["value"]} for x in tga_hist[-52:]],
        "rrp":      [{"date": x["date"], "value": x["value"]} for x in rrp_hist[-52:]],
        "m2":       [{"date": x["date"], "value": x["value"]} for x in m2_hist[-24:]],
    }

    # ── Assemble Final Output ─────────────────────────────────────────────
    ts_end  = datetime.now(timezone.utc)
    elapsed = round((ts_end - ts_start).total_seconds(), 1)

    # ── ops 4409: generic catalog fetch — every FRED_SERIES entry gets a
    # latest value + 5y z-score + 52pt history, grouped by category, so the
    # 47 new institutional series (credit OAS ladder, SLOOS, TIPS/breakevens,
    # bilateral USD, term premium, stress indices) reach the feed and page.
    catalog = {}
    KNOWN_CATS = {"fed_balance_sheet", "tga", "rrp", "soma", "reserves",
                  "money_supply", "dollar", "yields", "funding", "credit",
                  "stress"}
    for _entry in FRED_SERIES:
        sid = _entry[0]
        label = _entry[1] if len(_entry) > 1 else sid
        _p2 = _entry[2] if len(_entry) > 2 else ""
        if _p2 in KNOWN_CATS:
            cat = _p2
            unit = _entry[3] if len(_entry) > 3 else ""
            freq = _entry[4] if len(_entry) > 4 else "d"
        else:
            # 4-tuple shape (id, label, unit, freq) — no category declared
            cat = "other"
            unit = _p2
            freq = _entry[3] if len(_entry) > 3 else "d"
        try:
            hist = get_series_history(sid, limit=260)
            if not hist:
                continue
            vals = [h["value"] for h in hist if h.get("value") is not None]
            if not vals:
                continue
            latest = vals[-1]
            mean = sum(vals) / len(vals)
            var = sum((v - mean) ** 2 for v in vals) / max(1, len(vals) - 1)
            sd = var ** 0.5
            z = round((latest - mean) / sd, 2) if sd > 1e-9 else 0.0
            pct = round(100 * sum(1 for v in vals if v <= latest) / len(vals), 1)
            catalog.setdefault(cat, {})[sid] = {
                "label": label, "value": round(latest, 4), "unit": unit,
                "z": z, "pctile_5y": pct, "freq": freq,
                "date": hist[-1].get("date"),
                "spark": [{"date": h["date"], "value": h["value"]}
                          for h in hist[-52:]],
            }
        except Exception as _e:
            print(f"[catalog] {sid} skipped: {type(_e).__name__}")
    print(f"[LiqAgent] catalog built: {sum(len(v) for v in catalog.values())} "
          f"series across {len(catalog)} categories")

    output = {
        "meta": {
            "generated_at":  ts_end.isoformat(),
            "elapsed_sec":   elapsed,
            "agent_version": "2.0.0",
            "data_sources":  ["FRED"],
        },

        # ── ops 4409: full institutional series catalog by category ──
        "catalog": catalog,
        "part4": build_part4(catalog),   # ops 4421 structural additions

        # ── Core Liquidity Triad ──────────────────────────────────────────
        "core": {
            "net_liquidity": {
                "value_bn":     net_liquidity,
                "label":        composite_label,
                "score":        composite_score,
                "color":        composite_color,
            },
            "fed_balance_sheet": {
                "value_bn":     walcl_val,
                "date":         walcl_date,
                "unit":         "Billions USD",
            },
            "tga": {
                "value_bn":     tga_val,
                "date":         tga_date,
                "unit":         "Billions USD",
            },
            "rrp": {
                "value_bn":     rrp_val,
                "date":         rrp_date,
                "unit":         "Billions USD",
            },
        },

        # ── Regime Analysis ───────────────────────────────────────────────
        "regime": regime,

        # ── Component Analysis ────────────────────────────────────────────
        "components": {
            "tga_analysis":  tga_analysis,
            "rrp_analysis":  rrp_analysis,
        },

        # ── SPY Leading Indicator ─────────────────────────────────────────
        "spy_signal": spy_signal,

        # ── SOMA / Fed Holdings ───────────────────────────────────────────
        "soma": {
            "total_bn":      soma_val,
            "treasuries_bn": soma_tsys,
            "mbs_bn":        soma_mbs,
            "date":          soma_date,
        },

        # ── Reserve Balances ──────────────────────────────────────────────
        "reserves": {
            "total_bn":   reserves_val,
            "excess_bn":  excess_res,
            "date":       res_date,
        },

        # ── Money Supply ──────────────────────────────────────────────────
        "money_supply": {
            "m2_bn":         m2_val,
            "m1_bn":         m1_val,
            "monetary_base": mon_base,
            "m2_yoy_pct":    m2_mom,
            "date":          m2_date,
        },

        # ── Dollar Index ──────────────────────────────────────────────────
        "dollar": {
            "broad_index":   dxy_val,
            "advanced_econ": dxy_adv,
            "emerging_mkt":  dxy_em,
            "trend_4w_pct":  dollar_trend,
            "date":          dxy_date,
        },

        # ── Treasury Yields ───────────────────────────────────────────────
        "yields": {
            "y2":            y2_val,
            "y10":           y10_val,
            "y30":           y30_val,
            "spread_10_2":   spread_10_2,
            "spread_10_3m":  spread_10_3m,
            "tips_real":     tips_val,
            "date":          y10_date,
            "curve_status":  "INVERTED" if (spread_10_2 or 0) < 0 else "NORMAL",
        },

        # ── Funding Markets ───────────────────────────────────────────────
        "funding": {
            "sofr":          sofr_val,
            "fed_funds":     dff_val,
            "date":          sofr_date,
        },

        # ── Chart Data ────────────────────────────────────────────────────
        "chart_data": chart_data,

        # ── Signal Logger Integration ─────────────────────────────────────
        "signal_logger": {
            "signal_name":   "liquidity_spy_3_5d",
            "direction":     spy_signal["direction"],
            "score":         composite_score,
            "label":         composite_label,
            "lead_days":     "3-5",
            "metadata": {
                "net_liquidity_bn":  net_liquidity,
                "4w_change_bn":      regime["delta_4w_bn"],
                "tga_signal":        tga_analysis["signal"],
                "rrp_signal":        rrp_analysis["signal"],
                "regime":            regime["trend"],
            }
        }
    }

    # ── Write to S3 ───────────────────────────────────────────────────────
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=S3_KEY,
            Body=json.dumps(output, default=str).encode("utf-8"),
            ContentType="application/json",
            CacheControl="no-cache, max-age=0",
        )
        print(f"[LiqAgent] ✅ Saved to s3://{S3_BUCKET}/{S3_KEY}")
    except Exception as e:
        print(f"[LiqAgent] ❌ S3 write error: {e}")
        return {"statusCode": 500, "body": str(e)}

    print(f"[LiqAgent] Done in {elapsed}s | Net Liq={net_liquidity}B | Score={composite_score} ({composite_label})")
    return {
        "statusCode": 200,
        "body": json.dumps({
            "net_liquidity_bn": net_liquidity,
            "score":            composite_score,
            "label":            composite_label,
            "spy_signal":       spy_signal["direction"],
            "regime":           regime["trend"],
            "elapsed_sec":      elapsed,
        })
    }
