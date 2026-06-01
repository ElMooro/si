"""
justhodl-pump-mechanics
═══════════════════════
Market-structure layer for pump candidates. Two outputs per ticker:

1. SQUEEZE PROFILE (proxy — direct SI requires FINRA Reg SHO data,
   tracked in KHALID_ACTIONS #2)

   Components used as squeeze proxies:
   - Float size (shares outstanding from FMP profile)
   - Float tier: XS (<10M), S (<50M), M (<500M), L (<5B), XL (≥5B)
   - Float rotation = today's volume / shares outstanding (high = active turnover)
   - 20d avg float rotation (baseline)
   - Float rotation acceleration = today / baseline
   - SHORTS_COVERING flag from options-flow engine (if present in flags)
   - sec-filings 13D/13G concentration (insider concentration → squeeze risk)
   - ARK + institutional concentration in small float

   Squeeze proxy score (0-100):
     30 × float-tier-bonus (XS=30, S=22, M=14, L=6, XL=0)
   + 25 × float_rotation_accel_norm
   + 20 × options-flow CPR_SURGING or CALL_VOL_3X presence
   + 15 × ATR%
   + 10 × concentration_proxy

2. OPTIONS SKEW + IV RANK (from existing options-flow engine outputs)

   - Bullish/bearish skew: parsed from options-flow tier
   - CPR (call/put ratio) tier
   - Call volume multiplier (CALL_VOL_2X, CALL_VOL_3X)
   - IV rank proxy: current 14d ATR vs 90d ATR range (computed locally)
     0 = at 90d low (low expected moves), 100 = at 90d high (high expected moves)
   - Term structure proxy (5d realized vs 30d realized — backwardation flag)

INPUTS
══════
data/convergence-radar.json   →  pump_candidates[]
data/options-flow.json        →  for skew/CPR data
data/sec-filings-intel.json   →  for concentration signals

OUTPUT
══════
data/pump-mechanics.json
{
  "schema_version": "1.0",
  "generated_at":   "...",
  "n_candidates":   12,
  "candidates": [
    {
      "ticker": "PLTR",
      "squeeze_profile": {
        "shares_outstanding": 2.4e9,
        "float_tier":         "XL",
        "float_rotation_today": 0.018,
        "float_rotation_20d":   0.012,
        "rotation_accel":       1.50,
        "shorts_covering_flag": true,
        "concentration_signals": ["...", "..."],
        "squeeze_proxy_score":  42.5,
        "squeeze_caveat":       "Proxy score — direct SI requires FINRA Reg SHO"
      },
      "options_structure": {
        "from_engine":        "options-flow",
        "tier":               "TIER_A_BULLISH_FLOW",
        "skew":               "bullish",
        "flags":              ["CPR_SURGING", "CALL_VOL_3X"],
        "iv_rank_proxy":      72.5,
        "term_structure":     "backwardation",   # 5d > 30d HV
        "iv_pct_explanation": "Current 14d ATR at 72% of 90d range"
      }
    },
    ...
  ]
}

SCHEDULE
════════
cron(15 * * * ? *) — hourly at :15 (after positioning at :10)
"""
import json
import math
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

import boto3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

S3_BUCKET     = "justhodl-dashboard-live"
RADAR_KEY     = "data/convergence-radar.json"
OFLOW_KEY     = "data/options-flow.json"
SEC_KEY       = "data/sec-filings-intel.json"
OUTPUT_KEY    = "data/pump-mechanics.json"
FMP_KEY       = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")

s3 = boto3.client("s3", region_name="us-east-1")


# ═════════════════════════════════════════════════════════════════════
# FMP fetchers
# ═════════════════════════════════════════════════════════════════════

def fetch_profile(ticker: str) -> Optional[dict]:
    try:
        url = f"https://financialmodelingprep.com/stable/profile?symbol={ticker}&apikey={FMP_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl/mechanics"})
        with urllib.request.urlopen(req, timeout=12) as r:
            data = json.loads(r.read())
        if isinstance(data, list) and data:
            return data[0]
        return None
    except Exception as e:
        print(f"[profile] {ticker}: {str(e)[:100]}")
        return None


def fetch_price_history(ticker: str, days: int = 90) -> List[dict]:
    try:
        end = datetime.now(timezone.utc).date()
        start = end - timedelta(days=days)
        url = (f"https://financialmodelingprep.com/stable/historical-price-eod/full"
                f"?symbol={ticker}&from={start.isoformat()}&to={end.isoformat()}&apikey={FMP_KEY}")
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl/mechanics"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
        rows = data if isinstance(data, list) else data.get("historical", [])
        return sorted(rows, key=lambda x: x.get("date", ""))
    except Exception as e:
        print(f"[price] {ticker}: {str(e)[:100]}")
        return []


# ═════════════════════════════════════════════════════════════════════
# Float / squeeze analytics
# ═════════════════════════════════════════════════════════════════════

def classify_float_tier(shares_out: float) -> tuple:
    """Returns (tier_name, squeeze_bonus_pts)."""
    if shares_out is None:
        return ("?", 0)
    if shares_out < 10e6:    return ("XS", 30)
    if shares_out < 50e6:    return ("S", 22)
    if shares_out < 500e6:   return ("M", 14)
    if shares_out < 5e9:     return ("L", 6)
    return ("XL", 0)


def compute_float_rotation(rows: List[dict], shares_out: float, days: int = 1) -> Optional[float]:
    """Float rotation = total volume in last N days / shares outstanding."""
    if not rows or not shares_out or shares_out <= 0:
        return None
    vols = [r.get("volume", 0) or 0 for r in rows[-days:]]
    if not vols:
        return None
    return sum(vols) / shares_out / days  # avg per-day rotation


def compute_atr(rows: List[dict], period: int = 14) -> Optional[float]:
    if not rows or len(rows) < period + 1:
        return None
    trs = []
    for i in range(1, len(rows)):
        h = rows[i].get("high"); l = rows[i].get("low"); pc = rows[i-1].get("close")
        if None in (h, l, pc):
            continue
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period


def compute_atr_range_rank(rows: List[dict]) -> Optional[float]:
    """IV rank proxy: where does today's 14d ATR sit within the 90d ATR range?
    0 = at 90d low, 100 = at 90d high."""
    if not rows or len(rows) < 30:
        return None
    # Rolling 14d ATR series across the 90 days
    atrs = []
    for end_idx in range(15, len(rows) + 1):
        window = rows[end_idx-15:end_idx]
        a = compute_atr(window, period=14)
        if a is not None:
            atrs.append(a)
    if len(atrs) < 5:
        return None
    cur = atrs[-1]
    lo, hi = min(atrs), max(atrs)
    if hi == lo:
        return 50.0
    return round((cur - lo) / (hi - lo) * 100, 1)


def compute_realized_vol(rows: List[dict], window: int) -> Optional[float]:
    """Annualized realized vol over the last `window` days."""
    if not rows or len(rows) < window + 1:
        return None
    closes = [r.get("close") for r in rows[-(window+1):] if r.get("close")]
    if len(closes) < 5:
        return None
    rets = []
    for i in range(1, len(closes)):
        if closes[i] > 0 and closes[i-1] > 0:
            rets.append(math.log(closes[i] / closes[i-1]))
    if len(rets) < 3:
        return None
    m = sum(rets) / len(rets)
    var = sum((r - m) ** 2 for r in rets) / max(1, len(rets) - 1)
    return math.sqrt(var) * math.sqrt(252) * 100


# ═════════════════════════════════════════════════════════════════════
# Options skew extraction from options-flow engine output
# ═════════════════════════════════════════════════════════════════════

def load_options_flow_data() -> Dict[str, dict]:
    """Map ticker → options-flow record."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=OFLOW_KEY)
        d = json.loads(obj["Body"].read())
        out = {}
        for item in (d.get("all_qualifying") or []):
            t = item.get("symbol")
            if t:
                out[t] = item
        return out
    except Exception as e:
        print(f"[options-flow] {e}")
        return {}


def parse_options_skew(of_record: dict) -> dict:
    """From an options-flow record, derive skew + IV interpretation."""
    if not of_record:
        return {"available": False}
    tier = of_record.get("tier", "")
    flags = of_record.get("flags", []) or []
    score = of_record.get("score")

    if "BULLISH" in tier:
        skew = "bullish"
    elif "BEARISH" in tier:
        skew = "bearish"
    elif "UNUSUAL" in tier:
        skew = "unusual_mixed"
    else:
        skew = "neutral"

    # Multiplier from flags
    call_vol_mult = None
    for f in flags:
        if "CALL_VOL_" in str(f):
            try:
                call_vol_mult = float(str(f).replace("CALL_VOL_", "").replace("X", ""))
            except Exception:
                pass

    has_shorts_covering = any("SHORTS_COVERING" in str(f) for f in flags)
    has_cpr_surging = any("CPR_SURGING" in str(f) for f in flags)
    has_abs_cpr = any("ABS_CPR" in str(f) for f in flags)

    return {
        "available":           True,
        "from_engine":         "options-flow",
        "tier":                tier,
        "engine_score":        score,
        "skew":                skew,
        "flags":               flags,
        "call_vol_multiplier": call_vol_mult,
        "shorts_covering":     has_shorts_covering,
        "cpr_surging":         has_cpr_surging,
        "abs_cpr_unusual":     has_abs_cpr,
    }


# ═════════════════════════════════════════════════════════════════════
# Concentration signals from sec-filings + ARK
# ═════════════════════════════════════════════════════════════════════

def load_sec_filings_map() -> Dict[str, dict]:
    """ticker → sec-filings-intel record."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=SEC_KEY)
        d = json.loads(obj["Body"].read())
        out = {}
        for r in d.get("all_tickers", []):
            t = r.get("ticker")
            if t:
                out[t] = r
        return out
    except Exception:
        return {}


def extract_concentration_signals(sec_record: dict) -> List[str]:
    """Pull 13D/13G / large institutional ownership signals."""
    if not sec_record:
        return []
    out = []
    events = sec_record.get("events") or []
    for ev in events[:8]:
        if isinstance(ev, dict):
            t = (ev.get("type") or "").upper()
            if "13D" in t or "13G" in t:
                out.append(f"{ev.get('type','?')}: {(ev.get('description') or '')[:80]}")
            elif "INSIDER" in t and "BUY" in (ev.get("direction") or "").upper():
                out.append(f"Insider buying: {(ev.get('description') or '')[:80]}")
    return out[:5]


# ═════════════════════════════════════════════════════════════════════
# Compute squeeze proxy score per ticker
# ═════════════════════════════════════════════════════════════════════

def compute_squeeze_score(
    float_tier_bonus: float,
    rotation_accel: Optional[float],
    options_data: dict,
    atr_pct: Optional[float],
    concentration_count: int,
) -> float:
    """Combine the proxy signals into 0-100 squeeze score.

    Components:
      30 × float-tier (XS=30 ... XL=0)
      25 × rotation acceleration norm (capped at 3x = full pts)
      20 × options-flow squeeze signals
      15 × ATR % (clipped to 10%)
      10 × concentration_proxy (1+ signals → 10pts)
    """
    # Float tier already in pts
    s = float_tier_bonus  # 0-30

    # Rotation acceleration: 1x = 0, 2x = 12.5, 3x+ = 25
    if rotation_accel:
        s += min(25, max(0, (rotation_accel - 1.0) * 12.5))

    # Options-flow squeeze signals
    of_pts = 0
    if options_data.get("available"):
        if options_data.get("shorts_covering"):  of_pts += 8
        if options_data.get("cpr_surging"):      of_pts += 6
        if options_data.get("abs_cpr_unusual"):  of_pts += 4
        if (options_data.get("call_vol_multiplier") or 0) >= 3:
            of_pts += 2
    s += min(20, of_pts)

    # ATR%
    if atr_pct:
        s += min(15, atr_pct * 1.5)

    # Concentration
    if concentration_count >= 1:
        s += 10

    return round(min(100, s), 1)


def classify_squeeze_potential(score: float) -> str:
    if score >= 70: return "SQUEEZE_PRIMED"
    if score >= 55: return "SQUEEZE_POSSIBLE"
    if score >= 40: return "MILD_SQUEEZE"
    return "LOW"


def classify_term_structure(rv_5d: Optional[float], rv_30d: Optional[float]) -> str:
    """Realized vol term structure — backwardation = front higher than back (event/squeeze)."""
    if rv_5d is None or rv_30d is None:
        return "?"
    if rv_5d > rv_30d * 1.25:
        return "backwardation"  # event-driven, front-loaded
    if rv_5d < rv_30d * 0.8:
        return "contango"  # vol compressed, expansion possible
    return "flat"


# ═════════════════════════════════════════════════════════════════════
# Per-candidate enrichment
# ═════════════════════════════════════════════════════════════════════

def enrich_candidate(cand: dict, options_map: Dict[str, dict],
                       sec_map: Dict[str, dict]) -> dict:
    ticker = cand["ticker"]
    profile = fetch_profile(ticker)
    rows = fetch_price_history(ticker, 90)

    shares_out = (profile or {}).get("sharesOutstanding") or (profile or {}).get("shares_outstanding")
    # Fallback: derive from marketCap / price (FMP /stable/profile doesn't expose shares directly)
    if not shares_out and profile:
        mcap  = profile.get("marketCap")
        price = profile.get("price")
        if mcap and price and price > 0:
            shares_out = mcap / price
    float_tier, float_pts = classify_float_tier(shares_out)
    rotation_today = compute_float_rotation(rows, shares_out, days=1) if shares_out and rows else None
    rotation_20d   = compute_float_rotation(rows, shares_out, days=20) if shares_out and rows else None
    rotation_accel = (rotation_today / rotation_20d) if rotation_today and rotation_20d and rotation_20d > 0 else None

    atr_14 = compute_atr(rows, 14)
    current = (rows[-1].get("close") if rows else None) or (profile or {}).get("price")
    atr_pct = (atr_14 / current * 100) if atr_14 and current else None

    # Options data from existing engine
    options_data = parse_options_skew(options_map.get(ticker))
    iv_rank_proxy = compute_atr_range_rank(rows) if rows else None
    if options_data.get("available") and iv_rank_proxy is not None:
        options_data["iv_rank_proxy"] = iv_rank_proxy
        options_data["iv_pct_explanation"] = (
            f"Current 14d ATR at {iv_rank_proxy}% of 90d range "
            f"({'high' if iv_rank_proxy >= 70 else 'moderate' if iv_rank_proxy >= 30 else 'low'} expected move pressure)"
        )

    # Term structure proxy from realized vols
    rv_5d = compute_realized_vol(rows, 5) if rows else None
    rv_30d = compute_realized_vol(rows, 30) if rows else None
    term_struct = classify_term_structure(rv_5d, rv_30d)
    if options_data.get("available"):
        options_data["realized_vol_5d"]  = round(rv_5d, 2) if rv_5d else None
        options_data["realized_vol_30d"] = round(rv_30d, 2) if rv_30d else None
        options_data["term_structure"]   = term_struct

    # Concentration
    concentration = extract_concentration_signals(sec_map.get(ticker, {}))

    # Squeeze proxy score
    sq_score = compute_squeeze_score(
        float_tier_bonus=float_pts,
        rotation_accel=rotation_accel,
        options_data=options_data,
        atr_pct=atr_pct,
        concentration_count=len(concentration),
    )

    return {
        "ticker":          ticker,
        "pump_likelihood": cand.get("pump_likelihood"),
        "pump_category":   cand.get("pump_category"),

        "squeeze_profile": {
            "shares_outstanding":  shares_out,
            "float_tier":          float_tier,
            "float_tier_bonus":    float_pts,
            "rotation_today":      round(rotation_today, 4) if rotation_today else None,
            "rotation_20d_avg":    round(rotation_20d, 4) if rotation_20d else None,
            "rotation_accel":      round(rotation_accel, 2) if rotation_accel else None,
            "atr_14":              round(atr_14, 2) if atr_14 else None,
            "atr_pct":             round(atr_pct, 2) if atr_pct else None,
            "concentration_signals": concentration,
            "shorts_covering_flag": options_data.get("shorts_covering", False),
            "squeeze_proxy_score": sq_score,
            "squeeze_potential":   classify_squeeze_potential(sq_score),
            "squeeze_caveat":      ("Score is a PROXY built from float / rotation / options-flow / "
                                      "concentration. Direct SI requires FINRA Reg SHO (manual setup). "
                                      "Use as relative ranker, not absolute SI %."),
        },

        "options_structure": options_data,
    }


# ═════════════════════════════════════════════════════════════════════
# Lambda handler
# ═════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    t0 = time.time()
    print(f"[mechanics] start {datetime.now(timezone.utc).isoformat()}")

    try:
        radar = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=RADAR_KEY)["Body"].read())
    except Exception as e:
        return _write_error(f"Failed to load radar: {e}")
    candidates = (radar.get("pump_candidates") or [])[:12]
    if not candidates:
        return _write_error("No pump candidates in radar")

    options_map = load_options_flow_data()
    sec_map = load_sec_filings_map()

    print(f"[mechanics] {len(candidates)} candidates · "
          f"options-flow has {len(options_map)} tickers · "
          f"sec-filings has {len(sec_map)} tickers")

    enriched = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(enrich_candidate, c, options_map, sec_map): c
                    for c in candidates}
        for fut in as_completed(futures, timeout=120):
            try:
                enriched.append(fut.result())
            except Exception as e:
                cand = futures[fut]
                print(f"[enrich] {cand['ticker']}: {e}")
                enriched.append({**cand, "err": str(e)[:120]})

    # Sort by squeeze proxy score descending (highest pump+squeeze potential first)
    enriched.sort(key=lambda r: -(r.get("squeeze_profile", {}).get("squeeze_proxy_score", 0) or 0))

    output = {
        "schema_version": "1.0",
        "generated_at":   datetime.now(timezone.utc).isoformat(),
        "elapsed_sec":    round(time.time() - t0, 2),
        "n_candidates":   len(enriched),
        "candidates":     enriched,
        "metadata": {
            "squeeze_score_caveat": ("Proxy score — direct SI %/days-to-cover requires "
                                       "FINRA Reg SHO data (KHALID_ACTIONS #2). Use as relative ranker."),
            "options_data_source": "Existing options-flow engine — TIER/flags + locally computed IV rank proxy from ATR range.",
            "components": {
                "float_tier_weight":         30,
                "rotation_accel_weight":     25,
                "options_signals_weight":    20,
                "atr_pct_weight":            15,
                "concentration_weight":      10,
            },
        },
    }

    body = json.dumps(output, indent=2, default=str)
    s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY, Body=body,
                    ContentType="application/json", CacheControl="max-age=600")
    archive_key = (f"data/archive/pump-mechanics/"
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
        "top_squeeze":     [c["ticker"] for c in enriched[:5]],
        "n_primed":        sum(1 for c in enriched if c.get("squeeze_profile", {}).get("squeeze_potential") == "SQUEEZE_PRIMED"),
    }
    print(f"[mechanics] done: {summary}")
    return {"statusCode": 200, "body": json.dumps(summary)}


def _write_error(message: str, **extras) -> dict:
    payload = {"schema_version": "1.0", "generated_at": datetime.now(timezone.utc).isoformat(),
                "status": "error", "error": message, **extras}
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
                        Body=json.dumps(payload, default=str, indent=2),
                        ContentType="application/json", CacheControl="max-age=300")
    except Exception: pass
    print(f"[mechanics] ERROR: {message}")
    return {"statusCode": 500, "body": json.dumps({"status": "error", "error": message})}
