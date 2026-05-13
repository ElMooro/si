"""
justhodl-sector-rotation v1.0.0 — Roadmap #4 SECTOR ROTATION & MONEY FLOW
═════════════════════════════════════════════════════════════════════════════
Institutional-grade sector rotation detection. Designed to identify
leadership changes BEFORE they become obvious in price.

THE 9 INSTITUTIONAL SIGNALS (composite-scored 0-100 per sector)
───────────────────────────────────────────────────────────────
  1. RS vs SPY (level + slope + 2nd derivative acceleration)
  2. Chaikin Money Flow (20)        — buying/selling pressure
  3. On-Balance Volume slope (10)    — accumulation/distribution
  4. Money Flow Index (14)            — volume-weighted RSI
  5. Position vs 20/50/200 DMA       — trend structure
  6. Volume surge ratio              — institutional thrust
  7. 14 Cross-sector ratios          — risk-on/off composite
  8. Regime-expected leadership      — cycle-phase mapping
  9. Sub-sector leader confirmation  — earliest signal

EARLY-ROTATION DETECTION
────────────────────────
Sector flagged "ROTATING IN" when 3+ of these fire simultaneously:
  ✓ RS acceleration > 0 (sector outperforming at increasing rate)
  ✓ CMF > 0.10 (net buying pressure)
  ✓ Volume surge ≥ 1.5× with up close (institutional thrust)
  ✓ Golden cross within last 5 days (50DMA crosses above 200DMA)
  ✓ Sub-sector leader spread > 3% (e.g. SMH outpacing XLK)
  ✓ MFI > 60 (bullish volume-confirmed momentum)
"""
import json
import math
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

VERSION = "1.0.0"

S3_BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/sector-rotation.json"
ANOMALIES_KEY = "signals/anomalies.json"

POLY_KEY = os.environ.get("POLY_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
HTTP_TIMEOUT = 10
LOOKBACK_DAYS = 260

# ─── SECTOR ETFs ───
SECTORS = {
    "XLK": {"name": "Technology", "cycle_fit": ["EXPANSION-EARLY", "EXPANSION-MID"]},
    "XLF": {"name": "Financials", "cycle_fit": ["EXPANSION-EARLY", "EXPANSION-MID"]},
    "XLV": {"name": "Healthcare", "cycle_fit": ["CONTRACTION", "EXPANSION-LATE"]},
    "XLY": {"name": "Consumer Discretionary", "cycle_fit": ["EXPANSION-EARLY"]},
    "XLP": {"name": "Consumer Staples", "cycle_fit": ["CONTRACTION", "EXPANSION-LATE"]},
    "XLE": {"name": "Energy", "cycle_fit": ["EXPANSION-MID", "EXPANSION-LATE"]},
    "XLI": {"name": "Industrials", "cycle_fit": ["EXPANSION-MID"]},
    "XLU": {"name": "Utilities", "cycle_fit": ["CONTRACTION"]},
    "XLB": {"name": "Materials", "cycle_fit": ["EXPANSION-MID", "EXPANSION-LATE"]},
    "XLRE": {"name": "Real Estate", "cycle_fit": ["EXPANSION-EARLY", "CONTRACTION"]},
    "XLC": {"name": "Communications", "cycle_fit": ["EXPANSION-EARLY", "EXPANSION-MID"]},
}

# Sub-sector leaders
SUB_SECTORS = {
    "XLK": "SMH",   # Semis lead tech
    "XLV": "XBI",   # Biotech leads healthcare
    "XLF": "KRE",   # Regional banks lead financials
    "XLE": "XOP",   # E&P leads energy
}

BROAD = ["SPY", "QQQ", "IWM", "DIA"]
CROSS_ASSET = ["TLT", "IEF", "HYG", "LQD", "GLD", "SLV", "DBC", "USO", "UUP"]
ALL_TICKERS = (list(SECTORS.keys()) + list(SUB_SECTORS.values()) + BROAD + CROSS_ASSET)

# Cross-sector ratios — institutional staples
RATIO_PAIRS = [
    ("XLF", "XLU", "Financials/Utilities",
     "Rate-hike pricing · risk-on", "Rate-cut/defensive pricing · risk-off"),
    ("XLY", "XLP", "Discretionary/Staples",
     "Consumer risk appetite up · cyclical", "Defensive consumer · late-cycle"),
    ("XLK", "XLE", "Tech/Energy",
     "Growth dominating · low inflation", "Value/inflation regime · energy leading"),
    ("IWM", "SPY", "Small-Cap/Large-Cap",
     "Small-caps leading · risk-on broadening", "Mega-cap concentration · risk-off"),
    ("QQQ", "SPY", "Growth/Broad",
     "Growth premium expanding", "Growth premium compressing"),
    ("SMH", "XLK", "Semis/Tech",
     "Cycle leaders pulling tech up", "Cycle leaders failing · tech rotation done"),
    ("XBI", "XLV", "Biotech/Healthcare",
     "High-beta healthcare leading", "Defensive healthcare leading"),
    ("KRE", "XLF", "Regional/Broad Financials",
     "Credit-cycle expansion · regional confidence", "Credit-cycle stress · regional weakness"),
    ("HYG", "IEF", "HY Credit/Treasuries",
     "Credit risk appetite up · risk-on", "Credit-spread widening · risk-off"),
    ("TLT", "SPY", "Long Bonds/Stocks",
     "Bonds > stocks · risk-off", "Stocks > bonds · risk-on"),
    ("GLD", "SPY", "Gold/Stocks",
     "Safe-haven flow · uncertainty", "Risk-on · gold unloved"),
    ("DBC", "SPY", "Commodities/Stocks",
     "Inflation regime · real-asset leadership", "Disinflation · financial-asset leadership"),
    ("XLB", "XLU", "Materials/Utilities",
     "Industrial cycle strength", "Cyclical weakness · defensive"),
    ("USO", "SPY", "Oil/Stocks",
     "Oil thrust · supply or demand stress", "Oil weak vs equities"),
]

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


# ═══════════════════════════════════════════════════════════════════════════
# POLYGON FETCH
# ═══════════════════════════════════════════════════════════════════════════

def fetch_polygon_bars(symbol, lookback_days=LOOKBACK_DAYS):
    if not POLY_KEY: return None
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=int(lookback_days * 1.5))
    url = (f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/"
            f"{start.isoformat()}/{end.isoformat()}"
            f"?adjusted=true&sort=asc&limit=5000&apiKey={POLY_KEY}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Rotation/1.0"})
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            data = json.loads(r.read().decode("utf-8"))
        return [{"t": b["t"] // 1000, "o": float(b["o"]), "h": float(b["h"]),
                  "l": float(b["l"]), "c": float(b["c"]), "v": float(b["v"])}
                 for b in (data.get("results") or [])]
    except Exception as e:
        print(f"  fetch {symbol} err: {str(e)[:120]}")
        return None


def batch_fetch_bars(symbols, lookback_days=LOOKBACK_DAYS, workers=12):
    out = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(fetch_polygon_bars, s, lookback_days): s for s in symbols}
        for f in as_completed(futures):
            sym = futures[f]
            try:
                bars = f.result()
                if bars: out[sym] = bars
            except Exception as e:
                print(f"  batch {sym} err: {str(e)[:80]}")
    return out


# ═══════════════════════════════════════════════════════════════════════════
# PURE-PYTHON INDICATORS
# ═══════════════════════════════════════════════════════════════════════════

def mean(xs): return sum(xs) / len(xs) if xs else None
def stdev(xs):
    if len(xs) < 2: return None
    m = mean(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))

def z_score(value, history):
    if not history or len(history) < 10: return None
    m, s = mean(history), stdev(history)
    if not s or s == 0: return None
    return round((value - m) / s, 3)

def pct_rank(value, history):
    if not history: return None
    return round(100 * sum(1 for h in history if h < value) / len(history), 1)

def rolling_returns_pct(closes, window):
    if len(closes) < window + 1: return None
    return round((closes[-1] / closes[-1 - window] - 1) * 100, 2)

def slope_linear(xs, ys):
    n = len(xs)
    if n < 2: return None
    mx, my = mean(xs), mean(ys)
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den = sum((x - mx) ** 2 for x in xs)
    return num / den if den != 0 else None

def sma(values, window):
    if len(values) < window: return None
    return mean(values[-window:])

def sma_series(values, window):
    if len(values) < window: return []
    return [mean(values[i - window + 1:i + 1]) for i in range(window - 1, len(values))]

def position_vs_ma(close, ma_value):
    if ma_value is None or ma_value == 0: return None
    return round((close / ma_value - 1) * 100, 2)


def chaikin_money_flow(bars, period=20):
    if len(bars) < period: return None
    mfv, vol = 0.0, 0.0
    for b in bars[-period:]:
        rng = b["h"] - b["l"]
        if rng == 0: continue
        mf_mult = ((b["c"] - b["l"]) - (b["h"] - b["c"])) / rng
        mfv += mf_mult * b["v"]
        vol += b["v"]
    return round(mfv / vol, 4) if vol > 0 else None


def obv_slope(bars, window=10):
    if len(bars) < window + 1: return None
    obv = [0.0]
    for i in range(1, len(bars)):
        if bars[i]["c"] > bars[i-1]["c"]: obv.append(obv[-1] + bars[i]["v"])
        elif bars[i]["c"] < bars[i-1]["c"]: obv.append(obv[-1] - bars[i]["v"])
        else: obv.append(obv[-1])
    recent = obv[-window:]
    sl = slope_linear(list(range(window)), recent)
    if sl is None: return None
    avg = mean([abs(v) for v in recent]) or 1
    return round(sl / max(avg, 1), 6)


def money_flow_index(bars, period=14):
    if len(bars) < period + 1: return None
    pos, neg = 0.0, 0.0
    for i in range(len(bars) - period, len(bars)):
        if i == 0: continue
        tp_now = (bars[i]["h"] + bars[i]["l"] + bars[i]["c"]) / 3
        tp_prev = (bars[i-1]["h"] + bars[i-1]["l"] + bars[i-1]["c"]) / 3
        mf = tp_now * bars[i]["v"]
        if tp_now > tp_prev: pos += mf
        elif tp_now < tp_prev: neg += mf
    if neg == 0: return 100.0 if pos > 0 else 50.0
    return round(100 - (100 / (1 + pos / neg)), 1)


def volume_surge(bars, period=20):
    if len(bars) < period + 1: return None
    today = bars[-1]["v"]
    avg = mean([b["v"] for b in bars[-period - 1:-1]])
    return round(today / avg, 2) if avg > 0 else None


def relative_strength_metrics(sector_closes, spy_closes):
    if len(sector_closes) < 60 or len(spy_closes) < 60: return None
    n = min(len(sector_closes), len(spy_closes))
    sector_closes = sector_closes[-n:]; spy_closes = spy_closes[-n:]
    rs_ratio = [s / sp for s, sp in zip(sector_closes, spy_closes)]
    out = {}
    for label, days in [("1d", 1), ("5d", 5), ("1m", 21), ("3m", 63), ("6m", 126)]:
        if len(rs_ratio) > days:
            rs = (sector_closes[-1] / sector_closes[-1 - days] - 1) * 100
            rsp = (spy_closes[-1] / spy_closes[-1 - days] - 1) * 100
            out[f"rs_{label}_pct"] = round(rs - rsp, 2)
            out[f"return_{label}_pct"] = round(rs, 2)
            out[f"spy_return_{label}_pct"] = round(rsp, 2)
    if len(rs_ratio) >= 21:
        recent = rs_ratio[-21:]
        sl = slope_linear(list(range(21)), recent)
        if sl is not None:
            out["rs_slope_21d_pct_per_day"] = round((sl / max(recent[0], 1e-9)) * 100, 4)
    if len(rs_ratio) >= 21:
        sl_recent = slope_linear(list(range(10)), rs_ratio[-10:])
        sl_prior = slope_linear(list(range(10)), rs_ratio[-21:-11])
        if sl_recent is not None and sl_prior is not None:
            out["rs_acceleration"] = round(sl_recent - sl_prior, 6)
    if len(rs_ratio) >= 200:
        out["rs_pct_rank_1y"] = pct_rank(rs_ratio[-1], rs_ratio[-252:])
    return out


# ═══════════════════════════════════════════════════════════════════════════
# REGIME → CYCLE MAPPING
# ═══════════════════════════════════════════════════════════════════════════

def map_macro_to_cycle(mss, regime_label):
    if mss is None: return "UNKNOWN"
    if mss >= 80: return "CONTRACTION"
    if mss >= 60: return "EXPANSION-LATE"
    if mss >= 35: return "EXPANSION-MID"
    if mss < 20: return "EXPANSION-EARLY"
    return "EXPANSION-MID"

def expected_leaders(cycle_phase):
    return [sym for sym, meta in SECTORS.items() if cycle_phase in (meta.get("cycle_fit") or [])]


# ═══════════════════════════════════════════════════════════════════════════
# CROSS-SECTOR RATIO
# ═══════════════════════════════════════════════════════════════════════════

def analyze_ratio(num_bars, den_bars, label, interp_pos, interp_neg):
    if not num_bars or not den_bars: return None
    den_by_t = {b["t"]: b for b in den_bars}
    aligned = [(b, den_by_t[b["t"]]) for b in num_bars if b["t"] in den_by_t]
    if len(aligned) < 60: return None
    ratios = [n["c"] / d["c"] for n, d in aligned]
    current = ratios[-1]
    sma20 = sma(ratios, 20); sma50 = sma(ratios, 50)
    sma200 = sma(ratios, 200) if len(ratios) >= 200 else None
    ret_5d = rolling_returns_pct(ratios, 5)
    ret_21d = rolling_returns_pct(ratios, 21)
    ret_63d = rolling_returns_pct(ratios, 63)
    history_1y = ratios[-252:] if len(ratios) >= 252 else ratios
    z = z_score(current, history_1y)
    pct = pct_rank(current, history_1y)
    direction = "rising" if ret_21d and ret_21d > 0 else "falling" if ret_21d and ret_21d < 0 else "flat"
    return {
        "label": label, "current": round(current, 4),
        "ret_5d_pct": ret_5d, "ret_21d_pct": ret_21d, "ret_63d_pct": ret_63d,
        "sma20": round(sma20, 4) if sma20 else None,
        "sma50": round(sma50, 4) if sma50 else None,
        "sma200": round(sma200, 4) if sma200 else None,
        "pct_vs_sma20": position_vs_ma(current, sma20),
        "pct_vs_sma50": position_vs_ma(current, sma50),
        "pct_vs_sma200": position_vs_ma(current, sma200),
        "z_score_1y": z, "pct_rank_1y": pct, "direction": direction,
        "above_sma50": (sma50 is not None and current > sma50),
        "above_sma200": (sma200 is not None and current > sma200),
        "interp_active": interp_pos if direction == "rising" else interp_neg,
    }


# ═══════════════════════════════════════════════════════════════════════════
# PER-SECTOR ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════

def analyze_sector(symbol, bars, spy_bars, all_bars, cycle_phase):
    if not bars or len(bars) < 60:
        return {"symbol": symbol, "err": "insufficient bars"}
    meta = SECTORS.get(symbol, {})
    closes = [b["c"] for b in bars]
    spy_closes = [b["c"] for b in spy_bars] if spy_bars else []
    current = closes[-1]

    sma20 = sma(closes, 20); sma50 = sma(closes, 50)
    sma200 = sma(closes, 200) if len(closes) >= 200 else None
    golden = (sma50 is not None and sma200 is not None and sma50 > sma200)
    death = (sma50 is not None and sma200 is not None and sma50 < sma200)

    rs = relative_strength_metrics(closes, spy_closes) if spy_closes else {}
    cmf = chaikin_money_flow(bars, 20)
    obv_sl = obv_slope(bars, 10)
    mfi = money_flow_index(bars, 14)
    vol_surge = volume_surge(bars, 20)

    # Sub-sector confirmation
    sub_sym = SUB_SECTORS.get(symbol)
    sub_confirm = None
    if sub_sym and sub_sym in all_bars:
        sub_bars = all_bars[sub_sym]
        sub_closes = [b["c"] for b in sub_bars]
        if len(sub_closes) >= 21 and len(closes) >= 21:
            sub_21d = rolling_returns_pct(sub_closes, 21)
            sec_21d = rolling_returns_pct(closes, 21)
            if sub_21d is not None and sec_21d is not None:
                spread = sub_21d - sec_21d
                sub_confirm = {
                    "sub_symbol": sub_sym, "sub_21d_pct": sub_21d, "sec_21d_pct": sec_21d,
                    "leadership_spread": round(spread, 2),
                    "leader_confirms": spread > 0,
                    "leader_diverges": spread < -2.0,
                }

    in_cycle = cycle_phase in (meta.get("cycle_fit") or [])

    # ROTATION SCORE COMPOSITE
    sc = {}
    if rs:
        rs_3m = rs.get("rs_3m_pct") or 0
        rs_1m = rs.get("rs_1m_pct") or 0
        rs_accel = rs.get("rs_acceleration") or 0
        rs_score = 50 + (rs_3m * 2.5) + (rs_1m * 1.0) + (rs_accel * 10000)
        sc["rs_momentum"] = round(max(0, min(100, rs_score)), 1)
    else:
        sc["rs_momentum"] = 50

    mf_pieces = []
    if cmf is not None: mf_pieces.append(50 + cmf * 200)
    if obv_sl is not None: mf_pieces.append(50 + obv_sl * 5e6)
    if mfi is not None: mf_pieces.append(mfi)
    sc["money_flow"] = round(max(0, min(100, mean(mf_pieces) if mf_pieces else 50)), 1)

    br = []
    if sma20: br.append(50 + min(50, max(-50, position_vs_ma(current, sma20) * 5)))
    if sma50: br.append(50 + min(50, max(-50, position_vs_ma(current, sma50) * 3)))
    if sma200: br.append(50 + min(50, max(-50, position_vs_ma(current, sma200) * 2)))
    if vol_surge is not None: br.append(min(100, vol_surge * 30))
    sc["breadth"] = round(max(0, min(100, mean(br) if br else 50)), 1)

    sc["regime_fit"] = 75 if in_cycle else 30

    if sub_confirm:
        sub_score = 50 + min(50, max(-50, sub_confirm["leadership_spread"] * 5))
        sc["sub_sector"] = round(max(0, min(100, sub_score)), 1)
    else:
        sc["sub_sector"] = 50

    rotation_score = round(
        0.30 * sc["rs_momentum"] + 0.25 * sc["money_flow"]
        + 0.20 * sc["breadth"] + 0.15 * sc["regime_fit"]
        + 0.10 * sc["sub_sector"], 1
    )

    # Rotation flags
    flags_in = []
    if rs and (rs.get("rs_acceleration") or 0) > 0: flags_in.append("RS_ACCELERATING")
    if cmf is not None and cmf > 0.10: flags_in.append("BUYING_PRESSURE")
    if vol_surge and vol_surge >= 1.5 and bars[-1]["c"] > bars[-1]["o"]: flags_in.append("VOLUME_THRUST")
    if sma50 and sma200 and len(closes) > 200:
        s50 = sma_series(closes, 50); s200 = sma_series(closes, 200)
        if (len(s50) >= 5 and len(s200) >= 5 and s50[-6] <= s200[-6] and s50[-1] > s200[-1]):
            flags_in.append("GOLDEN_CROSS_5D")
    if sub_confirm and sub_confirm["leader_confirms"] and sub_confirm["leadership_spread"] > 3:
        flags_in.append("SUB_SECTOR_LEADING")
    if mfi is not None and mfi > 60: flags_in.append("MFI_BULLISH")

    flags_out = []
    if rs and (rs.get("rs_acceleration") or 0) < 0: flags_out.append("RS_DECELERATING")
    if cmf is not None and cmf < -0.05: flags_out.append("DISTRIBUTION")
    if vol_surge and vol_surge >= 1.5 and bars[-1]["c"] < bars[-1]["o"]: flags_out.append("DOWN_VOLUME_THRUST")
    if mfi is not None and mfi < 35: flags_out.append("MFI_BEARISH")
    if sub_confirm and sub_confirm.get("leader_diverges"): flags_out.append("SUB_SECTOR_FALLING")

    return {
        "symbol": symbol, "name": meta.get("name", symbol),
        "current_price": round(current, 2),
        "cycle_fit_phases": meta.get("cycle_fit", []),
        "in_current_cycle": in_cycle,
        "rotation_score": rotation_score, "score_components": sc,
        **(rs or {}),
        "sma20": round(sma20, 2) if sma20 else None,
        "sma50": round(sma50, 2) if sma50 else None,
        "sma200": round(sma200, 2) if sma200 else None,
        "pct_vs_sma20": position_vs_ma(current, sma20),
        "pct_vs_sma50": position_vs_ma(current, sma50),
        "pct_vs_sma200": position_vs_ma(current, sma200),
        "golden_cross_active": golden, "death_cross_active": death,
        "chaikin_money_flow_20": cmf, "obv_slope_10d": obv_sl,
        "money_flow_index_14": mfi, "volume_surge_ratio": vol_surge,
        "sub_sector_check": sub_confirm,
        "rotation_in_flags": flags_in, "rotation_out_flags": flags_out,
        "rotating_in": len(flags_in) >= 3, "rotating_out": len(flags_out) >= 3,
    }


def risk_appetite_composite(ratios):
    key_ratios = ["Financials/Utilities", "Discretionary/Staples",
                   "HY Credit/Treasuries", "Small-Cap/Large-Cap"]
    scores = []
    for label in key_ratios:
        r = next((r for r in ratios if r and r.get("label") == label), None)
        if r is None: continue
        ret = r.get("ret_21d_pct") or 0
        scores.append(max(0, min(100, 50 + ret * 6)))
    if not scores: return None
    composite = mean(scores)
    label = ("STRONG RISK-ON" if composite > 70 else
              "RISK-ON" if composite > 55 else
              "NEUTRAL" if composite > 45 else
              "RISK-OFF" if composite > 30 else
              "STRONG RISK-OFF")
    return {"score": round(composite, 1), "label": label, "components": len(scores)}


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
    print(f"=== SECTOR ROTATION v{VERSION} · {datetime.now(timezone.utc).isoformat()} ===")

    print(f"  fetching {len(ALL_TICKERS)} ETFs...")
    all_bars = batch_fetch_bars(ALL_TICKERS, lookback_days=LOOKBACK_DAYS, workers=12)
    print(f"  ✓ {len(all_bars)}/{len(ALL_TICKERS)} loaded")

    spy_bars = all_bars.get("SPY") or []
    if not spy_bars:
        return {"statusCode": 500, "body": json.dumps({"err": "SPY data unavailable"})}

    # Determine cycle phase
    cycle_phase = "EXPANSION-MID"; mss = None; regime_label = "NORMAL"
    try:
        anomalies = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=ANOMALIES_KEY)["Body"].read())
        mss = anomalies.get("macro_stress_score")
        interp = anomalies.get("stress_interpretation", "")
        for token in ["Goldilocks", "Normal", "Elevated", "High", "Crisis"]:
            if token.upper() in interp.upper(): regime_label = token.upper(); break
        cycle_phase = map_macro_to_cycle(mss, regime_label)
    except Exception as e:
        print(f"  anomalies load failed: {str(e)[:120]}")
    print(f"  macro: MSS={mss} · regime={regime_label} · cycle={cycle_phase}")

    # Per-sector
    sectors_out = []
    for sym in SECTORS:
        try:
            sectors_out.append(analyze_sector(sym, all_bars.get(sym), spy_bars, all_bars, cycle_phase))
        except Exception as e:
            print(f"  sector {sym} err: {str(e)[:120]}")
            sectors_out.append({"symbol": sym, "err": str(e)[:120]})

    valid = [s for s in sectors_out if not s.get("err")]
    valid.sort(key=lambda s: -(s.get("rotation_score") or 0))
    for i, s in enumerate(valid): s["rank"] = i + 1

    # Cross-sector ratios
    ratios = []
    for num, den, lbl, p, n in RATIO_PAIRS:
        try:
            r = analyze_ratio(all_bars.get(num), all_bars.get(den), lbl, p, n)
            if r: r["numerator"] = num; r["denominator"] = den; ratios.append(r)
        except Exception as e: print(f"  ratio {num}/{den} err: {str(e)[:120]}")

    risk_appetite = risk_appetite_composite(ratios)
    leaders = valid[:3]; laggards = valid[-3:]
    rotating_in = [s for s in valid if s.get("rotating_in")]
    rotating_out = [s for s in valid if s.get("rotating_out")]

    expected = expected_leaders(cycle_phase)
    actual_top = [s["symbol"] for s in leaders]
    expected_matching = [e for e in expected if e in actual_top]
    expected_missing = [e for e in expected if e not in actual_top]
    leadership_alignment = {
        "expected_for_cycle": expected, "actual_top_3": actual_top,
        "matched": expected_matching, "missing_expected": expected_missing,
        "alignment_pct": round(100 * len(expected_matching) / len(expected), 1) if expected else None,
    }

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "version": VERSION,
        "elapsed_seconds": round(time.time() - started, 2),
        "macro_context": {
            "macro_stress_score": mss, "regime_label": regime_label,
            "cycle_phase": cycle_phase, "expected_leaders": expected,
        },
        "risk_appetite": risk_appetite,
        "summary": {
            "n_sectors_analyzed": len(valid),
            "n_rotating_in": len(rotating_in), "n_rotating_out": len(rotating_out),
            "top_3_leaders": [{"sym": s["symbol"], "score": s["rotation_score"]} for s in leaders],
            "bottom_3_laggards": [{"sym": s["symbol"], "score": s["rotation_score"]} for s in laggards],
            "leadership_alignment": leadership_alignment,
        },
        "sectors": valid,
        "ratios": ratios,
        "rotation_alerts": {
            "rotating_in": [{"sym": s["symbol"], "score": s["rotation_score"],
                              "flags": s.get("rotation_in_flags")} for s in rotating_in],
            "rotating_out": [{"sym": s["symbol"], "score": s["rotation_score"],
                                "flags": s.get("rotation_out_flags")} for s in rotating_out],
        },
    }

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
            Body=json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8"),
            ContentType="application/json", CacheControl="public, max-age=900")
        print(f"  ✓ sector-rotation.json written")
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"err": f"put: {e}"})}

    alert_sent = False
    if (len(rotating_in) > 0 or len(rotating_out) > 0) and risk_appetite:
        chat_id = get_chat_id()
        if chat_id:
            lines = [f"🔄 *Sector Rotation Alert · {datetime.now(timezone.utc).strftime('%b %d')}*\n",
                      f"📊 Risk Appetite: *{risk_appetite['label']}* ({risk_appetite['score']}/100)",
                      f"🌐 Cycle: {cycle_phase}\n"]
            if rotating_in:
                lines.append("📈 *ROTATING IN:*")
                for s in rotating_in[:5]:
                    lines.append(f"  • {s['symbol']} ({s['name']}) · score {s['rotation_score']}")
                    lines.append(f"    flags: {', '.join(s.get('rotation_in_flags', []))}")
            if rotating_out:
                lines.append("\n📉 *ROTATING OUT:*")
                for s in rotating_out[:5]:
                    lines.append(f"  • {s['symbol']} ({s['name']}) · score {s['rotation_score']}")
                    lines.append(f"    flags: {', '.join(s.get('rotation_out_flags', []))}")
            lines.append("\n[Rotation Dashboard](https://justhodl.ai/rotation/)")
            try: alert_sent = send_telegram("\n".join(lines), chat_id)
            except Exception as e: print(f"  alert err: {e}")

    return {"statusCode": 200, "body": json.dumps({
        "success": True, "version": VERSION,
        "n_sectors": len(valid), "n_ratios": len(ratios),
        "n_rotating_in": len(rotating_in), "n_rotating_out": len(rotating_out),
        "risk_appetite_score": risk_appetite["score"] if risk_appetite else None,
        "risk_appetite_label": risk_appetite["label"] if risk_appetite else None,
        "cycle_phase": cycle_phase,
        "top_leader": leaders[0]["symbol"] if leaders else None,
        "alert_sent": alert_sent,
        "elapsed_seconds": round(time.time() - started, 2),
    })}
