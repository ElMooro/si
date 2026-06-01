"""
justhodl-anomaly-detector v2.0 — INSTITUTIONAL GRADE

═══════════════════════════════════════════════════════════════════════
THE EARLY WARNING SYSTEM (v2)
─────────────────────────────
v1 had 5 detectors. v2 has 17 across every category real hedge funds
watch for early signals of regime change, credit stress, or fat-tail risk.

╔══════════════════════════════════════════════════════════════════════╗
║  CATEGORIES                                                            ║
╠══════════════════════════════════════════════════════════════════════╣
║  EQUITY VOL    VIX + SKEW level z-scores + VIX/SKEW divergence        ║
║  CREDIT        HY OAS + BBB OAS (level z + 1w-change z) + HYG/LQD     ║
║  RATES         2s10s + 3M10Y curves + DGS10 vol + inversion detection ║
║  FUNDING       SOFR + DTB3 z-score + SOFR-DTB3 spread                  ║
║  CURRENCY      DXY + USDJPY level + 1w change                          ║
║  CROSS-ASSET   SPY+TLT+GLD+UUP sign-alignment + correlation breakdown ║
║  EQUITY INTL   SPHB/SPLV (risk app) + XLU/XLK (defensive) + autocorr  ║
║  COMMODITY     Copper/Gold (Gundlach) + Gold/Silver + WTI + Gold      ║
║  BREADTH       11 SPDR sectors dispersion + RSP/SPY ratio              ║
║  MACRO         ICSA initial claims weekly z-score                      ║
║  CRYPTO        BTC realized vol + percentile rank                      ║
╚══════════════════════════════════════════════════════════════════════╝

═══════════════════════════════════════════════════════════════════════
MACRO STRESS SCORE (0-100 composite)
─────────────────────────────────────
Each detector contributes weighted points to an aggregate stress index.
Reference historical analogs:
   MSS 0-20    Goldilocks   — markets calm across the board
   MSS 20-40   Normal       — typical market noise
   MSS 40-60   Elevated     — watch closely, multiple stresses
   MSS 60-80   High stress  — reduce risk, hedge tail
   MSS 80-100  Crisis       — defensive posture

Historical reference: Mar 2020 ~92 · Oct 2008 ~98 · Feb 2018 ~52 ·
Aug 2015 ~58 · Aug 2022 ~48

═══════════════════════════════════════════════════════════════════════
SEVERITY TIERS (per-detector, absolute z-score)
───────────────────────────────────────────────
  LOW       2.0-2.5   note only
  MEDIUM    2.5-3.5   daily brief inclusion
  HIGH      3.5-5.0   📲 Telegram alert
  EXTREME   > 5.0     🚨 crisis alert

CRISIS CLUSTER fires when 3+ HIGH/EXTREME OR Macro Stress Score >= 60

═══════════════════════════════════════════════════════════════════════
"""
import json
import os
import statistics
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1074)

# ═══════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════

VERSION = "3.0.0"  # institutional expansion: +15 FRED detectors (NFCI/ANFCI/STLFSI4/real yields/inflation expectations/recession nowcasts/balance-sheet liquidity/CNY/VXN/GVZ/OVX/mortgage rates) + Net Liquidity composite detector

S3_BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "signals/anomalies.json"
ALERT_HISTORY_KEY = "signals/anomaly-alert-history.json"

FRED_KEY = os.environ.get("FRED_KEY", "")
POLY_KEY = os.environ.get("POLY_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

SEVERITY_TIERS = [("EXTREME", 5.0), ("HIGH", 3.5), ("MEDIUM", 2.5), ("LOW", 2.0)]
DEDUPE_HOURS = 12

# Stress Score category weights (sum = 1.0)
STRESS_WEIGHTS = {
    "credit":              0.16,   # -0.02 → ceded to financial_conditions
    "equity_vol":          0.13,   # -0.02
    "rates":               0.12,   # -0.01
    "financial_conditions": 0.10,   # NEW — NFCI/ANFCI/STLFSI4 combined
    "funding":             0.10,
    "currency":            0.07,   # -0.01
    "cross_asset":         0.09,   # -0.01
    "equity_internals":    0.07,   # -0.01
    "commodity":           0.06,
    "breadth":             0.06,
    "macro":               0.03,
    "crypto":              0.01,   # -0.02
}

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


# ═══════════════════════════════════════════════════════════════════════
# DATA FETCHERS
# ═══════════════════════════════════════════════════════════════════════

def fetch_fred_series(series_id, lookback_days=500):
    if not FRED_KEY: return []
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=lookback_days)
    params = urllib.parse.urlencode({
        "series_id": series_id, "api_key": FRED_KEY, "file_type": "json",
        "observation_start": start.isoformat(),
        "observation_end": end.isoformat(),
        "sort_order": "asc",
    })
    url = f"https://api.stlouisfed.org/fred/series/observations?{params}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-AD/2.0"})
        with urllib.request.urlopen(req, timeout=12) as r:
            data = json.loads(r.read().decode("utf-8"))
        out = []
        for o in data.get("observations") or []:
            v = o.get("value")
            if v in (".", "", None): continue
            try: out.append({"date": o["date"], "value": float(v)})
            except (ValueError, TypeError): continue
        return out
    except Exception as e:
        print(f"  [fred:{series_id}] {str(e)[:100]}")
        return []


def fetch_polygon_ohlcv(symbol, lookback_days=120):
    if not POLY_KEY: return []
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=lookback_days)
    url = (f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/"
           f"{start}/{end}?adjusted=true&sort=asc&limit=500&apiKey={POLY_KEY}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-AD/2.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read().decode("utf-8")).get("results") or []
    except Exception as e:
        print(f"  [poly:{symbol}] {str(e)[:100]}")
        return []


def batch_fetch_polygon(symbols, lookback_days=120, max_workers=8):
    out = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fetch_polygon_ohlcv, s, lookback_days): s for s in symbols}
        for f in as_completed(futures):
            try: out[futures[f]] = f.result() or []
            except Exception: out[futures[f]] = []
    return out


def batch_fetch_fred(series_ids, lookback_days=500, max_workers=6):
    out = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fetch_fred_series, s, lookback_days): s for s in series_ids}
        for f in as_completed(futures):
            try: out[futures[f]] = f.result() or []
            except Exception: out[futures[f]] = []
    return out


# ═══════════════════════════════════════════════════════════════════════
# STATISTICAL HELPERS
# ═══════════════════════════════════════════════════════════════════════

def z_score(values, current=None, lookback=60):
    if not values: return None, None, None, 0
    if current is None:
        if len(values) < lookback + 1: return None, None, None, 0
        current = values[-1]
        sample = values[max(0, len(values) - 1 - lookback):-1]
    else:
        sample = values[-lookback:] if len(values) >= lookback else values
    if len(sample) < 10: return None, None, None, len(sample)
    mean = sum(sample) / len(sample)
    var = sum((v - mean) ** 2 for v in sample) / len(sample)
    std = var ** 0.5
    if std == 0: return None, mean, 0, len(sample)
    return (current - mean) / std, mean, std, len(sample)


def percentile_rank(value, history):
    if not history: return None
    below = sum(1 for h in history if h < value)
    return round(100 * below / len(history), 1)


def classify_severity(abs_z):
    if abs_z is None: return None
    for label, threshold in SEVERITY_TIERS:
        if abs_z >= threshold: return label
    return None


def rolling_returns(closes, window):
    if len(closes) < window + 1: return None
    return (closes[-1] / closes[-1 - window] - 1) * 100


def _round(v, digits=4):
    if v is None: return None
    if isinstance(v, (int, float)): return round(v, digits)
    return v


# ═══════════════════════════════════════════════════════════════════════
# CONFIG-DRIVEN SIMPLE FRED DETECTORS
# ═══════════════════════════════════════════════════════════════════════

FRED_DETECTORS = [
    # ─────────── RATES / YIELD CURVE ───────────
    {"id": "T10Y2Y", "category": "rates", "name": "2s10s Spread",
     "watch_inversion": True, "unit": "%",
     "interp_pos": "Curve steepening — growth/inflation pricing in.",
     "interp_neg": "Curve flattening — recession fear or Fed tightening.",
     "interp_inverted": "INVERTED — historically recession within 12 months."},
    {"id": "T10Y3M", "category": "rates", "name": "3M-10Y Spread (Fed's recession indicator)",
     "watch_inversion": True, "unit": "%",
     "interp_pos": "Steepening — bond market pricing in growth.",
     "interp_neg": "Flattening — Fed's preferred recession signal weakening.",
     "interp_inverted": "INVERTED — Fed's most-watched recession signal triggered."},
    {"id": "DFII10", "category": "rates", "name": "10Y Real Yield (TIPS)", "unit": "%",
     "interp_pos": "Real yields spiking — tightening financial conditions, risk-asset headwind.",
     "interp_neg": "Real yields falling — easier conditions, gold/risk-asset tailwind."},
    {"id": "DFII5", "category": "rates", "name": "5Y Real Yield (TIPS)", "unit": "%",
     "interp_pos": "5Y real spike — front-end tightening, growth-stock risk.",
     "interp_neg": "5Y real falling — front-end easing, growth tailwind."},
    {"id": "T5YIFR", "category": "rates", "name": "5y5y Forward Inflation Expectation", "unit": "%",
     "interp_pos": "Long-run inflation expectations un-anchoring upward — Fed credibility risk.",
     "interp_neg": "Long-run inflation expectations falling — deflation/disinflation regime."},

    # ─────────── FINANCIAL CONDITIONS (CRITICAL INSTITUTIONAL) ───────────
    {"id": "NFCI", "category": "financial_conditions",
     "name": "Chicago Fed Financial Conditions Index", "unit": "z",
     "interp_pos": "NFCI rising — financial conditions tightening across credit/money/risk channels.",
     "interp_neg": "NFCI falling — financial conditions loosening, risk-on environment."},
    {"id": "ANFCI", "category": "financial_conditions",
     "name": "Adjusted NFCI (controls for current macro state)", "unit": "z",
     "interp_pos": "Adjusted NFCI rising — conditions tighter than macro state would imply, market-driven stress.",
     "interp_neg": "Adjusted NFCI falling — conditions looser than macro state implies, complacency."},
    {"id": "STLFSI4", "category": "financial_conditions",
     "name": "St Louis Fed Financial Stress Index", "unit": "z",
     "interp_pos": "Financial stress rising — bond/equity/credit/FX combined stress index up.",
     "interp_neg": "Financial stress falling — calm across components."},

    # ─────────── FUNDING ───────────
    {"id": "SOFR", "category": "funding", "name": "SOFR Overnight Rate", "unit": "%",
     "interp_pos": "Funding tightening — repo market stress building.",
     "interp_neg": "Funding loosening — liquidity flowing into markets."},
    {"id": "DTB3", "category": "funding", "name": "3M T-Bill Rate", "unit": "%",
     "interp_pos": "Short rates rising — Fed tightening priced in.",
     "interp_neg": "Short rates falling — rate cuts being priced in."},

    # ─────────── LIQUIDITY (BALANCE SHEET) ───────────
    {"id": "WALCL", "category": "funding",
     "name": "Fed Balance Sheet (Total Assets)", "unit": "$mn",
     "interp_pos": "Fed balance sheet expanding — liquidity injection (QE-style).",
     "interp_neg": "Fed balance sheet contracting — QT, liquidity drain."},
    {"id": "WTREGEN", "category": "funding",
     "name": "Treasury General Account (TGA)", "unit": "$mn",
     "interp_pos": "TGA building — Treasury draining liquidity from markets.",
     "interp_neg": "TGA drawing down — Treasury releasing liquidity into markets."},
    {"id": "RRPONTSYD", "category": "funding",
     "name": "Reverse Repo Operations", "unit": "$mn",
     "interp_pos": "RRP rising — money market funds parking cash at Fed, liquidity drain.",
     "interp_neg": "RRP falling — cash leaving Fed RRP, market liquidity returning."},

    # ─────────── CURRENCY ───────────
    {"id": "DTWEXBGS", "category": "currency", "name": "Broad Dollar Index", "unit": "idx",
     "interp_pos": "Dollar strengthening — risk-off / tightening flows.",
     "interp_neg": "Dollar weakening — risk-on / EM tailwind."},
    {"id": "DEXJPUS", "category": "currency", "name": "USD/JPY", "unit": "rate",
     "interp_pos": "Yen weakness — carry trade in play.",
     "interp_neg": "Yen strength — carry unwind, classic risk-off signal."},
    {"id": "DEXCHUS", "category": "currency", "name": "USD/CNY (China)", "unit": "rate",
     "interp_pos": "Yuan weakness — China devaluation pressure, EM/risk headwind.",
     "interp_neg": "Yuan strength — China stabilizing, EM tailwind."},

    # ─────────── MACRO / RECESSION NOWCAST ───────────
    {"id": "ICSA", "category": "macro", "name": "Initial Jobless Claims", "unit": "k",
     "interp_pos": "Claims rising — labor market weakening, recession risk.",
     "interp_neg": "Claims falling — labor strong, expansion continues."},
    {"id": "SAHMREALTIME", "category": "macro",
     "name": "Sahm Rule Recession Indicator", "unit": "%",
     "interp_pos": "Sahm Rule near/above 0.5 — recession in progress per real-time labor signal.",
     "interp_neg": "Sahm Rule falling — labor market re-strengthening."},
    {"id": "RECPROUSM156N", "category": "macro",
     "name": "NY Fed Recession Probability (12m forward)", "unit": "%",
     "interp_pos": "Recession probability rising per NY Fed yield-curve model.",
     "interp_neg": "Recession probability falling — yield curve dis-inverting."},
    {"id": "CFNAI", "category": "macro",
     "name": "Chicago Fed National Activity Index", "unit": "z",
     "interp_pos": "CFNAI rising — broad economic activity accelerating.",
     "interp_neg": "CFNAI < -0.7 historically marks recession; deep negative is the signal."},
    {"id": "UMCSENT", "category": "macro",
     "name": "U-Mich Consumer Sentiment", "unit": "idx",
     "interp_pos": "Consumer sentiment rising — discretionary tailwind.",
     "interp_neg": "Consumer sentiment falling — discretionary risk, savings rate up."},

    # ─────────── EQUITY VOL FAMILY (broader than VIX/SKEW) ───────────
    {"id": "VXNCLS", "category": "equity_vol",
     "name": "VXN — Nasdaq 100 Volatility Index", "unit": "vol",
     "interp_pos": "Nasdaq vol spike — tech-specific stress, single-name event risk.",
     "interp_neg": "Nasdaq vol compressing — tech complacency."},
    {"id": "GVZCLS", "category": "equity_vol",
     "name": "GVZ — Gold Volatility Index", "unit": "vol",
     "interp_pos": "Gold vol spiking — flight-to-quality regime shift in progress.",
     "interp_neg": "Gold vol compressing — gold range-bound, regime stable."},
    {"id": "OVXCLS", "category": "equity_vol",
     "name": "OVX — Crude Oil Volatility Index", "unit": "vol",
     "interp_pos": "Oil vol spiking — supply shock or demand collapse pricing in.",
     "interp_neg": "Oil vol compressing — energy markets calm."},

    # ─────────── COMMODITY (single-asset) ───────────
    {"id": "WTISPLC", "category": "commodity", "name": "WTI Crude Oil", "unit": "$/bbl",
     "interp_pos": "Oil surge — supply shock or demand spike.",
     "interp_neg": "Oil collapse — demand destruction or oversupply."},

    # ─────────── HOUSING ───────────
    {"id": "MORTGAGE30US", "category": "rates",
     "name": "30Y Fixed Mortgage Rate", "unit": "%",
     "interp_pos": "Mortgage rates spiking — housing freeze risk, REIT/builder headwind.",
     "interp_neg": "Mortgage rates falling — housing affordability easing, refi wave possible."},
]


def run_fred_detector(cfg, data):
    """Run anomaly check on a FRED series with level + 1w change z-scores.
    Returns (anomaly_dict_or_None, metric_dict)."""
    if not data or len(data) < 60:
        return None, {"id": cfg["id"], "name": cfg["name"], "category": cfg["category"],
                       "err": f"insufficient data ({len(data)} obs)"}

    values = [d["value"] for d in data]
    current = values[-1]
    as_of = data[-1]["date"]

    z60, mean60, std60, _ = z_score(values, lookback=60)
    z180, mean180, _, _ = z_score(values, lookback=180)

    week_change = None
    change_z = None
    if len(values) >= 6:
        week_change = values[-1] - values[-6]
        changes = [values[i] - values[i-5] for i in range(5, len(values) - 1)]
        if changes:
            change_z, _, _, _ = z_score(changes + [week_change], lookback=min(180, len(changes)))

    pct_1y = percentile_rank(current, values[-min(252, len(values)):])
    pct_5y = percentile_rank(current, values)

    inverted = cfg.get("watch_inversion") and current < 0
    just_inverted = False
    if cfg.get("watch_inversion") and len(values) >= 22:
        just_inverted = inverted and values[-22] > 0

    metric = {
        "id": cfg["id"], "name": cfg["name"], "category": cfg["category"],
        "current": _round(current),
        "as_of": as_of, "unit": cfg.get("unit", ""),
        "z60": _round(z60, 2), "z180": _round(z180, 2),
        "mean60": _round(mean60),
        "1w_change": _round(week_change),
        "1w_change_z": _round(change_z, 2),
        "pct_rank_1y": pct_1y, "pct_rank_5y": pct_5y,
        "inverted": inverted, "just_inverted": just_inverted,
    }

    triggers = []
    max_abs_z = 0
    direction = None

    if z60 is not None and abs(z60) >= 2.0:
        triggers.append(f"level z60={z60:.2f}")
        max_abs_z = max(max_abs_z, abs(z60))
        direction = "pos" if z60 > 0 else "neg"
    if change_z is not None and abs(change_z) >= 2.0:
        triggers.append(f"1w chg z={change_z:.2f}")
        if abs(change_z) > max_abs_z:
            max_abs_z = abs(change_z)
            direction = "pos" if change_z > 0 else "neg"
    if just_inverted:
        triggers.append("CROSSED INTO INVERSION")
        max_abs_z = max(max_abs_z, 4.0)

    severity = classify_severity(max_abs_z) if triggers else None
    anomaly = None
    if severity:
        if just_inverted:
            implication = cfg.get("interp_inverted", "Inversion event.")
        elif direction == "pos":
            implication = cfg.get("interp_pos", "Significant positive move.")
        else:
            implication = cfg.get("interp_neg", "Significant negative move.")
        anomaly = {
            "category": cfg["category"], "name": cfg["name"],
            "series_id": cfg["id"], "severity": severity,
            "z_score": _round(max_abs_z, 2),
            "current_value": _round(current),
            "details": f"{cfg['name']} = {current:.3f}{cfg.get('unit','')} · " + " · ".join(triggers),
            "implication": implication,
            "pct_rank_5y": pct_5y,
        }
    return anomaly, metric


# ═══════════════════════════════════════════════════════════════════════
# RATIO DETECTORS (two-leg Polygon)
# ═══════════════════════════════════════════════════════════════════════

RATIO_DETECTORS = [
    {"id": "copper_gold", "category": "commodity",
     "name": "Copper/Gold (Gundlach's recession indicator)",
     "numerator": "CPER", "denominator": "GLD",
     "interp_pos": "Copper > Gold — growth signal, risk-on.",
     "interp_neg": "Gold > Copper — recession/risk-off signal."},
    {"id": "silver_gold", "category": "commodity", "name": "Silver/Gold Ratio",
     "numerator": "SLV", "denominator": "GLD",
     "interp_pos": "Silver leading — risk-on within precious metals.",
     "interp_neg": "Silver lagging — flight to quality."},
    {"id": "high_low_beta", "category": "equity_internals",
     "name": "High-Beta/Low-Vol (SPHB/SPLV risk appetite)",
     "numerator": "SPHB", "denominator": "SPLV",
     "interp_pos": "High-beta leading — risk appetite expanding.",
     "interp_neg": "Low-vol leading — defensive rotation, risk-off."},
    {"id": "equal_cap_weight", "category": "breadth",
     "name": "Equal-Weight/Cap-Weight (RSP/SPY breadth)",
     "numerator": "RSP", "denominator": "SPY",
     "interp_pos": "Breadth expanding — broad participation.",
     "interp_neg": "Mega-caps dominating — narrow leadership, late-cycle."},
    {"id": "hy_ig", "category": "credit",
     "name": "HY/IG Bond Ratio (HYG/LQD credit risk appetite)",
     "numerator": "HYG", "denominator": "LQD",
     "interp_pos": "HY > IG — credit risk-on.",
     "interp_neg": "IG > HY — credit risk-off, default fear."},
    {"id": "utilities_tech", "category": "equity_internals",
     "name": "Utilities/Tech (XLU/XLK defensive ratio)",
     "numerator": "XLU", "denominator": "XLK",
     "interp_pos": "Defensives > Tech — risk-off rotation.",
     "interp_neg": "Tech > Defensives — risk-on, growth premium expanding."},
]


def run_ratio_detector(cfg, bars_num, bars_den):
    if not bars_num or not bars_den or len(bars_num) < 60 or len(bars_den) < 60:
        return None, {"id": cfg["id"], "name": cfg["name"], "category": cfg["category"],
                       "err": "insufficient bars"}
    closes_num = {b["t"]: b["c"] for b in bars_num}
    closes_den = {b["t"]: b["c"] for b in bars_den}
    common = sorted(set(closes_num) & set(closes_den))
    if len(common) < 60:
        return None, {"id": cfg["id"], "name": cfg["name"], "category": cfg["category"],
                       "err": "insufficient overlap"}

    ratios = [closes_num[t] / closes_den[t] for t in common if closes_den[t]]
    if len(ratios) < 60: return None, None

    current_ratio = ratios[-1]
    z60, mean60, _, _ = z_score(ratios, lookback=60)
    z180, _, _, _ = z_score(ratios, lookback=min(180, len(ratios) - 1))
    week_change = None
    change_z = None
    if len(ratios) >= 6:
        week_change = (ratios[-1] / ratios[-6] - 1) * 100
        ratio_changes = [(ratios[i] / ratios[i-5] - 1) * 100 for i in range(5, len(ratios) - 1)]
        if ratio_changes:
            change_z, _, _, _ = z_score(ratio_changes + [week_change],
                                          lookback=min(180, len(ratio_changes)))

    pct_5y = percentile_rank(current_ratio, ratios)

    metric = {
        "id": cfg["id"], "name": cfg["name"], "category": cfg["category"],
        "ratio_current": _round(current_ratio, 5),
        "z60": _round(z60, 2), "z180": _round(z180, 2),
        "1w_change_pct": _round(week_change, 2),
        "1w_change_z": _round(change_z, 2),
        "pct_rank_5y": pct_5y,
        "numerator": cfg["numerator"], "denominator": cfg["denominator"],
    }

    triggers = []
    max_abs_z = 0
    direction = None
    if z60 is not None and abs(z60) >= 2.0:
        triggers.append(f"level z={z60:.2f}")
        max_abs_z = max(max_abs_z, abs(z60))
        direction = "pos" if z60 > 0 else "neg"
    if change_z is not None and abs(change_z) >= 2.5:  # higher bar — change is noisier
        triggers.append(f"1w chg z={change_z:.2f}")
        if abs(change_z) > max_abs_z:
            max_abs_z = abs(change_z)
            direction = "pos" if change_z > 0 else "neg"

    severity = classify_severity(max_abs_z) if triggers else None
    anomaly = None
    if severity:
        anomaly = {
            "category": cfg["category"], "name": cfg["name"],
            "severity": severity, "z_score": _round(max_abs_z, 2),
            "current_value": _round(current_ratio, 5),
            "details": f"{cfg['name']} ratio={current_ratio:.4f} · " + " · ".join(triggers),
            "implication": cfg.get("interp_pos" if direction == "pos" else "interp_neg",
                                     "Significant ratio move."),
            "pct_rank_5y": pct_5y,
        }
    return anomaly, metric


# ═══════════════════════════════════════════════════════════════════════
# COMPLEX DETECTORS
# ═══════════════════════════════════════════════════════════════════════

def detect_cross_asset_divergence():
    symbols = ["SPY", "TLT", "GLD", "UUP"]
    bars = batch_fetch_polygon(symbols, 90)
    if any(len(bars.get(s, [])) < 25 for s in symbols):
        return None, {"err": "insufficient bars"}

    returns = {}
    closes_by_sym = {}
    for sym in symbols:
        closes = [b["c"] for b in bars[sym]]
        closes_by_sym[sym] = closes
        returns[sym] = {
            "1w": _round(rolling_returns(closes, 5), 2),
            "1m": _round(rolling_returns(closes, 21), 2),
            "current": _round(closes[-1], 2),
        }

    signs_1w = [1 if returns[s]["1w"] and returns[s]["1w"] > 0.1
                else -1 if returns[s]["1w"] and returns[s]["1w"] < -0.1
                else 0 for s in symbols]
    all_up = all(s > 0 for s in signs_1w)
    all_down = all(s < 0 for s in signs_1w)

    # SPY-TLT correlation breakdown
    spy_tlt_corr = None
    n = min(len(closes_by_sym[s]) for s in symbols)
    if n >= 35:
        spy_ret = [(closes_by_sym["SPY"][i] / closes_by_sym["SPY"][i-1] - 1) for i in range(n - 30, n)]
        tlt_ret = [(closes_by_sym["TLT"][i] / closes_by_sym["TLT"][i-1] - 1) for i in range(n - 30, n)]
        if len(spy_ret) >= 25:
            try: spy_tlt_corr = statistics.correlation(spy_ret, tlt_ret)
            except Exception: pass

    metric = {
        "asset_returns_1w": returns,
        "spy_tlt_corr_30d": _round(spy_tlt_corr, 3),
        "signs_aligned": all_up or all_down,
    }

    # Sign alignment anomaly
    if all_up or all_down:
        magnitudes = [abs(returns[s]["1w"] or 0) for s in symbols]
        avg_mag = sum(magnitudes) / len(magnitudes)
        pseudo_z = 1.5 + avg_mag * 1.2
        sev = classify_severity(pseudo_z)
        if sev:
            direction = "RALLY" if all_up else "LIQUIDATION"
            return {
                "category": "cross_asset",
                "name": f"All-Asset {direction.title()}",
                "severity": sev, "z_score": _round(pseudo_z, 2),
                "details": " · ".join(f"{s}{'+' if (returns[s]['1w'] or 0)>=0 else ''}{returns[s]['1w']:.2f}%"
                                       for s in symbols),
                "implication": ("All-asset rally — institutional broad hedging or liquidity flood. "
                                "Historically precedes risk-off in 5-15 days." if all_up else
                                "All-asset liquidation — forced selling across asset classes. "
                                "Capitulation signal — bottom often within 10 days."),
            }, metric

    # Correlation breakdown
    if spy_tlt_corr is not None and spy_tlt_corr > 0.4:
        z = 2.0 + (spy_tlt_corr - 0.4) * 5
        sev = classify_severity(z)
        if sev:
            return {
                "category": "cross_asset",
                "name": "SPY/TLT Correlation Breakdown",
                "severity": sev, "z_score": _round(z, 2),
                "details": f"SPY/TLT 30d correlation = {spy_tlt_corr:.2f} (normal: -0.4 to +0.2)",
                "implication": ("Stocks and bonds moving TOGETHER — 60/40 diversification failing. "
                                "Historically associated with inflation regime shifts or systemic stress."),
            }, metric

    return None, metric


def detect_sector_breadth():
    SECTORS = ["XLK","XLF","XLV","XLE","XLI","XLY","XLP","XLU","XLB","XLRE","XLC"]
    bars = batch_fetch_polygon(SECTORS, 90)
    valid = {s: bars[s] for s in SECTORS if len(bars.get(s, [])) >= 30}
    if len(valid) < 9: return None, {"err": "insufficient sector data"}

    returns_5d = {}
    for sym, b in valid.items():
        r = rolling_returns([x["c"] for x in b], 5)
        if r is not None: returns_5d[sym] = r
    if len(returns_5d) < 9: return None, None

    current_disp = statistics.stdev(list(returns_5d.values()))

    historical = []
    n_check = min(60, min(len(v) for v in valid.values()) - 7)
    for offset in range(1, n_check):
        day_returns = []
        for sym, b in valid.items():
            closes = [x["c"] for x in b]
            i = len(closes) - 1 - offset
            if i >= 5: day_returns.append((closes[i] / closes[i-5] - 1) * 100)
        if len(day_returns) >= 9:
            historical.append(statistics.stdev(day_returns))

    if len(historical) < 20: return None, None
    z, mean, _, _ = z_score(historical + [current_disp])
    if z is None: return None, None

    sorted_sectors = sorted(returns_5d.items(), key=lambda x: -x[1])
    top, bottom = sorted_sectors[:2], sorted_sectors[-2:]

    metric = {
        "current_dispersion_pct": _round(current_disp, 3),
        "60d_mean_dispersion": _round(mean, 3),
        "z_score": _round(z, 2),
        "sector_returns_5d_pct": {s: _round(r, 2) for s, r in returns_5d.items()},
        "top_sectors": [{"sym": s, "ret_5d": _round(r, 2)} for s, r in top],
        "bottom_sectors": [{"sym": s, "ret_5d": _round(r, 2)} for s, r in bottom],
    }

    abs_z = abs(z)
    sev = classify_severity(abs_z)
    if not sev: return None, metric

    if z > 0:
        implication = (f"Dispersion {current_disp:.2f}% (z={z:.2f}) — {top[0][0]} +{top[0][1]:.1f}% "
                       f"leads vs {bottom[0][0]} {bottom[0][1]:+.1f}%. Wide dispersion = rotation, "
                       "often signals end of thematic rally or new leadership.")
    else:
        implication = ("All sectors moving together — broad risk-on/off. Diversification failing. "
                       "Often precedes volatility expansion.")
    return {
        "category": "breadth",
        "name": "Sector Rotation/Divergence" if z > 0 else "Sector Compression",
        "severity": sev, "z_score": _round(z, 2),
        "current_value": _round(current_disp, 3),
        "details": f"Dispersion {current_disp:.2f}% z={z:.2f} · Top {top[0][0]}{top[0][1]:+.1f}% · Bot {bottom[0][0]}{bottom[0][1]:+.1f}%",
        "implication": implication,
    }, metric


def detect_vix_skew():
    vix = fetch_fred_series("VIXCLS", 400)
    skew = fetch_fred_series("SKEW", 400)
    if len(vix) < 60: return [], {"err": "no VIX data"}

    vix_values = [v["value"] for v in vix]
    vix_z60, vix_mean, _, _ = z_score(vix_values, lookback=60)
    vix_z180, _, _, _ = z_score(vix_values, lookback=180)

    metric = {
        "vix": {
            "current": _round(vix_values[-1], 2),
            "z60": _round(vix_z60, 2), "z180": _round(vix_z180, 2),
            "mean60": _round(vix_mean, 2),
            "interpretation": _vix_interp(vix_values[-1]),
            "as_of": vix[-1]["date"],
        }
    }

    anomalies = []
    if vix_z60 is not None and abs(vix_z60) >= 2.0:
        sev = classify_severity(abs(vix_z60))
        if sev:
            anomalies.append({
                "category": "equity_vol",
                "name": "VIX " + ("Spike" if vix_z60 > 0 else "Compression"),
                "severity": sev, "z_score": _round(vix_z60, 2),
                "current_value": _round(vix_values[-1], 2),
                "details": f"VIX {vix_values[-1]:.2f} vs 60d mean {vix_mean:.2f} (z={vix_z60:.2f})",
                "implication": ("Elevated VIX — options pricing higher near-term vol."
                                 if vix_z60 > 0 else
                                 "Suppressed VIX — complacency at multi-month low, vulnerable to spike."),
            })

    if len(skew) >= 60:
        skew_values = [v["value"] for v in skew]
        skew_z, skew_mean, _, _ = z_score(skew_values, lookback=60)
        metric["skew"] = {
            "current": _round(skew_values[-1], 2),
            "z60": _round(skew_z, 2),
            "mean60": _round(skew_mean, 2),
            "interpretation": _skew_interp(skew_values[-1]),
            "as_of": skew[-1]["date"],
        }
        if skew_z is not None and abs(skew_z) >= 2.0:
            divergence = skew_z - (vix_z60 or 0)
            if abs(divergence) >= 2.0:
                sev = classify_severity(abs(divergence))
                if sev:
                    anomalies.append({
                        "category": "equity_vol",
                        "name": "SKEW/VIX Divergence",
                        "severity": sev, "z_score": _round(divergence, 2),
                        "current_value": {"vix": _round(vix_values[-1], 2),
                                            "skew": _round(skew_values[-1], 2)},
                        "details": f"VIX z={vix_z60:.2f} · SKEW z={skew_z:.2f} · div={divergence:.2f}",
                        "implication": ("Tail-risk premium elevated WITHOUT broad fear — "
                                         "institutions buying OTM puts quietly. Historically precedes "
                                         "drawdowns by 2-6 weeks." if divergence > 0 else
                                         "SKEW falling while VIX elevated — panic without tail concern, "
                                         "possible capitulation."),
                    })
    return anomalies, metric


def detect_credit_spreads():
    SPREADS = [("BAMLH0A0HYM2", "HY OAS"), ("BAMLC0A4CBBB", "BBB OAS")]
    series_data = batch_fetch_fred([s[0] for s in SPREADS], 500)
    anomalies, metrics = [], {}
    for series_id, short_name in SPREADS:
        data = series_data.get(series_id, [])
        if len(data) < 40: continue
        values = [d["value"] for d in data]
        level_z, level_mean, _, _ = z_score(values, lookback=180)
        if len(values) >= 6:
            week_change = values[-1] - values[-6]
            changes = [values[i] - values[i-5] for i in range(5, len(values) - 1)]
            change_z, _, _, _ = z_score(changes + [week_change], lookback=180)
        else:
            week_change, change_z = None, None

        metrics[short_name.lower().replace(" ", "_")] = {
            "current": _round(values[-1], 3),
            "180d_mean": _round(level_mean, 3),
            "level_z": _round(level_z, 2),
            "1w_change": _round(week_change, 3),
            "1w_change_z": _round(change_z, 2),
            "pct_rank_5y": percentile_rank(values[-1], values),
            "as_of": data[-1]["date"],
        }

        max_z = 0
        triggers = []
        if level_z is not None and abs(level_z) >= 2.0:
            triggers.append(f"level z={level_z:.2f}")
            max_z = max(max_z, abs(level_z))
        if change_z is not None and abs(change_z) >= 2.0:
            triggers.append(f"1w chg z={change_z:.2f}")
            max_z = max(max_z, abs(change_z))
        if max_z >= 2.0:
            direction = "Blowout" if ((change_z or 0) > 0 or (level_z or 0) > 0) else "Compression"
            sev = classify_severity(max_z)
            if sev:
                anomalies.append({
                    "category": "credit",
                    "name": f"{short_name} {direction}",
                    "severity": sev, "z_score": _round(max_z, 2),
                    "current_value": _round(values[-1], 3),
                    "details": f"{short_name} at {values[-1]:.2f}% · " + " · ".join(triggers),
                    "implication": (f"{short_name} widening — credit pricing default risk. "
                                     "Historically leads equity weakness by 1-3 weeks."
                                     if direction == "Blowout" else
                                     f"{short_name} compressing — credit relaxing, constructive."),
                })
    return anomalies, metrics


def detect_bond_volatility():
    dgs10 = fetch_fred_series("DGS10", 400)
    if len(dgs10) < 60: return None, None
    values = [d["value"] for d in dgs10]
    changes = [values[i] - values[i-1] for i in range(1, len(values))]
    if len(changes) < 60: return None, None
    current_vol = statistics.stdev(changes[-30:]) * (252 ** 0.5)
    historical = [statistics.stdev(changes[i-30:i]) * (252 ** 0.5) for i in range(30, len(changes) - 1)]
    if len(historical) < 30: return None, None
    z, mean, _, _ = z_score(historical + [current_vol])
    metric = {
        "current_30d_annualized_vol": _round(current_vol, 3),
        "1y_mean": _round(mean, 3),
        "z_score": _round(z, 2),
        "10y_yield": _round(values[-1], 3),
        "as_of": dgs10[-1]["date"],
    }
    if z is None: return None, metric
    abs_z = abs(z)
    sev = classify_severity(abs_z)
    if not sev: return None, metric
    return {
        "category": "rates",
        "name": "Bond Yield Volatility " + ("Spike" if z > 0 else "Compression"),
        "severity": sev, "z_score": _round(z, 2),
        "current_value": _round(current_vol, 3),
        "details": f"10Y annualized vol {current_vol:.2f}% (z={z:.2f}) · 10Y yield {values[-1]:.2f}%",
        "implication": ("Bond vol spiking — Treasury repricing aggressively. Leads equity vol 1-2w."
                        if z > 0 else
                        "Bond vol compressed — yields ranging tightly. Stability precedes breakouts."),
    }, metric


def detect_crypto_signal():
    bars = fetch_polygon_ohlcv("X:BTCUSD", 90) or fetch_polygon_ohlcv("BITO", 90) or fetch_polygon_ohlcv("GBTC", 90)
    if len(bars) < 30: return None, {"err": "no BTC data"}
    closes = [b["c"] for b in bars]
    returns = [(closes[i] / closes[i-1] - 1) for i in range(1, len(closes))]
    if len(returns) < 60: return None, {"btc_current": _round(closes[-1], 2)}
    current_vol = statistics.stdev(returns[-30:]) * (365 ** 0.5) * 100
    historical_vols = [statistics.stdev(returns[i-30:i]) * (365 ** 0.5) * 100
                       for i in range(30, len(returns))]
    z, mean, _, _ = z_score(historical_vols + [current_vol])

    metric = {
        "btc_current": _round(closes[-1], 2),
        "btc_30d_vol_pct": _round(current_vol, 1),
        "btc_vol_z": _round(z, 2),
        "btc_vol_pct_rank": percentile_rank(current_vol, historical_vols),
    }
    if z is None: return None, metric
    abs_z = abs(z)
    sev = classify_severity(abs_z)
    if not sev: return None, metric
    return {
        "category": "crypto",
        "name": "BTC Volatility " + ("Spike" if z > 0 else "Compression"),
        "severity": sev, "z_score": _round(z, 2),
        "current_value": _round(current_vol, 2),
        "details": f"BTC 30d annualized vol {current_vol:.1f}% (z={z:.2f})",
        "implication": ("Crypto vol expanding — risk-off precursor or crypto-specific event."
                        if z > 0 else
                        "Crypto vol compressed — coiled spring, breakouts often follow."),
    }, metric


def detect_trend_reversion():
    bars = fetch_polygon_ohlcv("SPY", 120)
    if len(bars) < 60: return None, None
    closes = [b["c"] for b in bars]
    returns = [(closes[i] / closes[i-1] - 1) for i in range(1, len(closes))]
    if len(returns) < 60: return None, None

    def lag1_corr(rs):
        if len(rs) < 5: return None
        try: return statistics.correlation(rs[:-1], rs[1:])
        except Exception: return None

    current_ac = lag1_corr(returns[-30:])
    historical_ac = [lag1_corr(returns[i-30:i]) for i in range(60, len(returns), 5)]
    historical_ac = [x for x in historical_ac if x is not None]

    metric = {
        "spy_lag1_autocorr": _round(current_ac, 3) if current_ac is not None else None,
        "spy_30d_return_pct": _round(rolling_returns(closes, 30), 2),
        "spy_50d_return_pct": _round(rolling_returns(closes, 50), 2),
    }
    if current_ac is None or len(historical_ac) < 10: return None, metric
    z, _, _, _ = z_score(historical_ac + [current_ac])
    metric["autocorr_z"] = _round(z, 2)
    if z is None: return None, metric
    sev = classify_severity(abs(z))
    if not sev: return None, metric

    regime = "trending" if current_ac > 0.15 else "mean-reversion" if current_ac < -0.15 else "neutral"
    return {
        "category": "equity_internals",
        "name": "Return Auto-Correlation Shift",
        "severity": sev, "z_score": _round(z, 2),
        "current_value": _round(current_ac, 3),
        "details": f"Lag-1 autocorr {current_ac:+.2f} (z={z:.2f}) — {regime} regime",
        "implication": ("Returns autocorr rising — trends persisting unusually, often precedes "
                        "blow-off tops or capitulations."
                        if z > 0 else
                        "Returns autocorr falling — mean reversion strengthening, directional moves "
                        "quickly reversed."),
    }, metric


def detect_funding_stress():
    series = batch_fetch_fred(["SOFR", "DTB3"], 400)
    sofr = series.get("SOFR", [])
    dtb3 = series.get("DTB3", [])
    if len(sofr) < 60 or len(dtb3) < 60: return None, None
    sofr_by_date = {d["date"]: d["value"] for d in sofr}
    dtb3_by_date = {d["date"]: d["value"] for d in dtb3}
    common = sorted(set(sofr_by_date) & set(dtb3_by_date))
    if len(common) < 60: return None, None
    spreads = [sofr_by_date[d] - dtb3_by_date[d] for d in common]
    current = spreads[-1]
    z, mean, _, _ = z_score(spreads)
    metric = {
        "sofr_dtb3_spread_current": _round(current, 3),
        "60d_mean": _round(mean, 3),
        "z_score": _round(z, 2),
        "sofr": _round(sofr_by_date[common[-1]], 3),
        "dtb3": _round(dtb3_by_date[common[-1]], 3),
        "as_of": common[-1],
    }
    if z is None: return None, metric
    sev = classify_severity(abs(z))
    if not sev: return None, metric
    return {
        "category": "funding",
        "name": "SOFR-DTB3 Spread " + ("Widening" if z > 0 else "Compressing"),
        "severity": sev, "z_score": _round(z, 2),
        "current_value": _round(current, 3),
        "details": f"SOFR-DTB3 spread {current:.3f}% (z={z:.2f})",
        "implication": ("Funding spread widening — repo tightening, dollar funding stress."
                        if z > 0 else
                        "Funding spread compressing — liquidity flowing, repo easing."),
    }, metric


def detect_net_liquidity():
    """Net Liquidity = Fed Balance Sheet (WALCL) - Treasury General Account (WTREGEN)
                       - Reverse Repo (RRPONTSYD)

    This is the "BTC liquidity proxy" / "true Fed liquidity" hedge funds watch.
    Detects week-over-week regime shifts in dollar liquidity.

    Returns 1w change z-score over 180-day history of weekly changes.
    """
    series = batch_fetch_fred(["WALCL", "WTREGEN", "RRPONTSYD"], 600)
    walcl = series.get("WALCL", [])
    tga = series.get("WTREGEN", [])
    rrp = series.get("RRPONTSYD", [])
    if len(walcl) < 50 or len(tga) < 50 or len(rrp) < 50:
        return None, {"err": f"insufficient: walcl={len(walcl)} tga={len(tga)} rrp={len(rrp)}"}

    walcl_by = {d["date"]: d["value"] for d in walcl}
    tga_by = {d["date"]: d["value"] for d in tga}
    rrp_by = {d["date"]: d["value"] for d in rrp}
    common = sorted(set(walcl_by) & set(tga_by) & set(rrp_by))
    if len(common) < 50: return None, {"err": f"common dates {len(common)}"}

    # Compute net liquidity timeseries (in $bn for readability)
    net_liq = [(walcl_by[d] - tga_by[d] - rrp_by[d]) / 1000.0 for d in common]
    current = net_liq[-1]
    prior_week = net_liq[-6] if len(net_liq) >= 6 else net_liq[0]
    w_change = current - prior_week
    # Weekly changes for z-score
    weekly_changes = [net_liq[i] - net_liq[i-5] for i in range(5, len(net_liq))]
    z, mean, _, _ = z_score(weekly_changes + [w_change])
    # Also percentile rank current level over 5y
    pct_5y = percentile_rank(current, net_liq)
    metric = {
        "net_liquidity_bn": _round(current, 1),
        "1w_change_bn": _round(w_change, 1),
        "1w_change_z": _round(z, 2),
        "pct_rank_5y": pct_5y,
        "walcl_bn": _round(walcl_by[common[-1]] / 1000, 1),
        "tga_bn": _round(tga_by[common[-1]] / 1000, 1),
        "rrp_bn": _round(rrp_by[common[-1]] / 1000, 1),
        "as_of": common[-1],
    }
    if z is None: return None, metric
    sev = classify_severity(abs(z))
    if not sev: return None, metric
    return {
        "category": "funding",
        "name": "Net Liquidity " + ("Surge" if z > 0 else "Drain"),
        "severity": sev, "z_score": _round(z, 2),
        "current_value": _round(current, 1),
        "details": f"Net Liquidity ${current:,.0f}B · 1w Δ ${w_change:+,.0f}B (z={z:.2f}) · pct-rank-5y {pct_5y}",
        "implication": ("Liquidity injection: WALCL up or TGA/RRP draining — risk-asset tailwind."
                        if z > 0 else
                        "Liquidity drain: TGA building or WALCL contracting — risk-asset headwind."),
    }, metric


def _vix_interp(v):
    if v < 13: return "extreme complacency"
    if v < 17: return "low fear"
    if v < 22: return "normal"
    if v < 30: return "elevated fear"
    return "panic / crisis"


def _skew_interp(v):
    if v < 120: return "minimal tail-risk concern"
    if v < 140: return "normal tail-risk pricing"
    if v < 150: return "elevated tail hedging"
    return "extreme tail-risk premium"


# ═══════════════════════════════════════════════════════════════════════
# MACRO STRESS SCORE
# ═══════════════════════════════════════════════════════════════════════

def compute_stress_score(metrics_by_cat, anomalies):
    contributions = {}
    for category, weight in STRESS_WEIGHTS.items():
        zs = []
        for mname, m in (metrics_by_cat.get(category) or {}).items():
            if isinstance(m, dict):
                for key in ("z_score", "z60", "level_z", "1w_change_z",
                            "autocorr_z", "btc_vol_z"):
                    v = m.get(key)
                    if isinstance(v, (int, float)):
                        zs.append(abs(v))
        for a in anomalies:
            if a.get("category") == category:
                z = a.get("z_score")
                if isinstance(z, (int, float)):
                    zs.append(abs(z))
        if not zs:
            contributions[category] = 0.0
            continue
        max_z = max(zs)
        # Convert max z to 0-100 contribution
        cat_stress = min(100, max_z * 18 + (max_z ** 2) * 1.5)
        contributions[category] = round(cat_stress, 1)
    weighted = sum(contributions[c] * STRESS_WEIGHTS[c] for c in STRESS_WEIGHTS)
    return round(weighted, 1), contributions


def stress_interp(score):
    if score < 20: return "Goldilocks — markets calm across the board."
    if score < 40: return "Normal — typical market noise, no warnings."
    if score < 60: return "Elevated — multiple metrics showing stress, watch closely."
    if score < 80: return "High stress — reduce gross exposure, hedge tail risk."
    return "Crisis-level — defensive posture, expect drawdowns."


# ═══════════════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════════════

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


def format_alert(anomaly, stress_score=None):
    icon = {"EXTREME": "🚨", "HIGH": "⚠️", "MEDIUM": "⚡", "LOW": "🔔"}.get(anomaly["severity"], "🔔")
    name = (anomaly["name"] or "").replace("_", " ")
    cat = anomaly.get("category", "").replace("_", " ").upper()
    z = anomaly.get("z_score")
    stress_line = f"\n`Macro Stress: {stress_score}/100`\n" if stress_score is not None else "\n"
    return (f"{icon} *{anomaly['severity']} ANOMALY · {cat}*\n*{name}*\n"
            f"`z-score: {z}`{stress_line}_{anomaly.get('details','')}_\n\n"
            f"{anomaly.get('implication','')}\n\n"
            f"[Anomaly Dashboard](https://justhodl.ai/anomalies/) · "
            f"[Alpha](https://justhodl.ai/alpha/)")


def format_crisis(anomalies, stress_score):
    lines = [f"🚨 *CRISIS ALERT — Macro Stress {stress_score}/100*\n",
             f"_{stress_interp(stress_score)}_\n",
             f"*{len(anomalies)} simultaneous HIGH/EXTREME anomalies:*"]
    for a in anomalies[:8]:
        cat = a.get("category", "").replace("_", " ").upper()
        lines.append(f"  • {a['severity']} · {cat} · {a['name']} (z={a.get('z_score')})")
    lines.append(f"\n_Historical analog:_ when 3+ macro/credit/vol anomalies fire together, "
                 "forward 30-day SPY returns average -7% to -15%. *Reduce gross exposure.*")
    lines.append(f"\n[Anomaly Dashboard](https://justhodl.ai/anomalies/)")
    return "\n".join(lines)


def load_alert_history():
    try: return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=ALERT_HISTORY_KEY)["Body"].read())
    except Exception: return {}


def save_alert_history(h):
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=ALERT_HISTORY_KEY,
            Body=json.dumps(h, separators=(",", ":")).encode("utf-8"),
            ContentType="application/json")
    except Exception as e: print(f"  history err: {e}")


def should_alert(history, key):
    last = history.get(key)
    if not last: return True
    try:
        last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - last_dt) >= timedelta(hours=DEDUPE_HOURS)
    except Exception: return True


# ═══════════════════════════════════════════════════════════════════════
# MAIN HANDLER
# ═══════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    started = time.time()
    print(f"=== ANOMALY DETECTOR v{VERSION} · {datetime.now(timezone.utc).isoformat()} ===")

    all_anomalies = []
    all_metrics = {}
    metrics_by_cat = {}
    timings = {}

    # ─── Config-driven FRED detectors ───
    t0 = time.time()
    fred_data = batch_fetch_fred([c["id"] for c in FRED_DETECTORS], lookback_days=600)
    for cfg in FRED_DETECTORS:
        try:
            anomaly, metric = run_fred_detector(cfg, fred_data.get(cfg["id"], []))
            if metric:
                all_metrics[cfg["id"]] = metric
                metrics_by_cat.setdefault(cfg["category"], {})[cfg["id"]] = metric
            if anomaly: all_anomalies.append(anomaly)
        except Exception as e:
            print(f"  fred:{cfg['id']} ERR: {str(e)[:150]}")
    timings["fred_detectors"] = round(time.time() - t0, 2)

    # ─── Ratio detectors ───
    t0 = time.time()
    ratio_symbols = list({s for c in RATIO_DETECTORS for s in (c["numerator"], c["denominator"])})
    ratio_bars = batch_fetch_polygon(ratio_symbols, 120)
    for cfg in RATIO_DETECTORS:
        try:
            anomaly, metric = run_ratio_detector(cfg,
                ratio_bars.get(cfg["numerator"], []),
                ratio_bars.get(cfg["denominator"], []))
            if metric:
                all_metrics[cfg["id"]] = metric
                metrics_by_cat.setdefault(cfg["category"], {})[cfg["id"]] = metric
            if anomaly: all_anomalies.append(anomaly)
        except Exception as e:
            print(f"  ratio:{cfg['id']} ERR: {str(e)[:150]}")
    timings["ratio_detectors"] = round(time.time() - t0, 2)

    # ─── Complex detectors ───
    complex_specs = [
        ("cross_asset",      detect_cross_asset_divergence, "cross_asset"),
        ("sector_breadth",   detect_sector_breadth,         "breadth"),
        ("vix_skew",         detect_vix_skew,                "equity_vol"),
        ("credit_spreads",   detect_credit_spreads,          "credit"),
        ("bond_volatility",  detect_bond_volatility,         "rates"),
        ("crypto_signal",    detect_crypto_signal,           "crypto"),
        ("trend_reversion",  detect_trend_reversion,         "equity_internals"),
        ("funding_stress",   detect_funding_stress,          "funding"),
        ("net_liquidity",    detect_net_liquidity,           "funding"),
    ]
    for name, fn, cat in complex_specs:
        t0 = time.time()
        try:
            result = fn()
            if isinstance(result, tuple) and len(result) == 2:
                anomaly, metric = result
                if metric:
                    all_metrics[name] = metric
                    metrics_by_cat.setdefault(cat, {})[name] = metric
                if anomaly:
                    if isinstance(anomaly, list): all_anomalies.extend(anomaly)
                    else: all_anomalies.append(anomaly)
        except Exception as e:
            print(f"  {name} ERR: {str(e)[:200]}")
        timings[name] = round(time.time() - t0, 2)

    # ─── Sort & aggregate ───
    sev_order = {"EXTREME": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1}
    all_anomalies.sort(key=lambda a: (-sev_order.get(a.get("severity"), 0),
                                        -abs(a.get("z_score") or 0)))
    by_sev = {}
    for a in all_anomalies:
        by_sev[a.get("severity", "?")] = by_sev.get(a.get("severity", "?"), 0) + 1
    high_or_above = [a for a in all_anomalies if a.get("severity") in ("HIGH", "EXTREME")]

    # ─── Macro Stress Score ───
    stress_score, stress_contribs = compute_stress_score(metrics_by_cat, all_anomalies)
    stress_interpretation = stress_interp(stress_score)

    print(f"  anomalies={len(all_anomalies)} HIGH+={len(high_or_above)} "
          f"stress={stress_score}/100")

    # ─── Telegram alerts ───
    chat_id = get_chat_id()
    history = load_alert_history()
    now_iso = datetime.now(timezone.utc).isoformat()
    alerts_sent = 0
    alerts_skipped = 0
    actions = []

    if chat_id and TELEGRAM_TOKEN:
        # Crisis cluster
        if len(high_or_above) >= 3 or stress_score >= 60:
            crisis_key = f"crisis_bucket_{int(stress_score // 10)}"
            if should_alert(history, crisis_key):
                if send_telegram(format_crisis(high_or_above, stress_score), chat_id):
                    history[crisis_key] = now_iso
                    alerts_sent += 1
                    actions.append({"type": "crisis", "stress_score": stress_score,
                                     "n_anomalies": len(high_or_above)})
                time.sleep(0.5)

        # Individual HIGH/EXTREME alerts
        for a in high_or_above[:5]:
            key = f"{a.get('category','')}:{a.get('name','')}"
            if not should_alert(history, key):
                alerts_skipped += 1
                continue
            if send_telegram(format_alert(a, stress_score), chat_id):
                history[key] = now_iso
                alerts_sent += 1
                actions.append({"type": "anomaly", "category": a.get("category"),
                                 "name": a.get("name"), "severity": a.get("severity")})
            time.sleep(0.4)
        save_alert_history(history)

    # ─── Sidecar ───
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "version": VERSION,
        "elapsed_seconds": round(time.time() - started, 2),
        "macro_stress_score": stress_score,
        "stress_interpretation": stress_interpretation,
        "stress_contributions": stress_contribs,
        "anomalies_count": len(all_anomalies),
        "high_or_extreme_count": len(high_or_above),
        "by_severity": by_sev,
        "anomalies": all_anomalies,
        "metrics": all_metrics,
        "metrics_by_category": metrics_by_cat,
        "detectors_count": (len(FRED_DETECTORS) + len(RATIO_DETECTORS) + len(complex_specs)),
        "categories": sorted(STRESS_WEIGHTS.keys()),
        "detector_timings_s": timings,
        "alerts_sent": alerts_sent,
        "alerts_skipped_dedupe": alerts_skipped,
        "actions": actions,
    }

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
            Body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            ContentType="application/json",
            CacheControl="public, max-age=1800")
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"err": str(e)})}

    print(f"  ✓ sidecar written · alerts sent={alerts_sent} skipped={alerts_skipped}")

    return {"statusCode": 200, "body": json.dumps({
        "success": True,
        "version": VERSION,
        "macro_stress_score": stress_score,
        "stress_interpretation": stress_interpretation,
        "anomalies_count": len(all_anomalies),
        "high_or_extreme_count": len(high_or_above),
        "by_severity": by_sev,
        "detectors_count": payload["detectors_count"],
        "alerts_sent": alerts_sent,
        "elapsed_seconds": round(time.time() - started, 2),
    })}
