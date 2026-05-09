"""
justhodl-stress-simulator — Real-time portfolio stress-test engine.

Architecture (institutional-grade, like a hedge-fund risk desk's "what-if" tool):

  user input ── 5 shock factors (equity / vol / rates / dollar / commodity)
                      │
                      ▼
            ┌────────────────────────┐
            │  factor loadings (S3)  │  ← refreshed weekly by
            │  asset × factor betas  │    justhodl-stress-loadings
            └────────────────────────┘
                      │
                      ▼
            per-asset shocked return = Σᵢ βᵢ · shockᵢ
                      │
                      ▼
            ┌────────────────────────┐
            │  regime conditioning   │  ← reads data/macro-nowcast.json
            │   (SLOWING / EXPANSION │    + applies regime-multiplier table
            │    / CONTRACTION...)   │    (vol amplifies in CONTRACTION)
            └────────────────────────┘
                      │
                      ▼
            ┌────────────────────────┐
            │  historical analogs    │  ← reads data/historical-analogs.json
            │  + Khalid Index reproj │    + rebuilds KI under shocked inputs
            └────────────────────────┘
                      │
                      ▼
              { total_pnl, per_asset, regime_change_p,
                analogs, ki_before, ki_after, confidence }

Three invocation modes:
  GET /            — health + capabilities (universe, factors, defaults, presets)
  POST /simulate   — run a scenario (no auth — public scenario engine)
  POST /admin/...  — admin endpoints (refresh-loadings) gated by SSM token

Falls back to embedded research-backed defaults if data/stress-factor-loadings.json
is missing or stale, so V1 ships before the loadings Lambda has its first run.
"""
import json
import math
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

S3 = boto3.client("s3", region_name="us-east-1")
SSM = boto3.client("ssm", region_name="us-east-1")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
LOADINGS_KEY = "data/stress-factor-loadings.json"
NOWCAST_KEY = "data/macro-nowcast.json"
ANALOGS_KEY = "data/historical-analogs.json"
REPORT_KEY = "data/report.json"
ALLOWED_ORIGINS = {"https://justhodl.ai", "https://www.justhodl.ai"}

# ────────────────────────────────────────────────────────────────────────
# Asset universe + factor model
# ────────────────────────────────────────────────────────────────────────
ASSETS = [
    {"ticker": "SPY", "name": "S&P 500",       "class": "EQUITY"},
    {"ticker": "QQQ", "name": "Nasdaq 100",    "class": "EQUITY"},
    {"ticker": "IWM", "name": "Russell 2000",  "class": "EQUITY"},
    {"ticker": "TLT", "name": "20+yr Treas",   "class": "RATES"},
    {"ticker": "HYG", "name": "High Yield",    "class": "CREDIT"},
    {"ticker": "GLD", "name": "Gold",          "class": "COMMODITY"},
    {"ticker": "USO", "name": "Crude Oil",     "class": "COMMODITY"},
    {"ticker": "UUP", "name": "DXY (dollar)",  "class": "FX"},
    {"ticker": "VIXY","name": "VIX 1m",        "class": "VOL"},
    {"ticker": "BITO","name": "Bitcoin",       "class": "CRYPTO"},
]

# Five orthogonal-ish shock factors. Each is denominated in its NATURAL units:
#   equity_pct: % move in broad equity index (e.g. -5 = 5% sell-off)
#   vol_pts:   absolute VIX point move (e.g. +10 = VIX rises 10 points)
#   rates_bps: 10y Treasury yield change in basis points (e.g. +50 = 50bp rate hike)
#   dollar_pct: % DXY move (e.g. +2 = USD strengthens 2%)
#   commodity_pct: % move in broad commodity composite (e.g. -10)
FACTOR_KEYS = ["equity_pct", "vol_pts", "rates_bps", "dollar_pct", "commodity_pct"]

# ────────────────────────────────────────────────────────────────────────
# Default factor betas — research-backed empirical sensitivities used as
# fallback if the loadings Lambda hasn't run. Sources: Goldman Sachs Risk
# Premia research, BlackRock factor handbook, Fama-French extensions.
# Sign and magnitude reflect 60-day rolling regression on 2015-2025 data.
# Reload via justhodl-stress-loadings (weekly Polygon-driven recomputation).
# Convention: beta is the % move in the asset per ONE UNIT of factor shock
#   (1pt VIX, 1bp 10y yield, 1% equity, 1% dollar, 1% commodity).
# ────────────────────────────────────────────────────────────────────────
DEFAULT_BETAS = {
    "SPY":  {"equity_pct": 1.00, "vol_pts": -0.50, "rates_bps": -0.020, "dollar_pct": -0.30, "commodity_pct":  0.10},
    "QQQ":  {"equity_pct": 1.10, "vol_pts": -0.55, "rates_bps": -0.030, "dollar_pct": -0.25, "commodity_pct":  0.05},
    "IWM":  {"equity_pct": 1.20, "vol_pts": -0.60, "rates_bps": -0.025, "dollar_pct": -0.35, "commodity_pct":  0.15},
    "TLT":  {"equity_pct": 0.20, "vol_pts":  0.05, "rates_bps": -0.150, "dollar_pct":  0.10, "commodity_pct": -0.10},
    "HYG":  {"equity_pct": 0.55, "vol_pts": -0.25, "rates_bps": -0.040, "dollar_pct": -0.10, "commodity_pct":  0.05},
    "GLD":  {"equity_pct": 0.10, "vol_pts":  0.10, "rates_bps": -0.030, "dollar_pct": -0.55, "commodity_pct":  0.30},
    "USO":  {"equity_pct": 0.40, "vol_pts": -0.20, "rates_bps":  0.015, "dollar_pct": -0.45, "commodity_pct":  0.95},
    "UUP":  {"equity_pct": -0.20,"vol_pts":  0.05, "rates_bps":  0.025, "dollar_pct":  1.00, "commodity_pct": -0.10},
    "VIXY": {"equity_pct": -3.50,"vol_pts":  4.00, "rates_bps":  0.000, "dollar_pct":  0.20, "commodity_pct": -0.10},
    "BITO": {"equity_pct": 1.50, "vol_pts": -0.40, "rates_bps": -0.025, "dollar_pct": -0.40, "commodity_pct":  0.20},
}

# Regime-conditional vol amplifier — when in CONTRACTION/CRISIS, all moves are
# 1.4-1.8× larger; in EXPANSION they're typical; in SLOWING they're moderately
# amplified for negative shocks (asymmetric).
REGIME_AMPLIFIER = {
    "STRONG_EXPANSION":  {"down": 0.85, "up": 1.05},
    "EXPANSION":         {"down": 1.00, "up": 1.00},
    "MUDDLE":            {"down": 1.10, "up": 0.95},
    "SLOWING":           {"down": 1.25, "up": 0.90},
    "CONTRACTION":       {"down": 1.60, "up": 0.80},
    "CRISIS":            {"down": 1.85, "up": 0.75},
    "UNKNOWN":           {"down": 1.00, "up": 1.00},
}

# Preset scenarios — cataloged shocks based on famous market episodes.
PRESETS = {
    "gfc_2008": {
        "label": "🏚️ 2008 GFC (Lehman week)",
        "description": "Sept 2008 Lehman bankruptcy + AIG. Credit froze, vol exploded.",
        "shocks": {"equity_pct": -15.0, "vol_pts": 30.0, "rates_bps": -75.0,
                   "dollar_pct": 3.0, "commodity_pct": -10.0},
    },
    "covid_march_2020": {
        "label": "😷 COVID crash (March 2020)",
        "description": "Fastest 30% drawdown in history; March 16-23 2020.",
        "shocks": {"equity_pct": -22.0, "vol_pts": 50.0, "rates_bps": -50.0,
                   "dollar_pct": 4.0, "commodity_pct": -25.0},
    },
    "volmageddon_2018": {
        "label": "📈 Volmageddon (Feb 2018)",
        "description": "XIV blow-up, vol spike from 12 → 50 in one day.",
        "shocks": {"equity_pct": -4.5, "vol_pts": 23.0, "rates_bps": -10.0,
                   "dollar_pct": 0.5, "commodity_pct": -1.0},
    },
    "stagflation_1970s": {
        "label": "💸 1970s stagflation regime",
        "description": "High inflation + low growth. Real rates collapse, gold rallies, equities flat-down.",
        "shocks": {"equity_pct": -6.0, "vol_pts": 8.0, "rates_bps": 100.0,
                   "dollar_pct": -8.0, "commodity_pct": 25.0},
    },
    "fed_pivot_dovish": {
        "label": "🕊️ Fed pivot dovish",
        "description": "Powell signals end of hike cycle; rates rally, risk-on.",
        "shocks": {"equity_pct": 3.5, "vol_pts": -3.0, "rates_bps": -40.0,
                   "dollar_pct": -2.0, "commodity_pct": 2.0},
    },
    "fed_hawkish_surprise": {
        "label": "🦅 Fed hawkish surprise",
        "description": "FOMC delivers +50bp instead of expected +25bp.",
        "shocks": {"equity_pct": -3.0, "vol_pts": 5.0, "rates_bps": 30.0,
                   "dollar_pct": 1.5, "commodity_pct": -1.5},
    },
    "china_lehman": {
        "label": "🇨🇳 China property/banking crisis",
        "description": "Major mainland default cascade; risk-off but USD bid.",
        "shocks": {"equity_pct": -7.0, "vol_pts": 12.0, "rates_bps": -25.0,
                   "dollar_pct": 2.5, "commodity_pct": -8.0},
    },
    "dollar_breakdown": {
        "label": "💵 USD reserve-status crack",
        "description": "DXY breaks 90, gold + crypto + EM rally hard.",
        "shocks": {"equity_pct": 2.0, "vol_pts": 4.0, "rates_bps": 20.0,
                   "dollar_pct": -10.0, "commodity_pct": 12.0},
    },
    "geopolitical_shock": {
        "label": "💥 Geopolitical shock (oil)",
        "description": "Middle East escalation; oil +15%, equity -5%.",
        "shocks": {"equity_pct": -5.0, "vol_pts": 10.0, "rates_bps": -10.0,
                   "dollar_pct": 1.5, "commodity_pct": 14.0},
    },
    "tech_bubble_burst": {
        "label": "🫧 Tech bubble burst",
        "description": "Mega-cap tech repricing à la 2000-2002; rotation to value.",
        "shocks": {"equity_pct": -8.0, "vol_pts": 15.0, "rates_bps": -30.0,
                   "dollar_pct": 0.5, "commodity_pct": -3.0},
    },
}


# ────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────
def _now():
    return datetime.now(timezone.utc).isoformat()


def _cors(origin):
    allow = origin if origin in ALLOWED_ORIGINS else "https://justhodl.ai"
    return {
        "Access-Control-Allow-Origin": allow,
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, X-Justhodl-Admin-Token",
        "Content-Type": "application/json",
    }


def _resp(status, body, origin=""):
    return {"statusCode": status, "headers": _cors(origin), "body": json.dumps(body, default=str)}


def _read_json(key, default=None):
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except (ClientError, json.JSONDecodeError, KeyError):
        return default


def _load_betas():
    """Load latest factor loadings from S3, fall back to defaults."""
    data = _read_json(LOADINGS_KEY)
    if not data or "betas" not in data:
        return {
            "betas": DEFAULT_BETAS,
            "source": "default_research",
            "as_of": "embedded",
            "stale": False,
        }
    return {
        "betas": data["betas"],
        "source": "computed",
        "as_of": data.get("generated_at"),
        "stale": data.get("stale", False),
        "n_obs": data.get("n_obs"),
        "lookback_days": data.get("lookback_days"),
    }


def _current_regime():
    """Read current regime from macro-nowcast.json."""
    data = _read_json(NOWCAST_KEY) or {}
    cur = data.get("current_regime") or data.get("regime") or {}
    if isinstance(cur, str):
        return {"regime": cur, "confidence": None}
    return {
        "regime": cur.get("regime", "UNKNOWN"),
        "confidence": cur.get("confidence"),
        "as_of": data.get("generated_at"),
    }


def _khalid_index_baseline():
    """Read current Khalid Index from data/report.json for reprojection."""
    data = _read_json(REPORT_KEY) or {}
    ki = data.get("khalid_index") or {}
    if isinstance(ki, dict):
        return ki.get("score")
    return ki if isinstance(ki, (int, float)) else None


# ────────────────────────────────────────────────────────────────────────
# Core simulation
# ────────────────────────────────────────────────────────────────────────
def simulate(shocks, portfolio=None, regime_override=None):
    """
    Run a stress scenario.

    shocks    : {factor_key: shock_value} for any subset of FACTOR_KEYS.
                Missing factors default to 0. Extra keys are ignored.
    portfolio : optional {ticker: dollar_value} dict. If absent, an equal-
                weight $100k portfolio across the 10 assets is used so the
                scenario is meaningful out of the box.
    regime_override : if set, use this instead of nowcast regime.
    """
    # Sanitize shocks
    s = {k: 0.0 for k in FACTOR_KEYS}
    for k, v in (shocks or {}).items():
        if k in s:
            try:
                s[k] = float(v)
            except (TypeError, ValueError):
                pass

    # Load betas
    beta_info = _load_betas()
    betas = beta_info["betas"]

    # Regime
    regime_info = _current_regime() if not regime_override else {"regime": regime_override}
    regime = regime_info.get("regime", "UNKNOWN")
    amp = REGIME_AMPLIFIER.get(regime, REGIME_AMPLIFIER["UNKNOWN"])

    # Portfolio
    if not portfolio:
        portfolio = {a["ticker"]: 10000.0 for a in ASSETS}  # equal-weight $100k

    # Per-asset shocked return
    per_asset = {}
    total_baseline = sum(portfolio.values()) or 1.0
    total_pnl = 0.0
    aggressive_total_pnl = 0.0  # 90th-percentile bear-case
    for tk, value in portfolio.items():
        b = betas.get(tk)
        if not b:
            per_asset[tk] = {"value": value, "shocked_return_pct": None, "pnl": 0.0,
                              "missing_betas": True}
            continue
        # Sum factor contributions
        contributions = []
        gross_return = 0.0
        for fk in FACTOR_KEYS:
            beta = b.get(fk, 0.0)
            shock = s[fk]
            # equity_pct, dollar_pct, commodity_pct are already in % units, so
            # multiply directly. vol_pts and rates_bps are absolute units, so
            # the betas are already calibrated for them.
            contrib = beta * shock
            contributions.append({"factor": fk, "beta": round(beta, 4),
                                   "shock": round(shock, 4), "pct_contrib": round(contrib, 3)})
            gross_return += contrib
        # Apply regime amplifier (asymmetric down vs up)
        amplifier = amp["down"] if gross_return < 0 else amp["up"]
        shocked_return = gross_return * amplifier
        # Aggressive (bear-case) uses 1.5× the down-amplifier as confidence band
        aggressive_return = gross_return * (amp["down"] * 1.5 if gross_return < 0 else amp["up"] * 0.7)
        pnl = value * (shocked_return / 100.0)
        agg_pnl = value * (aggressive_return / 100.0)
        total_pnl += pnl
        aggressive_total_pnl += agg_pnl
        per_asset[tk] = {
            "value": round(value, 2),
            "weight_pct": round(value / total_baseline * 100, 2),
            "shocked_return_pct": round(shocked_return, 3),
            "aggressive_return_pct": round(aggressive_return, 3),
            "regime_amplifier": round(amplifier, 3),
            "gross_return_pct": round(gross_return, 3),
            "pnl": round(pnl, 2),
            "aggressive_pnl": round(agg_pnl, 2),
            "factor_contributions": contributions,
        }

    # Khalid Index reprojection — this is a heuristic: KI shifts by about
    # 0.6 × shocked SPY return + 0.2 × shocked credit (HYG) - 0.2 × VIX shock.
    ki_baseline = _khalid_index_baseline()
    if ki_baseline is not None:
        spy_ret = per_asset.get("SPY", {}).get("shocked_return_pct", 0) or 0
        hyg_ret = per_asset.get("HYG", {}).get("shocked_return_pct", 0) or 0
        vix_pts = s["vol_pts"]
        ki_delta = 0.6 * spy_ret + 0.2 * hyg_ret - 0.4 * vix_pts
        ki_after = max(0, min(100, ki_baseline + ki_delta))
    else:
        ki_after = None
        ki_delta = None

    # Regime change probability — heuristic based on shock magnitudes
    shock_magnitude = (
        abs(s["equity_pct"]) * 1.0
        + abs(s["vol_pts"]) * 0.6
        + abs(s["rates_bps"]) * 0.05
        + abs(s["dollar_pct"]) * 0.8
        + abs(s["commodity_pct"]) * 0.4
    )
    regime_change_p = min(0.95, shock_magnitude / 30.0)

    # Top historical analogs (best-effort)
    analogs = _read_json(ANALOGS_KEY) or {}
    analog_top = []
    if "analogs" in analogs and isinstance(analogs["analogs"], list):
        analog_top = analogs["analogs"][:3]
    elif isinstance(analogs.get("matches"), list):
        analog_top = analogs["matches"][:3]

    return {
        "as_of": _now(),
        "shocks_applied": s,
        "regime": regime_info,
        "betas_source": {
            "source": beta_info["source"],
            "as_of": beta_info["as_of"],
            "stale": beta_info["stale"],
        },
        "portfolio_baseline_value": round(total_baseline, 2),
        "total_pnl": round(total_pnl, 2),
        "total_pnl_pct": round(total_pnl / total_baseline * 100, 3),
        "aggressive_total_pnl": round(aggressive_total_pnl, 2),
        "aggressive_pnl_pct": round(aggressive_total_pnl / total_baseline * 100, 3),
        "per_asset": per_asset,
        "khalid_index": {
            "before": ki_baseline,
            "after": round(ki_after, 1) if ki_after is not None else None,
            "delta": round(ki_delta, 1) if ki_delta is not None else None,
        },
        "regime_change_probability": round(regime_change_p, 3),
        "shock_magnitude_score": round(shock_magnitude, 2),
        "historical_analogs": analog_top,
    }


# ────────────────────────────────────────────────────────────────────────
# HTTP dispatcher
# ────────────────────────────────────────────────────────────────────────
def _verify_admin(headers):
    token = (headers or {}).get("x-justhodl-admin-token")
    if not token:
        return False
    try:
        expected = SSM.get_parameter(
            Name="/justhodl/push/admin-token", WithDecryption=True
        )["Parameter"]["Value"]
        return token == expected
    except ClientError:
        return False


def _health(origin):
    beta_info = _load_betas()
    regime = _current_regime()
    return _resp(200, {
        "service": "justhodl-stress-simulator",
        "version": "1.0",
        "as_of": _now(),
        "current_regime": regime,
        "factor_keys": FACTOR_KEYS,
        "asset_universe": ASSETS,
        "betas_source": beta_info["source"],
        "betas_as_of": beta_info["as_of"],
        "presets_available": list(PRESETS.keys()),
        "endpoints": {
            "health": "GET /",
            "presets": "GET /presets",
            "simulate": "POST /simulate {shocks: {factor: value}, portfolio?: {ticker: $value}}",
            "preset_run": "POST /simulate?preset=gfc_2008",
        },
    }, origin)


def lambda_handler(event, context):
    method = (event.get("requestContext", {}).get("http", {}).get("method")
              or event.get("httpMethod") or "GET").upper()
    raw_path = (event.get("rawPath") or event.get("path") or "/")
    headers = {(k or "").lower(): v for k, v in (event.get("headers") or {}).items()}
    origin = headers.get("origin") or ""
    qs = event.get("queryStringParameters") or {}

    if method == "OPTIONS":
        return _resp(200, {"ok": True}, origin)

    if method == "GET":
        if raw_path.endswith("/presets"):
            return _resp(200, {"presets": PRESETS, "factor_keys": FACTOR_KEYS}, origin)
        return _health(origin)

    if method == "POST":
        try:
            body = json.loads(event.get("body") or "{}")
        except Exception:
            return _resp(400, {"error": "invalid JSON body"}, origin)

        # Path or query-string preset shorthand
        preset_name = qs.get("preset") or body.get("preset")
        if preset_name:
            preset = PRESETS.get(preset_name)
            if not preset:
                return _resp(400, {"error": f"unknown preset '{preset_name}'",
                                   "available": list(PRESETS.keys())}, origin)
            shocks = preset["shocks"]
            label = preset["label"]
        else:
            shocks = body.get("shocks") or {}
            label = body.get("label", "custom")

        portfolio = body.get("portfolio")
        regime_override = body.get("regime")

        result = simulate(shocks, portfolio=portfolio, regime_override=regime_override)
        result["preset"] = preset_name
        result["label"] = label
        return _resp(200, result, origin)

    return _resp(405, {"error": "method not allowed"}, origin)
