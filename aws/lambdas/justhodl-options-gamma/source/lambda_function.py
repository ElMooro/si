"""
justhodl-options-gamma — Dealer gamma exposure (GEX) for major indices

Market makers (the dealers who write the bulk of options) have to
delta-hedge. Their aggregate gamma position determines whether they buy
into rallies and sell into dips (negative gamma — magnifies moves) or
vice versa (positive gamma — dampens moves).

GEX (Gamma Exposure) is a market-maker-positioning estimate that has
historically explained ~60% of intraday vol shape:

  - Positive GEX: dealers BUY into dips, SELL into rallies → vol crushed
  - Negative GEX: dealers SELL into dips, BUY into rallies → vol amplified
  - Zero GEX: pin-risk; markets pin to nearest big strike

You already have a Polygon premium key but only use it for stocks. The
options endpoints are part of the same subscription and unused. This
Lambda exercises them.

Methodology (standard Squeezemetrics formulation):
  GEX = sum over all calls of  (gamma_call * OI_call * 100 * spot * spot * 0.01)
      - sum over all puts of   (gamma_put  * OI_put  * 100 * spot * spot * 0.01)

We pull OI snapshot per strike for the current monthly + next monthly +
quarterly expiries on SPY (most liquid). Black-Scholes gamma computed
locally from IV provided by Polygon.

Output (data/options-gamma.json):
  {
    "generated_at": ...,
    "spy_spot": 552.34,
    "vix": 16.4,
    "expirations": ["2026-05-16", "2026-06-20", ...],
    "total_gex": +1.85e9,             (USD per 1% SPY move)
    "regime": "long_gamma" | "short_gamma" | "neutral",
    "zero_gamma_strike": 549.5,
    "key_strikes": {
      "550": {"call_gex": +0.4e9, "put_gex": -0.2e9, "net": +0.2e9, "oi_call":..., "oi_put":...},
      ...
    },
    "interpretation": "<plain English>"
  }
"""
from __future__ import annotations
import json
import math
import os
import time
import urllib.request
from datetime import datetime, timezone, date
from concurrent.futures import ThreadPoolExecutor

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/options-gamma.json")
POLYGON_KEY = os.environ.get("POLYGON_KEY", "")
USER_AGENT = os.environ.get("USER_AGENT", "JustHodl Research raafouis@gmail.com")
UNDERLYING = os.environ.get("UNDERLYING", "SPY")
MAX_PARALLEL = int(os.environ.get("MAX_PARALLEL", "8"))
POLY_BASE = "https://api.polygon.io"


def _fetch(url: str, timeout: int = 15):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def _poly(path: str, params: dict = None):
    p = dict(params or {})
    p["apiKey"] = POLYGON_KEY
    qs = "&".join(f"{k}={v}" for k, v in p.items())
    url = f"{POLY_BASE}{path}?{qs}"
    return _fetch(url)


def black_scholes_gamma(s, k, t, r, sigma):
    """Standard Black-Scholes gamma. s=spot, k=strike, t=years, r=rfr, sigma=vol."""
    if t <= 0 or sigma <= 0 or s <= 0 or k <= 0:
        return 0.0
    try:
        d1 = (math.log(s / k) + (r + 0.5 * sigma ** 2) * t) / (sigma * math.sqrt(t))
        # Standard normal pdf
        pdf = math.exp(-0.5 * d1 * d1) / math.sqrt(2 * math.pi)
        return pdf / (s * sigma * math.sqrt(t))
    except (ValueError, ZeroDivisionError):
        return 0.0


def fetch_spot_and_vix():
    """Get current SPY price + VIX level."""
    out = {}
    try:
        d = _poly(f"/v2/aggs/ticker/{UNDERLYING}/prev", {})
        out["spot"] = d["results"][0]["c"]
    except Exception as e:
        out["spot_err"] = str(e)

    try:
        d = _poly("/v2/aggs/ticker/I:VIX/prev", {})
        out["vix"] = d["results"][0]["c"]
    except Exception as e:
        out["vix_err"] = str(e)
    return out


def fetch_options_snapshot():
    """Polygon's snapshot endpoint returns full chain with greeks + IV."""
    url = (f"{POLY_BASE}/v3/snapshot/options/{UNDERLYING}"
           f"?limit=250&apiKey={POLYGON_KEY}")
    out = []
    pages = 0
    next_url = url
    while next_url and pages < 10:  # Cap at 10 pages = 2500 contracts
        try:
            req = urllib.request.Request(next_url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=20) as r:
                data = json.loads(r.read())
        except Exception:
            break
        results = data.get("results", [])
        out.extend(results)
        next_url = data.get("next_url")
        if next_url:
            next_url += f"&apiKey={POLYGON_KEY}"
        pages += 1
    return out


def compute_gex(snapshot: list, spot: float, rfr: float = 0.045) -> dict:
    """Aggregate gamma exposure across all contracts."""
    by_expiry = {}
    by_strike = {}
    total_gex = 0.0

    today = date.today()
    for c in snapshot:
        details = c.get("details") or {}
        greeks = c.get("greeks") or {}
        underlying = c.get("underlying_asset") or {}

        try:
            strike = float(details.get("strike_price", 0))
            expiry = details.get("expiration_date")
            ctype = details.get("contract_type", "").lower()
            oi = c.get("open_interest", 0) or 0
            iv = greeks.get("implied_volatility") or c.get("implied_volatility") or 0
            gamma = greeks.get("gamma")
        except Exception:
            continue

        if not strike or not expiry or not ctype or oi == 0:
            continue

        try:
            t_days = (date.fromisoformat(expiry) - today).days
            if t_days <= 0:
                continue
            t_years = t_days / 365
        except Exception:
            continue

        if not gamma:
            gamma = black_scholes_gamma(spot, strike, t_years, rfr, max(iv, 0.05))
        if not gamma or gamma == 0:
            continue

        # GEX per contract = gamma * OI * 100 * spot * spot * 0.01
        # Calls add positive GEX, puts add negative
        gex_contract = gamma * oi * 100 * spot * spot * 0.01
        if ctype == "call":
            gex_contract = +gex_contract
        else:
            gex_contract = -gex_contract

        total_gex += gex_contract

        # Aggregate by expiry
        if expiry not in by_expiry:
            by_expiry[expiry] = 0
        by_expiry[expiry] += gex_contract

        # Aggregate by strike (rounded to 5)
        strike_round = round(strike / 5) * 5
        if strike_round not in by_strike:
            by_strike[strike_round] = {"call_gex": 0, "put_gex": 0, "oi_call": 0, "oi_put": 0}
        if ctype == "call":
            by_strike[strike_round]["call_gex"] += gex_contract
            by_strike[strike_round]["oi_call"] += oi
        else:
            by_strike[strike_round]["put_gex"] += gex_contract  # already negated
            by_strike[strike_round]["oi_put"] += oi

    # Find zero-gamma strike (where cumulative GEX flips sign)
    sorted_strikes = sorted(by_strike.keys())
    cumulative = 0.0
    zero_gamma_strike = None
    for s in sorted_strikes:
        cumulative += by_strike[s]["call_gex"] + by_strike[s]["put_gex"]
        if zero_gamma_strike is None and cumulative >= 0:
            zero_gamma_strike = s

    return {
        "total_gex": round(total_gex, 0),
        "by_expiry": {k: round(v, 0) for k, v in sorted(by_expiry.items())},
        "by_strike": {
            str(int(k)): {
                "call_gex": round(v["call_gex"], 0),
                "put_gex": round(v["put_gex"], 0),
                "net_gex": round(v["call_gex"] + v["put_gex"], 0),
                "oi_call": v["oi_call"],
                "oi_put": v["oi_put"],
            }
            for k, v in sorted(by_strike.items()) if abs(v["call_gex"] + v["put_gex"]) > 1e6
        },
        "zero_gamma_strike": zero_gamma_strike,
        "contracts_analyzed": len(snapshot),
    }


def interpret_gex(gex_total: float, zero_gamma: float, spot: float, vix: float) -> str:
    if gex_total > 5e8:
        regime = "Strongly long gamma"
        impact = "Dealers are net-long gamma. They sell rallies and buy dips, dampening volatility. Range-bound action likely; SPX pins to nearest big strike."
    elif gex_total > 1e8:
        regime = "Modestly long gamma"
        impact = "Dealers slightly damping vol. Trends are short-lived; mean-reversion intraday."
    elif gex_total > -1e8:
        regime = "Neutral gamma"
        impact = "Dealers near zero net positioning. Trends can develop; pin-risk near spot."
    elif gex_total > -5e8:
        regime = "Modestly short gamma"
        impact = "Dealers amplifying moves. Selloffs accelerate; rallies extend further than fundamentals justify."
    else:
        regime = "Strongly short gamma"
        impact = "Dealers must hedge dynamically — buying at higher highs, selling at lower lows. Vol regime is fragile; gap risk in both directions."

    distance = ""
    if zero_gamma and spot:
        diff = (spot - zero_gamma) / spot
        if abs(diff) > 0.01:
            distance = f" Spot is {diff*100:+.1f}% from the zero-gamma strike at {zero_gamma:.0f} — "
            distance += "above (vol-suppressing zone)" if diff > 0 else "below (vol-amplifying zone)"
            distance += "."

    return f"{regime}. {impact}{distance}"


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    started = time.time()

    if not POLYGON_KEY:
        return {"statusCode": 500,
                "body": json.dumps({"error": "POLYGON_KEY not configured"})}

    market = fetch_spot_and_vix()
    spot = market.get("spot")
    vix = market.get("vix") or 16

    # If we can't get spot, write a 'spot_unavailable' marker so the file
    # always exists and downstream consumers know why GEX is stale. Don't
    # silently fail — that creates a 'missing S3 file' alert that masks
    # the real cause (Polygon API issue, key expired, rate limit, etc.)
    if not spot:
        try:
            existing_obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
            existing = json.loads(existing_obj["Body"].read())
        except Exception:
            existing = {}
        marker = {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "underlying": UNDERLYING,
            "spot": None,
            "vix": vix if vix != 16 else None,
            "regime": "spot_unavailable",
            "interpretation": "Could not fetch SPY spot price from Polygon — likely transient API issue or rate limit. GEX cannot be computed; last valid reading preserved.",
            "spot_unavailable": True,
            "last_valid_at": existing.get("generated_at") if existing else None,
            "last_valid_total_gex": existing.get("total_gex") if existing else None,
            "last_valid_regime": existing.get("regime") if existing else None,
            "polygon_errors": {"spot_err": market.get("spot_err"), "vix_err": market.get("vix_err")},
            "fetch_duration_s": round(time.time() - started, 1),
        }
        s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                      Body=json.dumps(marker).encode(),
                      ContentType="application/json", CacheControl="no-cache")
        print(f"GEX: spot fetch failed — wrote spot_unavailable marker. polygon_errors={marker['polygon_errors']}")
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"ok": True, "spot_unavailable": True}),
        }

    snapshot = fetch_options_snapshot()
    if not snapshot:
        # Market is closed (weekend, holiday, or after-hours).
        # Write a "market_closed" marker so:
        #   1. The S3 file always exists (no 'missing' alert in health monitor)
        #   2. Downstream consumers know why GEX data is stale
        #   3. The most recent valid GEX state can be preserved if we choose
        try:
            existing_obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
            existing = json.loads(existing_obj["Body"].read())
        except Exception:
            existing = {}

        marker = {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "underlying": UNDERLYING,
            "spot": spot,
            "vix": vix,
            "regime": "market_closed",
            "interpretation": "Options market closed (weekend / holiday / outside regular hours). GEX cannot be computed live; last valid reading preserved.",
            "market_closed": True,
            "last_valid_at": existing.get("generated_at") if existing else None,
            "last_valid_total_gex": existing.get("total_gex") if existing else None,
            "last_valid_regime": existing.get("regime") if existing else None,
            "fetch_duration_s": round(time.time() - started, 1),
        }
        s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                      Body=json.dumps(marker).encode(),
                      ContentType="application/json", CacheControl="no-cache")
        print(f"GEX: market closed — wrote marker (last valid: {marker['last_valid_at']})")
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"ok": True, "market_closed": True}),
        }

    gex = compute_gex(snapshot, spot)
    interp = interpret_gex(gex["total_gex"], gex["zero_gamma_strike"], spot, vix)

    if gex["total_gex"] > 5e8:
        regime = "long_gamma"
    elif gex["total_gex"] < -5e8:
        regime = "short_gamma"
    else:
        regime = "neutral"

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "underlying": UNDERLYING,
        "spot": spot,
        "vix": vix,
        "regime": regime,
        "interpretation": interp,
        **gex,
        "fetch_duration_s": round(time.time() - started, 1),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                  Body=json.dumps(output).encode(),
                  ContentType="application/json", CacheControl="no-cache")

    print(f"GEX: total={gex['total_gex']:.2e} regime={regime} zero_gamma={gex['zero_gamma_strike']}")
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"ok": True, "regime": regime,
                            "total_gex": gex["total_gex"],
                            "zero_gamma": gex["zero_gamma_strike"]}),
    }
