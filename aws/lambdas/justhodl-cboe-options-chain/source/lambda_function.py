"""justhodl-cboe-options-chain -- Bloomberg parity 5/10.

Fetches CBOE delayed option chains for the core universe and computes
in-house Black-Scholes implied volatility and Greeks (via aws/shared
black_scholes.py), so justhodl.ai owns the math instead of trusting the
vendor's greek numbers. CBOE's own IV/delta/gamma are kept alongside as
a calibration reference (iv_abs_diff, delta_abs_diff).

Data is free and keyless: https://cdn.cboe.com/api/global/delayed_quotes/

Outputs (S3, additive, backward compatible):
  data/cboe-options-chain.json          -- full per-ticker chains + analytics
  data/cboe-options-chain-history.json  -- daily ATM-IV snapshots (append-only)

Fail-soft everywhere: one bad symbol/contract never fails the run.
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timezone

import boto3
from botocore.exceptions import ClientError

import black_scholes

UNIVERSE = ["SPY", "QQQ", "IWM", "_SPX", "_NDX"]
CBOE_URL = "https://cdn.cboe.com/api/global/delayed_quotes/options/{symbol}.json"
USER_AGENT = "JustHodl.ai research contact@justhodl.ai"

# Approximate trailing dividend yields (documented approximations, used
# only for in-house IV/Greeks when the cache feed lacks a live value).
DIV_YIELDS = {"SPY": 0.013, "QQQ": 0.006, "IWM": 0.012, "_SPX": 0.013, "_NDX": 0.006}

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
CHAIN_KEY = os.environ.get("S3_CHAIN_KEY", "data/cboe-options-chain.json")
HISTORY_KEY = "data/cboe-options-chain-history.json"
FRED_CACHE_KEY = "data/fred-cache-secretary.json"

MAX_STRIKE_WINDOW = 0.30  # keep contracts within +/-30% of spot
MAX_SPREAD_FRAC = 0.50    # skip if (ask-bid)/mid > 50%
# Pacing between contract solves. Calibrated 2026-09-30 against the live
# CBOE feed: a Newton-Raphson IV solve costs ~0.1ms of pure local math
# (~52k contracts across the 5-symbol universe solve in ~5s), so the
# original 0.05s pacing would have added ~43 minutes of idle sleep and
# blown the 300s Lambda timeout. 0.001s still yields the GIL ~1000x/sec
# (no CPU spike) while keeping worst-case sleep under ~60s.
PACE_SECONDS = 0.001
RF_FALLBACK = 0.04        # risk-free fallback when FRED cache is missing


def _now_iso() -> str:
    """Current UTC timestamp in ISO-8601."""
    return datetime.now(timezone.utc).isoformat()


def fetch_chain(symbol: str) -> tuple:
    """Fetch the CBOE delayed option chain for one symbol.

    The API returns a wrapper {"data": {...}, "symbol": ..., "timestamp": ...}.
    The data block holds a flat list of contracts under "options" plus
    "current_price" (spot) and "iv30".

    Returns:
        (data_block, quote_time) or (None, None) on any failure.
    """
    url = CBOE_URL.format(symbol=symbol)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError,
            json.JSONDecodeError, UnicodeDecodeError, OSError) as exc:
        print(f"[cboe] fetch failed for {symbol}: {exc}")
        return None, None
    if not isinstance(payload, dict):
        print(f"[cboe] unexpected payload type for {symbol}: {type(payload).__name__}")
        return None, None
    data = payload.get("data")
    if not isinstance(data, dict) or not isinstance(data.get("options"), list):
        print(f"[cboe] missing/invalid data block for {symbol}")
        return None, None
    return data, payload.get("timestamp")


def get_risk_free_rate(s3) -> float:
    """Read the risk-free rate (FEDFUNDS) from the FRED S3 cache.

    The FRED cache stores values in percent; converts to decimal.
    Falls back to RF_FALLBACK when the cache is missing or unreadable.
    """
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=FRED_CACHE_KEY)
        cache = json.loads(obj["Body"].read().decode("utf-8"))
    except (ClientError, json.JSONDecodeError, UnicodeDecodeError,
            KeyError, OSError) as exc:
        print(f"[cboe] FRED cache unavailable, using fallback rate: {exc}")
        return RF_FALLBACK
    # Try the common cache layouts defensively.
    candidates = []
    if isinstance(cache, dict):
        fed = cache.get("FEDFUNDS")
        if isinstance(fed, dict):
            candidates.append(fed.get("latest"))
        series = cache.get("series")
        if isinstance(series, dict) and isinstance(series.get("FEDFUNDS"), dict):
            candidates.append(series["FEDFUNDS"].get("latest"))
    for value in candidates:
        try:
            if value is None:
                continue
            rate = float(value) / 100.0
            if 0.0 <= rate <= 0.25:
                return rate
        except (TypeError, ValueError):
            continue
    print("[cboe] FEDFUNDS not found in FRED cache, using fallback rate")
    return RF_FALLBACK


def _to_float(value) -> float | None:
    """Coerce a value to float, returning None on failure."""
    try:
        if value is None:
            return None
        f = float(value)
        if f != f:  # NaN
            return None
        return f
    except (TypeError, ValueError):
        return None


def solve_chain(symbol: str, chain_data: dict, r: float) -> tuple:
    """Solve in-house IV/Greeks for every tradeable contract in a chain.

    The CBOE data block holds a FLAT list of contracts under "options";
    expiry and put/call come from parsing each contract's OCC symbol.

    Args:
        symbol: ticker in UNIVERSE.
        chain_data: CBOE 'data' block (flat options list + current_price).
        r: risk-free rate (decimal).

    Returns:
        (contracts, stats) where contracts is a list of enriched contract
        dicts (trimmed to the +/-30% strike window) and stats counts
        skipped contracts by reason.
    """
    options = chain_data.get("options")
    spot = _to_float(chain_data.get("current_price"))
    contracts: list = []
    stats = {"seen": 0, "skipped_no_quote": 0, "skipped_wide_spread": 0,
             "skipped_no_iv": 0, "skipped_bad_occ": 0, "skipped_expired": 0,
             "ok": 0}
    if not isinstance(options, list) or spot is None or spot <= 0:
        return contracts, stats

    today = date.today()
    q = DIV_YIELDS.get(symbol, 0.0)

    for raw in options:
        if not isinstance(raw, dict):
            continue
        stats["seen"] += 1
        occ = raw.get("option")
        try:
            parsed = black_scholes.parse_occ_symbol(occ)
        except (ValueError, TypeError):
            stats["skipped_bad_occ"] += 1
            continue
        strike = parsed["strike"]
        # Trim to the strike window before any heavy work.
        if abs(strike / spot - 1.0) > MAX_STRIKE_WINDOW:
            continue
        bid = _to_float(raw.get("bid"))
        ask = _to_float(raw.get("ask"))
        if bid is None or ask is None or bid <= 0:
            stats["skipped_no_quote"] += 1
            continue
        mid = (bid + ask) / 2.0
        if (ask - bid) / mid > MAX_SPREAD_FRAC:
            stats["skipped_wide_spread"] += 1
            continue
        days = (parsed["expiry"] - today).days
        T = days / 365.0
        if T <= 0:
            stats["skipped_expired"] += 1
            continue
        kind = parsed["kind"]
        iv = black_scholes.implied_vol(mid, spot, strike, T, r, q, kind)
        if iv is None:
            stats["skipped_no_iv"] += 1
            continue
        time.sleep(PACE_SECONDS)
        greeks = black_scholes.bs_greeks(spot, strike, T, r, iv, q, kind)
        cboe_iv = _to_float(raw.get("iv"))
        cboe_delta = _to_float(raw.get("delta"))
        cboe_gamma = _to_float(raw.get("gamma"))
        contracts.append({
            "occ": occ,
            "expiry": parsed["expiry"].isoformat(),
            "kind": kind,
            "strike": strike,
            "bid": bid,
            "ask": ask,
            "mid": mid,
            "volume": raw.get("volume"),
            "open_interest": raw.get("open_interest"),
            "theo": _to_float(raw.get("theo")),
            "iv_solved": iv,
            "cboe_iv": cboe_iv,
            "cboe_delta": cboe_delta,
            "cboe_gamma": cboe_gamma,
            "iv_abs_diff": (abs(iv - cboe_iv) if cboe_iv is not None else None),
            "delta_abs_diff": (abs(greeks["delta"] - cboe_delta)
                               if cboe_delta is not None else None),
            "greeks": greeks,
        })
        stats["ok"] += 1

    contracts.sort(key=lambda c: (c["expiry"], c["strike"], c["kind"]))
    return contracts, stats


def derive_analytics(contracts: list, spot: float) -> dict:
    """Derive chain-level analytics from solved contracts.

    Returns:
        dict with atm_iv_by_expiry, skew_25d (nearest expiry),
        put_call_oi, put_call_vol.
    """
    analytics = {"atm_iv_by_expiry": {}, "skew_25d": None,
                 "put_call_oi": None, "put_call_vol": None}
    if not contracts or spot is None or spot <= 0:
        return analytics

    by_expiry: dict = {}
    for c in contracts:
        by_expiry.setdefault(c["expiry"], []).append(c)

    for expiry, bucket in by_expiry.items():
        atm = min(bucket, key=lambda c: abs(c["strike"] - spot))
        analytics["atm_iv_by_expiry"][expiry] = atm["iv_solved"]

    nearest = min(by_expiry)
    put_25 = call_25 = None
    for c in by_expiry[nearest]:
        d = c["greeks"]["delta"]
        if c["kind"] == "put":
            if put_25 is None or abs(d + 0.25) < abs(put_25["greeks"]["delta"] + 0.25):
                put_25 = c
        else:
            if call_25 is None or abs(d - 0.25) < abs(call_25["greeks"]["delta"] - 0.25):
                call_25 = c
    if put_25 is not None and call_25 is not None:
        analytics["skew_25d"] = put_25["iv_solved"] - call_25["iv_solved"]

    put_oi = sum((_to_float(c["open_interest"]) or 0.0) for c in contracts if c["kind"] == "put")
    call_oi = sum((_to_float(c["open_interest"]) or 0.0) for c in contracts if c["kind"] == "call")
    put_vol = sum((_to_float(c["volume"]) or 0.0) for c in contracts if c["kind"] == "put")
    call_vol = sum((_to_float(c["volume"]) or 0.0) for c in contracts if c["kind"] == "call")
    analytics["put_call_oi"] = (put_oi / call_oi) if call_oi > 0 else None
    analytics["put_call_vol"] = (put_vol / call_vol) if call_vol > 0 else None
    return analytics


def _read_json_s3(s3, key: str) -> dict | None:
    """Read and parse a JSON object from S3; None on any failure."""
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except (ClientError, json.JSONDecodeError, UnicodeDecodeError, OSError):
        return None


def _write_json_s3(s3, key: str, payload: dict) -> bool:
    """Write a JSON payload to S3; True on success."""
    try:
        s3.put_object(Bucket=BUCKET, Key=key,
                      Body=json.dumps(payload).encode("utf-8"),
                      ContentType="application/json")
        return True
    except (ClientError, OSError) as exc:
        print(f"[cboe] S3 write failed for {key}: {exc}")
        return False


def lambda_handler(event: dict, context) -> dict:
    """Fetch, solve, and publish CBOE delayed chains with in-house IV/Greeks.

    A 404/unreachable symbol is skipped (never fails the run). Writes the
    full chain artifact plus a daily ATM-IV history snapshot.
    """
    s3 = boto3.client("s3")
    r = get_risk_free_rate(s3)
    generated_at = _now_iso()

    tickers: dict = {}
    stats = {"symbols_ok": 0, "symbols_skipped": 0, "contracts": 0,
             "risk_free_rate": r}
    for symbol in UNIVERSE:
        chain_data, quote_time = fetch_chain(symbol)
        if chain_data is None:
            stats["symbols_skipped"] += 1
            continue
        spot = _to_float(chain_data.get("current_price"))
        contracts, cstats = solve_chain(symbol, chain_data, r)
        analytics = derive_analytics(contracts, spot)
        tickers[symbol] = {
            "spot": spot,
            "quote_time": quote_time,
            "iv30": _to_float(chain_data.get("iv30")),
            "n_contracts": len(contracts),
            "contracts": contracts,
            "analytics": analytics,
            "contract_stats": cstats,
        }
        stats["symbols_ok"] += 1
        stats["contracts"] += len(contracts)

    chain_payload = {
        "generated_at": generated_at,
        "risk_free_rate": r,
        "risk_free_source": "FRED cache FEDFUNDS (fallback 0.04)",
        "universe": UNIVERSE,
        "tickers": tickers,
        "meta": {
            "schema_version": "1.0",
            "source": "CBOE delayed quotes (cdn.cboe.com), 15-min delayed",
            "greeks_convention": "theta per-day, vega per 1 vol point, rho per 1 rate point",
            "disclaimer": "In-house Black-Scholes estimates for research; not investment advice.",
        },
    }
    _write_json_s3(s3, CHAIN_KEY, chain_payload)

    # Daily ATM-IV history: one snapshot per UTC date, updated in place.
    today = date.today().isoformat()
    history = _read_json_s3(s3, HISTORY_KEY) or {"snapshots": []}
    snapshots = [s for s in history.get("snapshots", [])
                 if isinstance(s, dict) and s.get("date") != today]
    snapshot = {"date": today, "generated_at": generated_at}
    for symbol, t in tickers.items():
        exp_iv = t.get("analytics", {}).get("atm_iv_by_expiry", {})
        snapshot[symbol] = {
            "spot": t.get("spot"),
            "iv30": t.get("iv30"),
            "atm_iv_nearest": exp_iv.get(min(exp_iv)) if exp_iv else None,
        }
    snapshots.append(snapshot)
    history = {"snapshots": snapshots[-365:]}
    _write_json_s3(s3, HISTORY_KEY, history)

    stats["generated_at"] = generated_at
    return {"statusCode": 200, "body": json.dumps(stats)}
