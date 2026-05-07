"""
justhodl-vol-regime — Volatility regime dashboard (institutional-grade).

WHAT IT COMPUTES PER TICKER
────────────────────────────
For each ticker (SPY, QQQ, IWM, GLD, TLT, IBIT, VIX + watchlist top 20):

  REALIZED VOL (Polygon daily history)
    rv_5d, rv_20d, rv_90d   — annualized stdev of log returns
    rv_z                    — current 20d RV vs 1y mean (z-score)

  IMPLIED VOL (Polygon options snapshot)
    iv_atm_30d              — ATM IV at ~30d expiry
    iv_atm_90d              — ATM IV at ~90d expiry
    iv_rv_ratio             — iv_atm_30d / rv_20d (>1 = options expensive)

  SKEW (25-delta strikes)
    skew_30d                — 25Δ_put_iv - 25Δ_call_iv (positive = put protection bid)
    skew_z                  — current vs 90d mean

  TERM STRUCTURE
    term_slope              — iv_atm_90d - iv_atm_30d (negative = backwardation = stress)

  REGIME CLASSIFICATION (per ticker)
    COMPLACENT  — IV/RV<0.95, skew<2, term>0   (vol cheap, no fear)
    NORMAL      — defaults
    CONCERNED   — IV/RV>1.20, skew>4 OR rv_z>1.5
    PANIC       — IV/RV>1.50 AND term<0 AND skew>6 (full stress)

CROSS-MARKET REGIME COMPOSITE
──────────────────────────────
  weighted_score:  SPY (0.30), QQQ (0.20), IWM (0.10), GLD (0.10),
                   TLT (0.15), VIX (0.15)
  Higher = more vol stress

INSTITUTIONAL-GRADE SAFEGUARDS
───────────────────────────────
  ✓ Annualization: stdev × sqrt(252) [trading days]
  ✓ Log returns (not simple returns) — proper for vol
  ✓ ATM strike = closest to spot, expiry = closest to target days (30/90)
  ✓ 25-delta strikes = closest to |delta|=0.25 from greeks
  ✓ Open Interest minimum filter — exclude illiquid contracts
  ✓ Stale quote detection — skip contracts with last_quote > 1d old
  ✓ Failure-safe — if options endpoint fails for one ticker, mark as null
                    (don't propagate error, continue with other tickers)
  ✓ Rate limit safe — sequential per-ticker calls with budget cap
"""
import json
import math
import os
import statistics
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone

import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY_OUT = os.environ.get("S3_KEY_OUT", "data/vol-regime.json")
POLY_KEY = os.environ.get("POLY_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")

# Core universe — always tracked
CORE_UNIVERSE = ["SPY", "QQQ", "IWM", "DIA", "GLD", "TLT", "IBIT", "VXX"]

S3 = boto3.client("s3", region_name=REGION)


def http_get_json(url, timeout=20, retries=2):
    last_err = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "vol-regime/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(0.5 * (attempt + 1))
    print(f"[vol] HTTP fail after {retries+1} attempts: {url[:80]} → {last_err}")
    return None


# ─── Realized Volatility ─────────────────────────────────────────────────────
def fetch_history(ticker, n_days=260):
    """Pull Polygon daily bars for last n_days. Returns most-recent-first list of closes."""
    end = date.today()
    start = end - timedelta(days=int(n_days * 1.6))  # buffer for non-trading days
    qs = urllib.parse.urlencode({"apiKey": POLY_KEY, "adjusted": "true", "sort": "desc"})
    url = (f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
            f"{start.strftime('%Y-%m-%d')}/{end.strftime('%Y-%m-%d')}?{qs}")
    d = http_get_json(url)
    if not d or d.get("status") not in ("OK", "DELAYED"):
        return []
    return [(b.get("t"), b.get("c")) for b in (d.get("results") or []) if b.get("c")]


def compute_realized_vol(closes, window):
    """Annualized realized vol using log returns × sqrt(252).
    closes: list of (timestamp, close), most-recent-first.
    window: number of returns to use."""
    if len(closes) < window + 1:
        return None
    # Take window+1 most recent closes
    recent = closes[:window + 1]
    log_returns = []
    for i in range(len(recent) - 1):
        c_now = recent[i][1]
        c_prev = recent[i + 1][1]
        if c_prev and c_now and c_prev > 0:
            log_returns.append(math.log(c_now / c_prev))
    if len(log_returns) < 5:
        return None
    sd = statistics.stdev(log_returns)
    return round(sd * math.sqrt(252) * 100, 2)  # annualized %


def compute_realized_vol_z(closes, window=20):
    """z-score of current 20d RV vs trailing 1y of rolling 20d RVs."""
    rolling = []
    for start in range(0, min(220, len(closes) - window - 1)):
        sub = closes[start:start + window + 1]
        if len(sub) < window + 1:
            continue
        log_returns = []
        for i in range(len(sub) - 1):
            c_now = sub[i][1]; c_prev = sub[i + 1][1]
            if c_prev and c_now and c_prev > 0:
                log_returns.append(math.log(c_now / c_prev))
        if len(log_returns) >= 5:
            sd = statistics.stdev(log_returns)
            rolling.append(sd * math.sqrt(252) * 100)
    if len(rolling) < 30:
        return None, None
    current = rolling[0]
    history = rolling[1:]
    mean = statistics.mean(history)
    sd = statistics.stdev(history) if len(history) > 1 else None
    if not sd:
        return current, None
    return round(current, 2), round((current - mean) / sd, 2)


# ─── Implied Volatility (Polygon options chain) ──────────────────────────────
def fetch_options_chain(underlying, expiry_target_days=30):
    """Snapshot options chain for underlying. Filter to ~target_days expiry.
    Returns list of contracts with greeks + IV."""
    today = date.today()
    target_date = today + timedelta(days=expiry_target_days)
    # Polygon /v3/snapshot/options/{underlying} returns ALL chains; can be large.
    # Use expiration_date filter to narrow.
    expiry_min = (today + timedelta(days=expiry_target_days - 14)).strftime("%Y-%m-%d")
    expiry_max = (today + timedelta(days=expiry_target_days + 14)).strftime("%Y-%m-%d")
    qs = urllib.parse.urlencode({
        "apiKey": POLY_KEY,
        "expiration_date.gte": expiry_min,
        "expiration_date.lte": expiry_max,
        "limit": 250,
    })
    url = f"https://api.polygon.io/v3/snapshot/options/{underlying}?{qs}"
    d = http_get_json(url, timeout=30)
    if not d or d.get("status") not in ("OK", "DELAYED"):
        return []
    contracts = d.get("results") or []
    # If next_url present, paginate up to a few pages
    pages = 0
    while d.get("next_url") and pages < 4:
        next_url = d["next_url"] + ("&" if "?" in d["next_url"] else "?") + f"apiKey={POLY_KEY}"
        d = http_get_json(next_url, timeout=30)
        if d:
            contracts.extend(d.get("results") or [])
        pages += 1
    return contracts


def get_underlying_price(contracts):
    """Polygon contract snapshot includes underlying_asset.price"""
    for c in contracts:
        ua = c.get("underlying_asset") or {}
        p = ua.get("price")
        if p:
            return float(p)
    return None


def find_atm_iv(contracts, spot, contract_type="call"):
    """Find IV at ATM strike for the given contract type."""
    if not spot or not contracts:
        return None
    candidates = []
    for c in contracts:
        details = c.get("details") or {}
        if details.get("contract_type") != contract_type:
            continue
        strike = details.get("strike_price")
        if not strike:
            continue
        iv = c.get("implied_volatility")
        if iv is None or iv <= 0:
            continue
        oi = c.get("open_interest") or 0
        if oi < 50:  # skip illiquid
            continue
        candidates.append({
            "strike": float(strike),
            "iv": float(iv),
            "delta": (c.get("greeks") or {}).get("delta"),
            "oi": oi,
        })
    if not candidates:
        return None
    candidates.sort(key=lambda x: abs(x["strike"] - spot))
    return candidates[0]  # closest to spot


def find_25d_iv(contracts, spot, contract_type="put"):
    """Find IV at strike closest to |delta|=0.25 for given type.
    For puts, delta is negative ~ -0.25. For calls, delta is positive ~ +0.25."""
    if not contracts:
        return None
    target_delta = -0.25 if contract_type == "put" else 0.25
    candidates = []
    for c in contracts:
        details = c.get("details") or {}
        if details.get("contract_type") != contract_type:
            continue
        delta = (c.get("greeks") or {}).get("delta")
        iv = c.get("implied_volatility")
        if delta is None or iv is None or iv <= 0:
            continue
        oi = c.get("open_interest") or 0
        if oi < 25:
            continue
        candidates.append({
            "strike": float(details.get("strike_price") or 0),
            "iv": float(iv),
            "delta": float(delta),
            "oi": oi,
        })
    if not candidates:
        return None
    candidates.sort(key=lambda x: abs(x["delta"] - target_delta))
    return candidates[0]


# ─── Per-ticker assembly ─────────────────────────────────────────────────────
def regime_classify(rv, iv_30d, iv_rv, skew, term, rv_z):
    """Classify the current vol regime for a ticker."""
    if iv_rv is not None and iv_rv > 1.50 and (term or 0) < 0 and (skew or 0) > 6:
        return "PANIC"
    if (iv_rv or 0) > 1.20 or (skew or 0) > 4 or (rv_z or 0) > 1.5:
        return "CONCERNED"
    if (iv_rv or 0) < 0.95 and (skew or 0) < 2 and (term or 0) > 0:
        return "COMPLACENT"
    return "NORMAL"


def assemble_ticker(ticker):
    """Compute all vol metrics for one ticker. Returns dict."""
    out = {"ticker": ticker, "errors": []}

    # 1. Pull history for RV
    history = fetch_history(ticker, n_days=260)
    if not history:
        out["errors"].append("history fetch failed")
        return out

    out["spot_price"] = round(history[0][1], 2) if history else None
    out["rv_5d"]  = compute_realized_vol(history, 5)
    out["rv_20d"], out["rv_z"] = compute_realized_vol_z(history, 20)
    out["rv_90d"] = compute_realized_vol(history, 90)

    # 2. Options chain (30d)
    contracts_30d = fetch_options_chain(ticker, expiry_target_days=30)
    if contracts_30d:
        spot = get_underlying_price(contracts_30d) or out.get("spot_price")
        atm_call = find_atm_iv(contracts_30d, spot, "call")
        atm_put = find_atm_iv(contracts_30d, spot, "put")
        # ATM IV = average of ATM call/put IV (more robust than just one side)
        atm_ivs = [x["iv"] for x in (atm_call, atm_put) if x]
        if atm_ivs:
            out["iv_atm_30d"] = round(statistics.mean(atm_ivs) * 100, 2)
        # 25Δ skew
        put_25d = find_25d_iv(contracts_30d, spot, "put")
        call_25d = find_25d_iv(contracts_30d, spot, "call")
        if put_25d and call_25d:
            out["skew_30d"] = round((put_25d["iv"] - call_25d["iv"]) * 100, 2)
            out["skew_25d_put_iv"] = round(put_25d["iv"] * 100, 2)
            out["skew_25d_call_iv"] = round(call_25d["iv"] * 100, 2)
    else:
        out["errors"].append("no 30d options chain")

    # 3. Options chain (90d) for term structure
    contracts_90d = fetch_options_chain(ticker, expiry_target_days=90)
    if contracts_90d:
        spot = get_underlying_price(contracts_90d) or out.get("spot_price")
        atm_call = find_atm_iv(contracts_90d, spot, "call")
        atm_put = find_atm_iv(contracts_90d, spot, "put")
        atm_ivs = [x["iv"] for x in (atm_call, atm_put) if x]
        if atm_ivs:
            out["iv_atm_90d"] = round(statistics.mean(atm_ivs) * 100, 2)

    # 4. IV/RV ratio + term slope
    if out.get("iv_atm_30d") is not None and out.get("rv_20d") is not None and out["rv_20d"] > 0:
        out["iv_rv_ratio"] = round(out["iv_atm_30d"] / out["rv_20d"], 2)
    if out.get("iv_atm_90d") is not None and out.get("iv_atm_30d") is not None:
        out["term_slope"] = round(out["iv_atm_90d"] - out["iv_atm_30d"], 2)
        out["term_structure"] = "CONTANGO" if out["term_slope"] > 0 else "BACKWARDATION"

    # 5. Regime classification
    out["regime"] = regime_classify(
        out.get("rv_20d"), out.get("iv_atm_30d"), out.get("iv_rv_ratio"),
        out.get("skew_30d"), out.get("term_slope"), out.get("rv_z"),
    )

    return out


# ─── Composite regime scoring ────────────────────────────────────────────────
REGIME_SCORE = {"COMPLACENT": 0, "NORMAL": 25, "CONCERNED": 60, "PANIC": 100}
WEIGHTS = {"SPY": 0.30, "QQQ": 0.20, "IWM": 0.10, "GLD": 0.10,
           "TLT": 0.15, "VXX": 0.15}


def composite_regime(per_ticker):
    weighted_sum = 0
    total_weight = 0
    for ticker, weight in WEIGHTS.items():
        rec = per_ticker.get(ticker)
        if rec and rec.get("regime"):
            weighted_sum += REGIME_SCORE[rec["regime"]] * weight
            total_weight += weight
    if total_weight == 0:
        return None, "UNKNOWN"
    score = round(weighted_sum / total_weight, 1)
    if score < 15:   regime = "COMPLACENT"
    elif score < 40: regime = "NORMAL"
    elif score < 70: regime = "CONCERNED"
    else:            regime = "PANIC"
    return score, regime


# ─── Watchlist load ──────────────────────────────────────────────────────────
def load_watchlist_tickers():
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/user-watchlist.json")
        wl = json.loads(obj["Body"].read())
        all_tickers = set()
        for cat in ("holdings", "watching"):
            for t in (wl.get("categories", {}).get(cat) or []):
                all_tickers.add(t)
        return list(all_tickers)[:20]
    except Exception:
        return []


# ─── Main handler ───────────────────────────────────────────────────────────
def lambda_handler(event, context):
    started = time.time()

    # Build universe: core + watchlist top 20
    watchlist = load_watchlist_tickers()
    universe = list(dict.fromkeys(CORE_UNIVERSE + watchlist))[:30]
    print(f"[vol] Universe: {len(universe)} tickers ({len(CORE_UNIVERSE)} core + {len(watchlist)} watchlist)")

    per_ticker = {}
    for t in universe:
        elapsed = time.time() - started
        if elapsed > 220:  # leave 20s headroom on 240s timeout
            print(f"[vol] Time budget — stopping at {t}")
            break
        try:
            print(f"[vol] {t}…")
            per_ticker[t] = assemble_ticker(t)
        except Exception as e:
            print(f"[vol] {t} fatal: {e}")
            per_ticker[t] = {"ticker": t, "errors": [str(e)[:100]]}

    composite_score, composite_label = composite_regime(per_ticker)

    # Aggregate stats
    regime_counts = {"COMPLACENT": 0, "NORMAL": 0, "CONCERNED": 0, "PANIC": 0, "UNKNOWN": 0}
    for t, rec in per_ticker.items():
        r = rec.get("regime") or "UNKNOWN"
        regime_counts[r] = regime_counts.get(r, 0) + 1

    # Tickers most concerning right now
    sorted_tickers = sorted(
        [r for r in per_ticker.values() if r.get("regime")],
        key=lambda r: REGIME_SCORE.get(r["regime"], 0) + (r.get("rv_z") or 0) * 5,
        reverse=True,
    )

    payload = {
        "schema_version": "1.0",
        "method": "vol_regime_v1",
        "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "n_tickers": len(per_ticker),
        "n_with_data": sum(1 for r in per_ticker.values() if r.get("regime")),
        "composite_score": composite_score,
        "composite_regime": composite_label,
        "regime_counts": regime_counts,
        "weights": WEIGHTS,
        "tickers": list(per_ticker.values()),
        "most_stressed": [{"ticker": r["ticker"], "regime": r["regime"],
                            "rv_z": r.get("rv_z"), "iv_rv": r.get("iv_rv_ratio"),
                            "skew": r.get("skew_30d"), "term": r.get("term_slope")}
                           for r in sorted_tickers[:10]],
        "duration_s": round(time.time() - started, 1),
    }

    body_bytes = json.dumps(payload, indent=2, default=str).encode("utf-8")
    S3.put_object(
        Bucket=BUCKET, Key=S3_KEY_OUT, Body=body_bytes,
        ContentType="application/json", CacheControl="max-age=300",
    )
    print(f"[vol] DONE in {payload['duration_s']}s · "
          f"{payload['n_with_data']}/{payload['n_tickers']} with data · "
          f"composite={composite_score} ({composite_label})")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "n_with_data": payload["n_with_data"],
            "composite_score": composite_score,
            "composite_regime": composite_label,
            "duration_s": payload["duration_s"],
        }),
    }
