"""
justhodl-vol-regime — Volatility regime dashboard (v2: FRED VIX-family).

CHANGE FROM v1
──────────────
v1 used Polygon /v3/snapshot/options which is paid-tier-locked (HTTP 403).
v2 uses FRED VIX-family indices — same data CBOE publishes daily, FREE.

WHAT IT COMPUTES PER TICKER
────────────────────────────
For each ticker (SPY, QQQ, IWM, DIA, GLD, TLT, IBIT, VXX + watchlist top 20):

  REALIZED VOL (Polygon daily history — works on free tier)
    rv_5d, rv_20d, rv_90d   — annualized stdev of log returns × √252
    rv_z                    — current 20d RV vs 1y mean (z-score)

  IMPLIED VOL (FRED VIX-family — daily-published CBOE indices)
    Lookup table (ticker → FRED series):
      SPY → VIXCLS    (30-day VIX, the canonical equity vol index)
      QQQ → VXNCLS    (NASDAQ-100 implied vol)
      IWM → RVXCLS    (Russell 2000 implied vol)
      DIA → VXDCLS    (DJIA implied vol)
      GLD → GVZCLS    (gold ETF implied vol)
      USO → OVXCLS    (oil ETF implied vol)
      TLT → no direct vol index → null (use RV only)
      IBIT → no direct vol index → null (use RV only)
      VXX → uses VIXCLS itself (it's a VIX product)

  TERM STRUCTURE (where available)
    SPY: VXVCLS (3-month VIX) - VIXCLS (30-day VIX)
    Negative term slope = backwardation = stress

  IV/RV RATIO
    Computed where IV is available

  REGIME CLASSIFICATION (per ticker)
    PANIC      — IV/RV>1.50 AND term<0 AND rv_z>1.5
    CONCERNED  — IV/RV>1.20 OR rv_z>1.5
    COMPLACENT — IV/RV<0.95 AND rv_z<-0.5 AND term>0
    NORMAL     — defaults

CROSS-MARKET REGIME COMPOSITE
──────────────────────────────
  weighted by liquidity:
  SPY (0.30), QQQ (0.20), IWM (0.10), GLD (0.10), TLT (0.15), VXX (0.15)

INSTITUTIONAL-GRADE
────────────────────
  ✓ FRED is the gold-standard public source for CBOE vol indices
  ✓ Annualization: stdev × sqrt(252) [trading days]
  ✓ Log returns (not simple returns) — proper for vol
  ✓ Single FRED call per ticker (lightweight, parallel via ThreadPoolExecutor)
  ✓ Failure-safe — RV always computed even if IV missing
  ✓ Cross-asset coverage: equities + gold + oil + treasuries (RV only)
"""
import json
import math
import os
import statistics
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone

import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY_OUT = os.environ.get("S3_KEY_OUT", "data/vol-regime.json")
POLY_KEY = os.environ.get("POLY_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")

CORE_UNIVERSE = ["SPY", "QQQ", "IWM", "DIA", "GLD", "TLT", "IBIT", "VXX"]

# Map ticker → FRED VIX-family series for direct implied vol
IV_FRED_SERIES = {
    "SPY":  "VIXCLS",   # 30-day VIX
    "QQQ":  "VXNCLS",   # NASDAQ-100 vol
    "IWM":  "RVXCLS",   # Russell 2000 vol
    "DIA":  "VXDCLS",   # DJIA vol
    "GLD":  "GVZCLS",   # gold ETF vol
    "USO":  "OVXCLS",   # oil ETF vol
    "VXX":  "VIXCLS",   # VXX is a VIX product
    # No direct vol index for: TLT, IBIT, single stocks
}

# Term structure pairs: (front-month series, longer-month series)
TERM_STRUCTURE_PAIRS = {
    "SPY": ("VIXCLS", "VXVCLS"),  # 30-day vs 3-month
}

S3 = boto3.client("s3", region_name=REGION)


def http_get_json(url, timeout=20, retries=2):
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "vol-regime/2.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception as e:
            if attempt < retries:
                time.sleep(0.5 * (attempt + 1))
            else:
                print(f"[vol] HTTP fail: {url[:80]} → {e}")
    return None


def fetch_fred_latest(series_id, n=300):
    """Fetch FRED series. Returns list of {date, value} sorted chronologically."""
    qs = urllib.parse.urlencode({
        "series_id": series_id, "api_key": FRED_KEY,
        "file_type": "json", "limit": n, "sort_order": "desc",
    })
    d = http_get_json(f"https://api.stlouisfed.org/fred/series/observations?{qs}")
    if not d:
        return []
    obs = []
    for o in d.get("observations", []):
        v = o.get("value")
        if v and v != ".":
            try:
                obs.append({"date": o["date"], "value": float(v)})
            except ValueError:
                continue
    return obs[::-1]


def fetch_history(ticker, n_days=260):
    end = date.today()
    start = end - timedelta(days=int(n_days * 1.6))
    qs = urllib.parse.urlencode({"apiKey": POLY_KEY, "adjusted": "true", "sort": "desc"})
    url = (f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
            f"{start.strftime('%Y-%m-%d')}/{end.strftime('%Y-%m-%d')}?{qs}")
    d = http_get_json(url)
    if not d or d.get("status") not in ("OK", "DELAYED"):
        return []
    return [(b.get("t"), b.get("c")) for b in (d.get("results") or []) if b.get("c")]


def compute_realized_vol(closes, window):
    if len(closes) < window + 1:
        return None
    recent = closes[:window + 1]
    log_returns = []
    for i in range(len(recent) - 1):
        c_now = recent[i][1]; c_prev = recent[i + 1][1]
        if c_prev and c_now and c_prev > 0:
            log_returns.append(math.log(c_now / c_prev))
    if len(log_returns) < 5:
        return None
    sd = statistics.stdev(log_returns)
    return round(sd * math.sqrt(252) * 100, 2)


def compute_realized_vol_z(closes, window=20):
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


def assemble_ticker(ticker):
    out = {"ticker": ticker}

    # 1. RV from Polygon history
    history = fetch_history(ticker, n_days=260)
    if not history:
        out["err"] = "no history"
        return out

    out["spot_price"] = round(history[0][1], 2) if history else None
    out["rv_5d"]  = compute_realized_vol(history, 5)
    out["rv_20d"], out["rv_z"] = compute_realized_vol_z(history, 20)
    out["rv_90d"] = compute_realized_vol(history, 90)

    # 2. IV from FRED VIX-family
    fred_series = IV_FRED_SERIES.get(ticker)
    if fred_series:
        obs = fetch_fred_latest(fred_series, n=10)
        if obs:
            out["iv_atm_30d"] = round(obs[-1]["value"], 2)
            out["iv_source"] = fred_series
            out["iv_date"] = obs[-1]["date"]

    # 3. Term structure (where 3-month series exists)
    if ticker in TERM_STRUCTURE_PAIRS:
        front_id, back_id = TERM_STRUCTURE_PAIRS[ticker]
        front_obs = fetch_fred_latest(front_id, n=10)
        back_obs = fetch_fred_latest(back_id, n=10)
        if front_obs and back_obs:
            front = front_obs[-1]["value"]
            back = back_obs[-1]["value"]
            out["iv_atm_90d"] = round(back, 2)
            out["term_slope"] = round(back - front, 2)
            out["term_structure"] = "CONTANGO" if out["term_slope"] > 0 else "BACKWARDATION"

    # 4. IV/RV ratio
    if out.get("iv_atm_30d") is not None and out.get("rv_20d") is not None and out["rv_20d"] > 0:
        out["iv_rv_ratio"] = round(out["iv_atm_30d"] / out["rv_20d"], 2)

    # 5. Regime classification
    iv_rv = out.get("iv_rv_ratio")
    rv_z = out.get("rv_z")
    term = out.get("term_slope")

    if iv_rv is not None and iv_rv > 1.50 and (term or 0) < 0 and (rv_z or 0) > 1.5:
        out["regime"] = "PANIC"
    elif (iv_rv or 0) > 1.20 or (rv_z or 0) > 1.5:
        out["regime"] = "CONCERNED"
    elif iv_rv is not None and iv_rv < 0.95 and (rv_z or 0) < -0.5 and (term or 0) > 0:
        out["regime"] = "COMPLACENT"
    elif rv_z is not None:
        if rv_z > 1.5:    out["regime"] = "CONCERNED"
        elif rv_z < -1.0: out["regime"] = "COMPLACENT"
        else:             out["regime"] = "NORMAL"
    else:
        out["regime"] = "NORMAL"

    return out


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


def lambda_handler(event, context):
    started = time.time()

    watchlist = load_watchlist_tickers()
    universe = list(dict.fromkeys(CORE_UNIVERSE + watchlist))[:30]
    print(f"[vol] Universe: {len(universe)} tickers ({len(CORE_UNIVERSE)} core + {len(watchlist)} watchlist)")

    per_ticker = {}
    with ThreadPoolExecutor(max_workers=4) as exe:
        futures = {exe.submit(assemble_ticker, t): t for t in universe}
        for fut in as_completed(futures):
            t = futures[fut]
            try:
                per_ticker[t] = fut.result()
            except Exception as e:
                per_ticker[t] = {"ticker": t, "err": str(e)[:100]}

    composite_score, composite_label = composite_regime(per_ticker)

    regime_counts = {"COMPLACENT": 0, "NORMAL": 0, "CONCERNED": 0, "PANIC": 0, "UNKNOWN": 0}
    for rec in per_ticker.values():
        r = rec.get("regime") or "UNKNOWN"
        regime_counts[r] = regime_counts.get(r, 0) + 1

    sorted_tickers = sorted(
        [r for r in per_ticker.values() if r.get("regime")],
        key=lambda r: REGIME_SCORE.get(r["regime"], 0) + (r.get("rv_z") or 0) * 5,
        reverse=True,
    )

    payload = {
        "schema_version": "2.0",
        "method": "vol_regime_v2_fred",
        "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "n_tickers": len(per_ticker),
        "n_with_iv": sum(1 for r in per_ticker.values() if r.get("iv_atm_30d") is not None),
        "composite_score": composite_score,
        "composite_regime": composite_label,
        "regime_counts": regime_counts,
        "weights": WEIGHTS,
        "iv_sources_used": IV_FRED_SERIES,
        "tickers": list(per_ticker.values()),
        "most_stressed": [{"ticker": r["ticker"], "regime": r["regime"],
                            "rv_z": r.get("rv_z"), "iv_rv": r.get("iv_rv_ratio")}
                           for r in sorted_tickers[:10]],
        "duration_s": round(time.time() - started, 1),
    }
    body_bytes = json.dumps(payload, indent=2, default=str).encode("utf-8")
    S3.put_object(
        Bucket=BUCKET, Key=S3_KEY_OUT, Body=body_bytes,
        ContentType="application/json", CacheControl="max-age=300",
    )
    print(f"[vol] DONE in {payload['duration_s']}s · "
          f"{payload['n_with_iv']}/{payload['n_tickers']} with IV · "
          f"composite={composite_score} ({composite_label})")
    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "n_with_iv": payload["n_with_iv"],
            "composite_score": composite_score,
            "composite_regime": composite_label,
            "duration_s": payload["duration_s"],
        }),
    }
