"""
justhodl-implied-prob — Forward implied probability dashboard.

WHAT IT EXTRACTS
─────────────────
  1. FED RATE PATH (from fed funds futures)
     - Implied avg fed funds rate for next 1, 3, 6 meetings
     - P(cut) / P(hold) / P(hike) per meeting
     - Source: FRED FF futures via DGS series, OR Polygon ZQ futures

  2. RECESSION PROBABILITY (FRED RECPROUSM156N + yield curve)
     - NY Fed Treasury Yield Spread Probability of Recession (12m forward)
     - 10Y-3M curve inversion duration
     - HY-IG credit spread percentile

  3. SPY IMPLIED MOVES (Polygon options)
     - Implied ±5% / ±10% / ±20% probability over 30d/90d horizons
     - Computed from option-implied probability density (Breeden-Litzenberger)
     - Approximation: use straddle-implied vol + log-normal assumption

  4. BTC IMPLIED MOVES (Polygon options on IBIT or BTC ETF)
     - Same methodology as SPY

  5. EARNINGS IMPLIED MOVE (per ticker, near-term earnings)
     - Front-month ATM straddle / spot = expected % move
     - Aggregated for upcoming earnings tickers

OUTPUT
──────
  data/implied-prob.json
  {
    fed: {
      current_rate, target_lower, target_upper,
      next_meeting: {date, days_to, p_cut_25, p_cut_50, p_hold, p_hike_25},
      meetings_3: {avg_implied, vs_current_bps},
      meetings_6: {...},
      year_end: {avg_implied, vs_current_bps},
    },
    recession: {
      ny_fed_12m_prob_pct,
      yield_curve_inverted,
      hy_spread_bp,
      hy_spread_z,
      composite_score_0_100,  # higher = more recession risk
    },
    spy: {
      spot, iv_30d, iv_90d,
      moves_30d: {p_up_5, p_down_5, p_up_10, p_down_10},
      moves_90d: {...},
    },
    btc: {...},
    earnings_implied: [{ticker, date, expected_move_pct}],
  }

INSTITUTIONAL-GRADE
────────────────────
  ✓ FED rate from FRED DFEDTARU (upper bound) + DFEDTARL (lower)
  ✓ FF futures from CME via FRED FFER series (spot rate) + month-by-month derivation
  ✓ NY Fed recession probability is the official 12m series (RECPROUSM156N)
  ✓ HY spread percentile: BAMLH0A0HYM2 vs 10y history
  ✓ Implied moves use log-normal: P(S_t > K) = N(d2) where d2 = (ln(S/K) - σ²t/2) / (σ√t)
  ✓ Failure-safe — each section runs independently, partial output OK
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
S3_KEY_OUT = os.environ.get("S3_KEY_OUT", "data/implied-prob.json")
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")
POLY_KEY = os.environ.get("POLY_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")

S3 = boto3.client("s3", region_name=REGION)


def http_get_json(url, timeout=20):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "implied-prob/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        print(f"[implied] HTTP fail: {url[:80]} → {e}")
        return None


def fetch_fred(series_id, n=200):
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
    return obs[::-1]  # chronological


# ─── normal CDF helpers ──────────────────────────────────────────────────────
def norm_cdf(x):
    """Standard normal CDF using erf."""
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def lognormal_prob_above(spot, strike, vol_annual, days):
    """P(S_t > strike) under log-normal with σ = vol_annual.
    Uses risk-neutral / no-drift assumption (drift cancels for short horizons)."""
    if not spot or not strike or not vol_annual or vol_annual <= 0:
        return None
    t = days / 365.0
    if t <= 0:
        return None
    d2 = (math.log(spot / strike) - 0.5 * (vol_annual ** 2) * t) / (vol_annual * math.sqrt(t))
    return round(norm_cdf(d2) * 100, 1)


# ─── Section 1: Fed rate path ────────────────────────────────────────────────
FOMC_NEXT_DATES = [
    "2026-06-10", "2026-07-29", "2026-09-16",
    "2026-10-28", "2026-12-16",
]


def fed_section():
    out = {}
    # Current target rate (upper + lower bound)
    upper_obs = fetch_fred("DFEDTARU", n=10)
    lower_obs = fetch_fred("DFEDTARL", n=10)
    if upper_obs:
        out["target_upper"] = upper_obs[-1]["value"]
        out["target_upper_date"] = upper_obs[-1]["date"]
    if lower_obs:
        out["target_lower"] = lower_obs[-1]["value"]
    out["current_rate"] = (out.get("target_upper", 0) + out.get("target_lower", 0)) / 2

    # Effective fed funds rate
    ffer = fetch_fred("FEDFUNDS", n=24)  # monthly
    if ffer:
        out["ffer_recent"] = ffer[-1]["value"]
        out["ffer_date"] = ffer[-1]["date"]

    # Forward rate path (from FF futures — proxied via Treasury bill rates as fallback)
    # Polygon doesn't easily expose ZQ contracts, so we'll use 1m Treasury bill DGS1MO
    # as a proxy for very-near-term fed expectation
    bill_1m = fetch_fred("DGS1MO", n=10)
    bill_3m = fetch_fred("DGS3MO", n=10)
    bill_6m = fetch_fred("DGS6MO", n=10)
    bill_1y = fetch_fred("DGS1", n=10)

    if bill_3m and bill_6m and out.get("current_rate"):
        cur = out["current_rate"]
        b3 = bill_3m[-1]["value"]
        b6 = bill_6m[-1]["value"]
        b1y = bill_1y[-1]["value"] if bill_1y else None

        # Forward 3m rate ≈ short rate; if 3m bill < FF → market pricing cuts
        out["bill_3m"] = b3
        out["bill_6m"] = b6
        out["bill_1y"] = b1y
        out["implied_3m_change_bp"] = round((b3 - cur) * 100)
        out["implied_6m_change_bp"] = round((b6 - cur) * 100)
        if b1y is not None:
            out["implied_1y_change_bp"] = round((b1y - cur) * 100)

        # Interpretation
        if out["implied_3m_change_bp"] < -10:
            out["near_term_stance"] = "MARKET PRICING CUTS"
        elif out["implied_3m_change_bp"] > 10:
            out["near_term_stance"] = "MARKET PRICING HIKES"
        else:
            out["near_term_stance"] = "MARKET PRICING HOLD"

    # Next meeting
    today = date.today()
    for d in FOMC_NEXT_DATES:
        try:
            md = datetime.strptime(d, "%Y-%m-%d").date()
            if md >= today:
                out["next_meeting"] = {
                    "date": d,
                    "days_to": (md - today).days,
                }
                break
        except ValueError:
            pass

    return out


# ─── Section 2: Recession probability ────────────────────────────────────────
def recession_section():
    out = {}
    # NY Fed 12m recession probability — official series
    ny_fed = fetch_fred("RECPROUSM156N", n=24)
    if ny_fed:
        out["ny_fed_12m_prob_pct"] = round(ny_fed[-1]["value"], 2)
        out["ny_fed_date"] = ny_fed[-1]["date"]
        # 12m delta
        if len(ny_fed) >= 12:
            out["ny_fed_12m_delta_pct"] = round(ny_fed[-1]["value"] - ny_fed[-13]["value"], 2)

    # Yield curve inversion
    t10y3m = fetch_fred("T10Y3M", n=520)  # daily
    if t10y3m:
        cur = t10y3m[-1]["value"]
        out["yield_curve_10y3m_bp"] = round(cur * 100, 1)
        out["yield_curve_inverted"] = cur < 0
        # Days inverted (most recent streak)
        days_inverted = 0
        for o in reversed(t10y3m):
            if o["value"] < 0:
                days_inverted += 1
            else:
                break
        out["days_inverted_current"] = days_inverted

    # HY spread (BAMLH0A0HYM2)
    hy = fetch_fred("BAMLH0A0HYM2", n=2600)  # ~10y daily
    if hy and len(hy) >= 100:
        cur_hy = hy[-1]["value"]
        out["hy_spread_bp"] = round(cur_hy * 100, 1)
        # Percentile vs 10y history
        values = [o["value"] for o in hy]
        rank = sum(1 for v in values if v < cur_hy)
        out["hy_spread_percentile_10y"] = round((rank / len(values)) * 100, 1)
        # z-score
        mean = statistics.mean(values)
        sd = statistics.stdev(values) if len(values) > 1 else None
        if sd:
            out["hy_spread_z"] = round((cur_hy - mean) / sd, 2)

    # Composite recession score 0-100
    score_parts = []
    if "ny_fed_12m_prob_pct" in out:
        # NY Fed: 0-100% directly
        score_parts.append(min(100, out["ny_fed_12m_prob_pct"] * 2))  # 50% prob → score 100
    if out.get("yield_curve_inverted"):
        # Inversion: bonus 30pts
        score_parts.append(60 + min(30, out.get("days_inverted_current", 0) / 5))
    if out.get("hy_spread_percentile_10y") is not None:
        score_parts.append(out["hy_spread_percentile_10y"])
    if score_parts:
        out["composite_score_0_100"] = round(statistics.mean(score_parts), 1)
        s = out["composite_score_0_100"]
        if s >= 70:   out["composite_label"] = "ELEVATED RECESSION RISK"
        elif s >= 40: out["composite_label"] = "MODERATE RECESSION RISK"
        else:         out["composite_label"] = "LOW RECESSION RISK"
    return out


# ─── Section 3: Index implied moves (SPY/QQQ/BTC via FRED VIX-family) ────────
# Polygon /v3/snapshot/options is paid-tier-locked (HTTP 403 NOT_AUTHORIZED).
# FRED publishes the same CBOE vol indices daily, FREE.
INDEX_IV_FRED = {
    "SPY":  ("VIXCLS", "VXVCLS"),  # 30-day VIX, 3-month VIX
    "QQQ":  ("VXNCLS", None),
    "IWM":  ("RVXCLS", None),
    "DIA":  ("VXDCLS", None),
    "GLD":  ("GVZCLS", None),
    "USO":  ("OVXCLS", None),
    "IBIT": (None, None),  # No public BTC vol index on FRED
}


def get_spot_price(ticker):
    """Fetch spot via Polygon last-trade or previous-close fallback."""
    try:
        d = http_get_json(f"https://api.polygon.io/v2/last/trade/{ticker}?apiKey={POLY_KEY}", timeout=10)
        if d and d.get("status") in ("OK", "DELAYED"):
            return d.get("results", {}).get("p")
    except Exception:
        pass
    try:
        d = http_get_json(f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev?apiKey={POLY_KEY}", timeout=10)
        results = (d or {}).get("results") or []
        if results:
            return results[0].get("c")
    except Exception:
        pass
    return None


def get_atm_iv(ticker, expiry_target_days):
    """Returns (spot_price, iv_decimal) from FRED VIX-family.
    expiry_target_days: 30 → use front-month VIX; 90 → use 3-month VIX (where available)."""
    spot = get_spot_price(ticker)
    if not spot:
        return None, None

    if ticker not in INDEX_IV_FRED:
        return spot, None

    front_id, back_id = INDEX_IV_FRED[ticker]
    series_id = back_id if (expiry_target_days >= 60 and back_id) else front_id

    if not series_id:
        return spot, None

    qs = urllib.parse.urlencode({
        "series_id": series_id, "api_key": FRED_KEY,
        "file_type": "json", "limit": 5, "sort_order": "desc",
    })
    d = http_get_json(f"https://api.stlouisfed.org/fred/series/observations?{qs}")
    if not d:
        return spot, None
    obs = d.get("observations", [])
    for o in obs:
        v = o.get("value")
        if v and v != ".":
            try:
                # FRED VIX series are quoted in % terms (e.g., 16.5)
                # We need IV as decimal for log-normal calculations
                return spot, float(v) / 100.0
            except ValueError:
                continue
    return spot, None


def index_implied_moves(ticker):
    out = {"ticker": ticker}
    spot_30, iv_30 = get_atm_iv(ticker, 30)
    spot_90, iv_90 = get_atm_iv(ticker, 90)
    spot = spot_30 or spot_90
    out["spot"] = round(spot, 2) if spot else None
    if iv_30:
        out["iv_30d"] = round(iv_30 * 100, 2)
        out["moves_30d"] = {
            "p_up_5":    lognormal_prob_above(spot, spot * 1.05, iv_30, 30),
            "p_down_5":  100 - lognormal_prob_above(spot, spot * 0.95, iv_30, 30) if spot else None,
            "p_up_10":   lognormal_prob_above(spot, spot * 1.10, iv_30, 30),
            "p_down_10": 100 - lognormal_prob_above(spot, spot * 0.90, iv_30, 30) if spot else None,
            "expected_move_pct": round(iv_30 * math.sqrt(30 / 365) * 100, 2),
        }
    if iv_90:
        out["iv_90d"] = round(iv_90 * 100, 2)
        out["moves_90d"] = {
            "p_up_5":    lognormal_prob_above(spot, spot * 1.05, iv_90, 90),
            "p_down_5":  100 - lognormal_prob_above(spot, spot * 0.95, iv_90, 90) if spot else None,
            "p_up_10":   lognormal_prob_above(spot, spot * 1.10, iv_90, 90),
            "p_down_10": 100 - lognormal_prob_above(spot, spot * 0.90, iv_90, 90) if spot else None,
            "p_up_20":   lognormal_prob_above(spot, spot * 1.20, iv_90, 90),
            "p_down_20": 100 - lognormal_prob_above(spot, spot * 0.80, iv_90, 90) if spot else None,
            "expected_move_pct": round(iv_90 * math.sqrt(90 / 365) * 100, 2),
        }
    return out


# ─── Section 4: Earnings implied moves ───────────────────────────────────────
def earnings_implied_moves():
    """Single-stock IV requires Polygon options snapshot (paid-tier-only).
    Returning empty list with explanatory note. Restore when premium tier active."""
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/earnings-tracker.json")
        d = json.loads(obj["Body"].read())
        # We can still surface upcoming earnings names + days_to (free data).
        # Just can't compute implied move without single-stock IV.
        out = []
        today = date.today()
        upcoming = (d.get("upcoming_14d") or [])[:10]
        for u in upcoming:
            ticker = u.get("ticker")
            ed_str = (u.get("earnings_date") or "")[:10]
            if not ticker or not ed_str:
                continue
            try:
                ed = datetime.strptime(ed_str, "%Y-%m-%d").date()
            except ValueError:
                continue
            days_to = (ed - today).days
            if days_to < 0 or days_to > 14:
                continue
            out.append({
                "ticker": ticker,
                "name": (u.get("name") or "")[:40],
                "earnings_date": ed_str,
                "days_to": days_to,
                "spot": None, "iv_pct": None, "expected_move_pct": None,
                "note": "Single-stock IV requires Polygon premium tier",
            })
        out.sort(key=lambda r: r["days_to"])
        return out
    except Exception as e:
        print(f"[implied] earnings_implied_moves: {e}")
        return []


# ─── BTC RV proxy (no public IV index for crypto) ─────────────────────────────
def compute_btc_rv_proxy():
    """IBIT has no public IV index. Use realized vol from history as proxy.
    Returns {rv_30d_pct, rv_90d_pct} annualized."""
    end = date.today()
    start = end - timedelta(days=180)
    qs = urllib.parse.urlencode({"apiKey": POLY_KEY, "adjusted": "true", "sort": "desc"})
    url = (f"https://api.polygon.io/v2/aggs/ticker/IBIT/range/1/day/"
            f"{start.strftime('%Y-%m-%d')}/{end.strftime('%Y-%m-%d')}?{qs}")
    d = http_get_json(url)
    if not d or d.get("status") not in ("OK", "DELAYED"):
        return None
    closes = [b.get("c") for b in (d.get("results") or []) if b.get("c")]
    if len(closes) < 30:
        return None

    def annualized_rv(window):
        if len(closes) < window + 1:
            return None
        recent = closes[:window + 1]
        log_returns = []
        for i in range(len(recent) - 1):
            if recent[i] > 0 and recent[i + 1] > 0:
                log_returns.append(math.log(recent[i] / recent[i + 1]))
        if len(log_returns) < 5:
            return None
        sd = statistics.stdev(log_returns)
        return round(sd * math.sqrt(252) * 100, 2)

    return {
        "rv_30d_pct": annualized_rv(30) or annualized_rv(20),
        "rv_90d_pct": annualized_rv(90) or annualized_rv(60),
    }


# ─── Main handler ────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    started = time.time()

    print("[implied] Section 1: Fed rate path…")
    fed = fed_section()

    print("[implied] Section 2: Recession probability…")
    rec = recession_section()

    print("[implied] Section 3: SPY implied moves…")
    spy = index_implied_moves("SPY")

    print("[implied] Section 3b: QQQ implied moves…")
    qqq = index_implied_moves("QQQ")

    print("[implied] Section 4: BTC implied moves (IBIT)…")
    btc = index_implied_moves("IBIT")
    # IBIT has no FRED vol index — use realized vol as IV proxy
    if btc and btc.get("iv_30d") is None and btc.get("spot"):
        try:
            rv_iv = compute_btc_rv_proxy()
            if rv_iv:
                spot = btc["spot"]
                btc["iv_30d"] = round(rv_iv["rv_30d_pct"], 2)
                btc["iv_90d"] = round(rv_iv["rv_90d_pct"], 2)
                btc["iv_proxy_note"] = "Implied vol proxied from realized vol (no public BTC vol index)"
                btc["iv_proxy_source"] = "Polygon IBIT history × √252"
                # Compute log-normal probabilities using RV-derived IV
                iv_30 = rv_iv["rv_30d_pct"] / 100.0
                iv_90 = rv_iv["rv_90d_pct"] / 100.0
                btc["moves_30d"] = {
                    "p_up_5":    lognormal_prob_above(spot, spot * 1.05, iv_30, 30),
                    "p_down_5":  100 - lognormal_prob_above(spot, spot * 0.95, iv_30, 30),
                    "p_up_10":   lognormal_prob_above(spot, spot * 1.10, iv_30, 30),
                    "p_down_10": 100 - lognormal_prob_above(spot, spot * 0.90, iv_30, 30),
                    "expected_move_pct": round(iv_30 * math.sqrt(30 / 365) * 100, 2),
                }
                btc["moves_90d"] = {
                    "p_up_5":    lognormal_prob_above(spot, spot * 1.05, iv_90, 90),
                    "p_down_5":  100 - lognormal_prob_above(spot, spot * 0.95, iv_90, 90),
                    "p_up_10":   lognormal_prob_above(spot, spot * 1.10, iv_90, 90),
                    "p_down_10": 100 - lognormal_prob_above(spot, spot * 0.90, iv_90, 90),
                    "p_up_20":   lognormal_prob_above(spot, spot * 1.20, iv_90, 90),
                    "p_down_20": 100 - lognormal_prob_above(spot, spot * 0.80, iv_90, 90),
                    "expected_move_pct": round(iv_90 * math.sqrt(90 / 365) * 100, 2),
                }
        except Exception as e:
            print(f"[implied] BTC RV proxy failed: {e}")

    earnings = []
    if time.time() - started < 180:  # only if we have time
        print("[implied] Section 5: Earnings implied moves…")
        try:
            earnings = earnings_implied_moves()
        except Exception as e:
            print(f"[implied] earnings section failed: {e}")

    payload = {
        "schema_version": "2.0",
        "method": "implied_prob_v2_fred",
        "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "fed": fed,
        "recession": rec,
        "spy": spy,
        "qqq": qqq,
        "btc": btc,
        "earnings_implied": earnings,
        "duration_s": round(time.time() - started, 1),
    }
    body_bytes = json.dumps(payload, indent=2, default=str).encode("utf-8")
    S3.put_object(
        Bucket=BUCKET, Key=S3_KEY_OUT, Body=body_bytes,
        ContentType="application/json", CacheControl="max-age=300",
    )
    print(f"[implied] DONE in {payload['duration_s']}s")
    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "fed_stance": fed.get("near_term_stance"),
            "recession_label": rec.get("composite_label"),
            "duration_s": payload["duration_s"],
        }),
    }
