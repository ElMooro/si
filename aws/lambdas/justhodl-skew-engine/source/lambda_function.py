"""
justhodl-skew-engine — Implied volatility skew + term structure.

For each underlying (SPY, QQQ, IWM), pulls the option chain snapshot from
Polygon and computes per-expiry:

  - 25-delta put IV (the "fear premium") via interpolation
  - 25-delta call IV
  - 25Δ put/call skew = put_iv − call_iv  (in vol points)
  - ATM IV (delta closest to 0.5)
  - Risk reversal = call_iv − put_iv  (negative = put-skewed/fearful)
  - Butterfly = (put_iv + call_iv)/2 − atm_iv  (kurtosis premium)

Across expiries we get an IV term structure — front (≤30d), belly (30-90d),
back (≥90d).

Composite signals:
  - skew_regime    "PANIC" | "FEAR" | "NEUTRAL" | "COMPLACENT"
  - term_structure "CONTANGO" | "FLAT" | "BACKWARDATION"
  - directional_bias from sign+magnitude of front-month risk reversal

Polygon endpoint: /v3/snapshot/options/{underlying}?greeks=true
Output: data/skew.json
Schedule: every 1 hour during market hours (cron(0 13-21 ? * MON-FRI *))
"""

import json
import math
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
KEY = "data/skew.json"

POLYGON_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
UNDERLYINGS = ["SPY", "QQQ", "IWM"]
TARGET_DELTA_PUT = -0.25  # 25-delta put
TARGET_DELTA_CALL = 0.25  # 25-delta call

# Polygon options snapshot endpoint
SNAPSHOT_URL_TPL = (
    "https://api.polygon.io/v3/snapshot/options/{u}"
    "?greeks=true&limit=250&apiKey={k}"
)


def fetch_chain(underlying):
    """Fetch all option contracts (paginated) with greeks for the underlying."""
    out = []
    url = SNAPSHOT_URL_TPL.format(u=underlying, k=POLYGON_KEY)
    pages = 0
    while url and pages < 12:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "justhodl-skew/1.0"})
            with urllib.request.urlopen(req, timeout=30) as r:
                payload = json.loads(r.read().decode())
        except Exception as e:
            print(f"[poly] {underlying} page {pages} failed: {e}")
            break
        results = payload.get("results", [])
        out.extend(results)
        next_url = payload.get("next_url")
        if next_url:
            sep = "&" if "?" in next_url else "?"
            url = f"{next_url}{sep}apiKey={POLYGON_KEY}"
        else:
            url = None
        pages += 1
        time.sleep(0.05)
    print(f"[poly] {underlying}: fetched {len(out)} contracts in {pages} pages")
    return out


def parse_expiry(exp_str):
    """YYYY-MM-DD → datetime, or None."""
    try:
        return datetime.strptime(exp_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def days_to_expiry(exp_str, now=None):
    now = now or datetime.now(timezone.utc)
    exp = parse_expiry(exp_str)
    if not exp:
        return None
    return (exp - now).total_seconds() / 86400


def interpolate_iv_at_delta(contracts, target_delta, contract_type):
    """Given list of (delta, iv) for one expiry/type, find iv at target_delta.

    Polygon delta convention: calls 0..1, puts -1..0. We linearly interpolate.
    """
    pts = []
    for c in contracts:
        d = c.get("greeks", {}).get("delta") if c.get("greeks") else None
        iv = c.get("implied_volatility")
        if d is None or iv is None:
            continue
        if iv <= 0 or iv > 5:
            continue
        pts.append((d, iv))
    if len(pts) < 2:
        return None
    # Sort by delta
    pts.sort(key=lambda x: x[0])
    # Find bracketing pair
    for i in range(len(pts) - 1):
        d0, iv0 = pts[i]
        d1, iv1 = pts[i + 1]
        if (d0 <= target_delta <= d1) or (d1 <= target_delta <= d0):
            # Linear interp
            if d1 == d0:
                return iv0
            t = (target_delta - d0) / (d1 - d0)
            return iv0 + t * (iv1 - iv0)
    # Extrapolate to the closer end
    if abs(pts[0][0] - target_delta) < abs(pts[-1][0] - target_delta):
        return pts[0][1]
    return pts[-1][1]


def compute_per_expiry(chain):
    """Group by expiry → compute skew metrics."""
    by_expiry = {}
    for c in chain:
        det = c.get("details", {})
        exp = det.get("expiration_date")
        ctype = det.get("contract_type")
        if not exp or not ctype:
            continue
        if exp not in by_expiry:
            by_expiry[exp] = {"call": [], "put": []}
        if ctype == "call":
            by_expiry[exp]["call"].append(c)
        elif ctype == "put":
            by_expiry[exp]["put"].append(c)

    out = []
    now = datetime.now(timezone.utc)
    for exp, types in by_expiry.items():
        dte = days_to_expiry(exp, now)
        if dte is None or dte < 0 or dte > 365 * 2:
            continue
        calls = types["call"]
        puts = types["put"]
        if len(calls) < 3 or len(puts) < 3:
            continue
        iv_call_25d = interpolate_iv_at_delta(calls, TARGET_DELTA_CALL, "call")
        iv_put_25d = interpolate_iv_at_delta(puts, TARGET_DELTA_PUT, "put")
        # ATM: delta closest to 0.5 for calls
        def atm_iv(opts, side):
            target = 0.5 if side == "call" else -0.5
            best = None
            best_diff = 99
            for c in opts:
                d = c.get("greeks", {}).get("delta") if c.get("greeks") else None
                iv = c.get("implied_volatility")
                if d is None or iv is None or iv <= 0 or iv > 5:
                    continue
                diff = abs(d - target)
                if diff < best_diff:
                    best_diff = diff
                    best = iv
            return best
        atm_call = atm_iv(calls, "call")
        atm_put = atm_iv(puts, "put")
        atm = None
        if atm_call is not None and atm_put is not None:
            atm = (atm_call + atm_put) / 2
        elif atm_call is not None:
            atm = atm_call
        elif atm_put is not None:
            atm = atm_put

        skew_25d = None
        risk_reversal = None
        butterfly = None
        if iv_put_25d is not None and iv_call_25d is not None:
            # In vol points (multiply by 100 to get vol-pct)
            skew_25d = round((iv_put_25d - iv_call_25d) * 100, 3)
            risk_reversal = round((iv_call_25d - iv_put_25d) * 100, 3)
            if atm is not None:
                butterfly = round(((iv_put_25d + iv_call_25d) / 2 - atm) * 100, 3)

        out.append({
            "expiry": exp,
            "dte": round(dte, 1),
            "n_calls": len(calls),
            "n_puts": len(puts),
            "iv_call_25d": round(iv_call_25d * 100, 3) if iv_call_25d else None,
            "iv_put_25d": round(iv_put_25d * 100, 3) if iv_put_25d else None,
            "iv_atm": round(atm * 100, 3) if atm else None,
            "skew_25d_vol_pts": skew_25d,
            "risk_reversal_vol_pts": risk_reversal,
            "butterfly_vol_pts": butterfly,
        })

    out.sort(key=lambda x: x["dte"])
    return out


def classify_skew(rr_front):
    """Risk reversal in vol pts → regime."""
    if rr_front is None:
        return "UNKNOWN", "Insufficient data"
    if rr_front <= -8:
        return "PANIC", f"Risk reversal {rr_front:+.1f} vol pts — extreme put-skew, deep fear pricing"
    if rr_front <= -4:
        return "FEAR", f"Risk reversal {rr_front:+.1f} vol pts — meaningful put-skew, defensive"
    if rr_front <= -1:
        return "NEUTRAL", f"Risk reversal {rr_front:+.1f} vol pts — typical put-skew, normal market"
    if rr_front <= 1:
        return "BALANCED", f"Risk reversal {rr_front:+.1f} vol pts — symmetric IV smile, unusual"
    return "COMPLACENT", f"Risk reversal {rr_front:+.1f} vol pts — call-skewed, melt-up positioning"


def classify_term_structure(front_atm, back_atm):
    if front_atm is None or back_atm is None:
        return "UNKNOWN", None
    diff = back_atm - front_atm
    if diff >= 1.5:
        return "CONTANGO", f"Back-month IV {diff:+.2f} pts above front — calm now, hedging future"
    if diff <= -1.5:
        return "BACKWARDATION", f"Front IV {-diff:+.2f} pts above back — stress now, reverting"
    return "FLAT", f"Front≈back IV (Δ={diff:+.2f}) — uniform pricing across tenors"


def lambda_handler(event=None, context=None):
    started = time.time()

    out = {
        "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "underlyings": {},
    }

    for u in UNDERLYINGS:
        chain = fetch_chain(u)
        if not chain:
            out["underlyings"][u] = {"error": "no_chain"}
            continue

        per_exp = compute_per_expiry(chain)
        # Aggregate front (≤30d), belly (30-90d), back (≥90d)
        front = [e for e in per_exp if e["dte"] <= 30]
        belly = [e for e in per_exp if 30 < e["dte"] <= 90]
        back = [e for e in per_exp if e["dte"] > 90]

        def avg(items, k):
            vals = [e[k] for e in items if e.get(k) is not None]
            return round(sum(vals) / len(vals), 3) if vals else None

        front_summary = {
            "n_expiries": len(front),
            "atm_iv": avg(front, "iv_atm"),
            "skew_25d": avg(front, "skew_25d_vol_pts"),
            "risk_reversal": avg(front, "risk_reversal_vol_pts"),
            "butterfly": avg(front, "butterfly_vol_pts"),
        }
        belly_summary = {
            "n_expiries": len(belly),
            "atm_iv": avg(belly, "iv_atm"),
            "skew_25d": avg(belly, "skew_25d_vol_pts"),
            "risk_reversal": avg(belly, "risk_reversal_vol_pts"),
            "butterfly": avg(belly, "butterfly_vol_pts"),
        }
        back_summary = {
            "n_expiries": len(back),
            "atm_iv": avg(back, "iv_atm"),
            "skew_25d": avg(back, "skew_25d_vol_pts"),
            "risk_reversal": avg(back, "risk_reversal_vol_pts"),
            "butterfly": avg(back, "butterfly_vol_pts"),
        }

        # Regime classifications
        skew_regime, skew_desc = classify_skew(front_summary["risk_reversal"])
        ts_regime, ts_desc = classify_term_structure(front_summary["atm_iv"], back_summary["atm_iv"])

        out["underlyings"][u] = {
            "n_contracts": len(chain),
            "n_expiries": len(per_exp),
            "front": front_summary,
            "belly": belly_summary,
            "back": back_summary,
            "skew_regime": skew_regime,
            "skew_desc": skew_desc,
            "term_structure": ts_regime,
            "term_desc": ts_desc,
            "per_expiry": per_exp[:30],  # limit to first 30 for size
        }
        print(f"[skew] {u}: {len(chain)} contracts, {len(per_exp)} expiries, skew={skew_regime}, ts={ts_regime}")

    out["duration_s"] = round(time.time() - started, 2)

    # Summary across underlyings
    summary = {
        "spy_skew_regime": out["underlyings"].get("SPY", {}).get("skew_regime"),
        "spy_term_structure": out["underlyings"].get("SPY", {}).get("term_structure"),
        "spy_front_atm_iv": out["underlyings"].get("SPY", {}).get("front", {}).get("atm_iv"),
        "spy_front_risk_reversal": out["underlyings"].get("SPY", {}).get("front", {}).get("risk_reversal"),
    }
    out["summary"] = summary

    body = json.dumps(out, default=str).encode("utf-8")
    S3.put_object(Bucket=BUCKET, Key=KEY, Body=body, ContentType="application/json", CacheControl="public, max-age=600")
    print(f"[skew] wrote {len(body):,}b in {out['duration_s']}s")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "duration_s": out["duration_s"],
            "underlyings": list(out["underlyings"].keys()),
            "summary": summary,
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None), indent=2))
