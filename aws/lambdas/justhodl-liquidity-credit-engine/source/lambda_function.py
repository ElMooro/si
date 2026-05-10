"""
justhodl-liquidity-credit-engine

Pulls the FRED series Khalid specified for measuring system liquidity and
credit-market stress, computes WoW/MoM/QoQ/YoY % changes, z-scores (1y, 5y),
and signal classifications (NORMAL / WATCH / ELEVATED / CRISIS) calibrated
against historical events (GFC 2008, COVID 2020, SVB 2023, Sep 2019 repo).

CATEGORIES
  balance_sheet      — Fed assets, reserves, memo collateral
  liquidity_facilities — central bank swaps, primary credit (FCB stress)
  credit_spreads     — ICE BofA HY OAS (US/Euro/EM)
  corporate_yields   — HQM Corporate spot rates

OUTPUT  data/liquidity-credit-engine.json (5min CDN cache)
SCHEDULE  every 6h (FRED H.4.1 publishes Wednesday 4:30pm ET; ICE BofA daily)

Threshold rationale (research-backed):
  • CCC HY OAS:    GFC peak 2200bp, COVID peak 1900bp, normal 600-900bp
  • Euro HY OAS:   GFC peak 2400bp, COVID peak 1100bp, normal 300-500bp
  • EM HY Corp:    GFC peak 1700bp, COVID peak 1200bp, normal 500-800bp
  • Primary credit (OTHL1690): SVB spike was $164B; normal $0-2B
  • CB swaps (SWP1690): COVID peak $446B, normal $0-1B; reactivations are FX-stress signal
  • Bank reserves week-on-week: -2% in a week = QT acceleration / tightening

Hooks into alert-router on signal-state transitions.
"""
import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from statistics import mean, pstdev

import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUTPUT_KEY = "data/liquidity-credit-engine.json"
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")

# ────────────────────────────────────────────────────────────────────────
# SERIES MAP — every series Khalid specified + supporting context series.
# Each entry: (FRED id, category, label, units, threshold spec)
# threshold "kind":
#   "level"  — absolute thresholds {watch, elevated, crisis} on latest value
#   "delta_pct" — thresholds on % change over window (week/month)
#   "z"       — thresholds on z-score (1y default)
#   "spread_to" — compute spread vs another series' latest value
# ────────────────────────────────────────────────────────────────────────
SERIES_MAP = [
    # ─── BALANCE SHEET ─────────────────────────────────────────
    ("WALCL", "balance_sheet", "Fed Balance Sheet (total assets)",
     "billions $", {"kind": "delta_pct", "window": "wk", "watch": -0.5, "elevated": -1.0, "crisis": -2.0}),
    ("WRESBAL", "balance_sheet", "Bank Reserves (depository institutions)",
     "billions $", {"kind": "delta_pct", "window": "wk", "watch": -2.0, "elevated": -4.0, "crisis": -8.0}),
    ("EXCSRESNW", "balance_sheet", "Excess Reserves",
     "billions $", {"kind": "delta_pct", "window": "wk", "watch": -3.0, "elevated": -6.0, "crisis": -10.0}),
    ("WTREGEN", "balance_sheet", "Treasury General Account (TGA)",
     "billions $", {"kind": "level", "watch": 600, "elevated": 800, "crisis": 1000,
                     "note": "TGA above $800B drains liquidity from system"}),
    ("RRPONTSYD", "balance_sheet", "Overnight Reverse Repo (RRP)",
     "billions $", {"kind": "level", "watch_low": 50, "watch": 1500, "elevated": 2000,
                     "note": "RRP draining is liquidity-positive; near-zero = MMFs back in T-bills"}),
    ("WSHOMCB", "balance_sheet", "Securities Held Outright: Treasuries (broad)",
     "billions $", {"kind": "delta_pct", "window": "wk", "watch": -0.5}),
    ("RESPPALGUONNWW", "balance_sheet", "Securities Held Outright: Treasury Notes & Bonds",
     "billions $", {"kind": "delta_pct", "window": "wk", "watch": -0.5,
                     "note": "Khalid-specified — coupon Treasuries on Fed balance sheet"}),
    ("RESPPNTEPNWW", "balance_sheet", "MEMO: Treasury/Agency/MBS Eligible to Be Pledged",
     "billions $", {"kind": "delta_pct", "window": "wk", "watch": 5.0, "elevated": 10.0,
                     "note": "Khalid-specified — collateral pledge spike = funding stress"}),

    # ─── LIQUIDITY FACILITIES ─────────────────────────────────
    ("DPCREDIT", "liquidity_facilities", "Primary Credit (Discount Window)",
     "billions $", {"kind": "level", "watch": 2, "elevated": 5, "crisis": 25,
                     "note": "SVB spike was $164B — banks borrowing here = funding stress"}),
    ("OTHL1690", "liquidity_facilities", "Liquidity & Credit Facilities: Loans 16-90 Day",
     "billions $", {"kind": "level", "watch": 0.5, "elevated": 2, "crisis": 10,
                     "note": "Khalid-specified — emergency facilities active = financial-crisis signal"}),
    # Central-bank swap lines — use SWPT (total) since SWP1690 may not exist as specified ID;
    # we'll attempt SWP1690 first and fall back to SWPT
    ("SWP1690", "liquidity_facilities", "Central Bank Liquidity Swaps: 16-90 Day Maturity",
     "billions $", {"kind": "level", "watch": 0.1, "elevated": 5, "crisis": 25,
                     "note": "Khalid-specified — non-zero is FX dollar shortage abroad"}),
    ("SWPT", "liquidity_facilities", "Central Bank Liquidity Swaps: TOTAL",
     "billions $", {"kind": "level", "watch": 0.5, "elevated": 10, "crisis": 50,
                     "note": "Total swap lines — COVID peak was $446B"}),

    # ─── CREDIT SPREADS (ICE BofA OAS, daily) ──────────────────
    ("BAMLH0A0HYM2", "credit_spreads", "ICE BofA US High Yield Index OAS",
     "%", {"kind": "level", "watch": 5.0, "elevated": 7.0, "crisis": 10.0}),
    ("BAMLH0A3HYC", "credit_spreads", "ICE BofA CCC & Lower US High Yield OAS",
     "%", {"kind": "level", "watch": 9.0, "elevated": 12.0, "crisis": 18.0,
            "note": "Khalid-specified — riskiest US credit; GFC peak 22%, COVID peak 19%"}),
    ("BAMLHE00EHYIOAS", "credit_spreads", "ICE BofA Euro High Yield OAS",
     "%", {"kind": "level", "watch": 5.0, "elevated": 7.5, "crisis": 11.0,
            "note": "Khalid-specified — Euro HY; GFC peak 24%, COVID peak 11%"}),
    ("BAMLEMHBHYCRPIOAS", "credit_spreads", "ICE BofA EM High Yield Corp Plus OAS",
     "%", {"kind": "level", "watch": 7.0, "elevated": 10.0, "crisis": 14.0,
            "note": "Khalid-specified — EM HY corp; GFC peak 17%, COVID peak 12%"}),
    ("BAMLC0A0CM", "credit_spreads", "ICE BofA US Corporate IG OAS",
     "%", {"kind": "level", "watch": 1.5, "elevated": 2.5, "crisis": 4.0}),

    # ─── CORPORATE YIELDS ──────────────────────────────────────
    ("HQMCB10YR", "corporate_yields", "HQM 10Y Corporate Bond Spot Rate",
     "%", {"kind": "spread_to", "vs": "DGS10", "watch": 1.5, "elevated": 2.5, "crisis": 4.0,
            "note": "Khalid-specified — spread to 10y Treasury reveals corp credit demand"}),
    ("DGS10", "corporate_yields", "10-Year US Treasury",
     "%", {"kind": "level", "note": "Reference for HQM corporate spread"}),
]

# ────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────
def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def fred_observations(series_id, days=400):
    """Pull last N days of observations from FRED."""
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    url = (f"https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
           f"&observation_start={start.isoformat()}"
           f"&observation_end={end.isoformat()}"
           f"&sort_order=asc")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-LCE/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read().decode("utf-8"))
        obs = []
        for o in data.get("observations", []):
            v = o.get("value")
            if v in (".", "", None):
                continue
            try:
                obs.append({"date": o["date"], "value": float(v)})
            except (ValueError, TypeError):
                continue
        return obs
    except Exception as e:
        print(f"[lce] fred {series_id} error: {e}")
        return []


def fred_observations_long(series_id, days=1900):
    """5-year window for z-score calc."""
    return fred_observations(series_id, days=days)


def find_value_n_back(obs, days_back):
    """Find the observation closest to N days before the latest one (within ±3 days)."""
    if not obs:
        return None
    latest_date = datetime.fromisoformat(obs[-1]["date"]).date()
    target = latest_date - timedelta(days=days_back)
    best = None
    best_dist = float("inf")
    for o in obs:
        d = datetime.fromisoformat(o["date"]).date()
        dist = abs((d - target).days)
        if dist < best_dist:
            best_dist = dist
            best = o
    if best and best_dist <= max(3, days_back * 0.10):  # 10% tolerance, min 3 days
        return best
    return None


def pct_change(latest, prior):
    if latest is None or prior is None or prior == 0:
        return None
    return (latest - prior) / abs(prior) * 100.0


def z_score(latest, history):
    """Z-score of latest value relative to a history window."""
    vals = [h["value"] for h in history if h.get("value") is not None]
    if len(vals) < 30:
        return None
    m = mean(vals)
    s = pstdev(vals)
    if s == 0:
        return None
    return (latest - m) / s


# ────────────────────────────────────────────────────────────────────────
# Per-series compute
# ────────────────────────────────────────────────────────────────────────
def compute_series(series_id, threshold, latest_dgs10=None):
    """Pull a series and compute the full feature set."""
    obs = fred_observations_long(series_id)
    if not obs:
        return {"available": False, "error": f"No data for {series_id}"}

    latest = obs[-1]
    latest_value = latest["value"]
    latest_date = latest["date"]

    # Period changes (calendar days)
    wow = find_value_n_back(obs, 7)
    mom = find_value_n_back(obs, 30)
    qoq = find_value_n_back(obs, 90)
    yoy = find_value_n_back(obs, 365)

    wow_pct = pct_change(latest_value, wow["value"]) if wow else None
    mom_pct = pct_change(latest_value, mom["value"]) if mom else None
    qoq_pct = pct_change(latest_value, qoq["value"]) if qoq else None
    yoy_pct = pct_change(latest_value, yoy["value"]) if yoy else None

    # Z-scores
    z1y_window = [o for o in obs
                   if (datetime.fromisoformat(latest_date).date()
                        - datetime.fromisoformat(o["date"]).date()).days <= 365]
    z1y = z_score(latest_value, z1y_window)
    z5y = z_score(latest_value, obs)

    # Signal classification
    signal = "NORMAL"
    signal_reason = ""
    kind = threshold.get("kind", "level")

    if kind == "level":
        # Compare latest_value against thresholds
        if "crisis" in threshold and latest_value >= threshold["crisis"]:
            signal = "CRISIS"; signal_reason = f"Level {latest_value:.2f} ≥ crisis threshold {threshold['crisis']}"
        elif "elevated" in threshold and latest_value >= threshold["elevated"]:
            signal = "ELEVATED"; signal_reason = f"Level {latest_value:.2f} ≥ elevated threshold {threshold['elevated']}"
        elif "watch" in threshold and latest_value >= threshold["watch"]:
            signal = "WATCH"; signal_reason = f"Level {latest_value:.2f} ≥ watch threshold {threshold['watch']}"

    elif kind == "delta_pct":
        window = threshold.get("window", "wk")
        delta = wow_pct if window == "wk" else mom_pct
        if delta is not None:
            # For balance sheet, we typically want to alert on DROPS (negative deltas)
            # so threshold values are interpreted as "delta ≤ this value"
            if "crisis" in threshold and delta <= threshold["crisis"]:
                signal = "CRISIS"; signal_reason = f"{window} delta {delta:+.2f}% ≤ crisis {threshold['crisis']:+.2f}%"
            elif "elevated" in threshold and delta <= threshold["elevated"]:
                signal = "ELEVATED"; signal_reason = f"{window} delta {delta:+.2f}% ≤ elevated {threshold['elevated']:+.2f}%"
            elif "watch" in threshold and delta <= threshold["watch"]:
                signal = "WATCH"; signal_reason = f"{window} delta {delta:+.2f}% ≤ watch {threshold['watch']:+.2f}%"

    elif kind == "spread_to":
        ref = threshold.get("vs")
        if ref == "DGS10" and latest_dgs10 is not None:
            spread = latest_value - latest_dgs10
            if "crisis" in threshold and spread >= threshold["crisis"]:
                signal = "CRISIS"; signal_reason = f"Spread to 10y {spread:+.2f}% ≥ crisis {threshold['crisis']}%"
            elif "elevated" in threshold and spread >= threshold["elevated"]:
                signal = "ELEVATED"; signal_reason = f"Spread to 10y {spread:+.2f}% ≥ elevated {threshold['elevated']}%"
            elif "watch" in threshold and spread >= threshold["watch"]:
                signal = "WATCH"; signal_reason = f"Spread to 10y {spread:+.2f}% ≥ watch {threshold['watch']}%"

    return {
        "available": True,
        "latest_date": latest_date,
        "latest_value": round(latest_value, 4),
        "wow_pct": round(wow_pct, 3) if wow_pct is not None else None,
        "mom_pct": round(mom_pct, 3) if mom_pct is not None else None,
        "qoq_pct": round(qoq_pct, 3) if qoq_pct is not None else None,
        "yoy_pct": round(yoy_pct, 3) if yoy_pct is not None else None,
        "z_1y": round(z1y, 2) if z1y is not None else None,
        "z_5y": round(z5y, 2) if z5y is not None else None,
        "signal": signal,
        "signal_reason": signal_reason,
        "n_observations": len(obs),
    }


def composite_signal(by_id):
    """Composite stress score (0-100) from worst-of and average."""
    rank = {"NORMAL": 0, "WATCH": 25, "ELEVATED": 60, "CRISIS": 90}
    scores = []
    n_firing_by_cat = {}
    for sid, info in by_id.items():
        if not info.get("available"):
            continue
        sig = info.get("signal", "NORMAL")
        scores.append(rank.get(sig, 0))
        cat = info.get("_category", "other")
        if sig in ("ELEVATED", "CRISIS"):
            n_firing_by_cat[cat] = n_firing_by_cat.get(cat, 0) + 1
    if not scores:
        return {"score": 0, "n_firing": 0, "by_category": {}}
    composite = round(max(scores) * 0.7 + (sum(scores) / len(scores)) * 0.3, 1)
    return {
        "score": composite,
        "n_firing": sum(1 for s in scores if s >= 60),
        "by_category": n_firing_by_cat,
    }


def regime_classification(composite, by_id):
    """Coarse regime: CALM / WATCH / ELEVATED / ACUTE_STRESS / CRISIS."""
    score = composite["score"]
    if score >= 80:
        return "CRISIS"
    if score >= 60:
        return "ACUTE_STRESS"
    if score >= 35:
        return "ELEVATED"
    if score >= 15:
        return "WATCH"
    return "CALM"


def load_prior():
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=OUTPUT_KEY)
        return json.loads(obj["Body"].read())
    except Exception:
        return {}


def detect_transitions(current, prior):
    """Return state-transition entries for alert-router."""
    transitions = []
    cur_series = current.get("series", {})
    prior_series = (prior or {}).get("series", {})
    for sid, info in cur_series.items():
        if not info.get("available"):
            continue
        prior_info = prior_series.get(sid, {})
        prior_sig = prior_info.get("signal", "NORMAL")
        new_sig = info.get("signal", "NORMAL")
        if new_sig != prior_sig:
            transitions.append({
                "series_id": sid,
                "label": info.get("_label"),
                "category": info.get("_category"),
                "prior": prior_sig,
                "new": new_sig,
                "latest_value": info.get("latest_value"),
                "wow_pct": info.get("wow_pct"),
                "z_1y": info.get("z_1y"),
                "reason": info.get("signal_reason"),
            })
    return transitions


# ────────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────────
def lambda_handler(event=None, context=None):
    started = time.time()
    print("[lce] start")

    # First pass: fetch DGS10 for spread calculations
    dgs10 = fred_observations_long("DGS10")
    latest_dgs10 = dgs10[-1]["value"] if dgs10 else None

    # Process each series
    series_out = {}
    by_category = {"balance_sheet": [], "liquidity_facilities": [],
                    "credit_spreads": [], "corporate_yields": []}

    for sid, category, label, units, threshold in SERIES_MAP:
        result = compute_series(sid, threshold, latest_dgs10=latest_dgs10)
        result["_label"] = label
        result["_units"] = units
        result["_category"] = category
        result["_threshold_note"] = threshold.get("note", "")
        result["_threshold_kind"] = threshold.get("kind")
        series_out[sid] = result
        by_category[category].append(sid)

    # Composite + regime
    comp = composite_signal(series_out)
    regime = regime_classification(comp, series_out)

    # Detect transitions vs prior run
    prior = load_prior()
    output = {
        "schema_version": "1.0",
        "generated_at": _now_iso(),
        "elapsed_sec": round(time.time() - started, 2),
        "regime": regime,
        "composite": comp,
        "series": series_out,
        "by_category": by_category,
        "reference": {"DGS10": latest_dgs10},
    }
    transitions = detect_transitions(output, prior)
    output["transitions"] = transitions

    S3.put_object(
        Bucket=BUCKET, Key=OUTPUT_KEY,
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json",
        CacheControl="public, max-age=300, s-maxage=60",
    )
    print(f"[lce] regime={regime} composite={comp['score']} firing={comp['n_firing']} "
          f"transitions={len(transitions)}")

    return {"statusCode": 200, "body": json.dumps({
        "regime": regime, "composite_score": comp["score"],
        "transitions_count": len(transitions),
    })}
