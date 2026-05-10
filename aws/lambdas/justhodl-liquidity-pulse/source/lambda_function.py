"""
justhodl-liquidity-pulse — Unified liquidity & credit-stress pulse.

Single source-of-truth Lambda that consolidates the FRED series most
predictive of Fed liquidity moves, credit-cycle inflection, and FX-swap
stress. Output: data/liquidity-pulse.json (refreshed every 6 hours).

Series tracked (per Khalid's spec, with full FRED IDs):

  ── FED BALANCE SHEET (liquidity proxies) ──
    WALCL              Total Assets of the Federal Reserve (H.4.1 headline)
    WRESBAL            Reserve Balances at Federal Reserve Banks
    WTREGEN            Treasury General Account (TGA) at NY Fed
    RESPPALGUONNWW     Securities Held Outright: UST Notes & Bonds (Wed level)
    RESPPNTEPNWW       Memo: UST/Agency/MBS Eligible to be Pledged

  ── EMERGENCY FACILITIES (crisis signals) ──
    OTHL1690           Loans: Maturing 16-90d (primary credit window —
                       NON-ZERO = real-time crisis indicator)
    SWP1690            Central Bank Liquidity Swaps: Maturing 16-90d
                       (offshore USD dispensing = FX stress signal)

  ── CREDIT STRESS (Fed-policy-anticipating signals) ──
    BAMLH0A3HYC        ICE BofA CCC & Lower US HY OAS (deep junk)
    BAMLHE00EHYIOAS    ICE BofA Euro HY OAS
    BAMLEMHBHYCRPIOAS  ICE BofA HY EM Corp Plus OAS
    HQMCB10YR          10Y HQM Corporate Bond Spot Rate

For each series we compute, vs latest available point:
    wow_pct    week-over-week  (~1 obs back for weekly, ~5 for daily)
    mom_pct    month-over-month (~4 obs back for weekly, ~21 for daily)
    qoq_pct    quarter-over-quarter (~13 obs back for weekly, ~63 for daily)
    yoy_pct    year-over-year (~52 obs back for weekly, ~252 for daily)
    z_score    1-year rolling z-score (z=2 → ELEVATED, z=3 → CRISIS)

PLUS:
  • Composite liquidity score (0-100) from balance-sheet trajectory
  • Composite credit-stress score (0-100) from spread z-scores
  • Plain-English interpretation per signal
  • State transitions detected vs prior run → fed to alert-router

NO DUPLICATION of existing scrapers — this Lambda is THE canonical place
to read multi-horizon liquidity & credit deltas across the platform. Other
pages should consume data/liquidity-pulse.json rather than rolling their own.
"""
import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from statistics import mean, pstdev

import boto3
from botocore.exceptions import ClientError

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUTPUT_KEY = "data/liquidity-pulse.json"
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")

# ────────────────────────────────────────────────────────────────────────
# Series catalog
# ────────────────────────────────────────────────────────────────────────
# group, frequency, signal_polarity, label, description
# polarity: +1 means "rising = good" (e.g. WALCL up = liquidity adding)
#           -1 means "rising = bad" (e.g. credit spreads widening = stress)
SERIES = [
    # FED BALANCE SHEET — "Securities Held Outright, etc."
    ("WALCL",            "balance",  "weekly",  +1,
     "Fed Total Assets (H.4.1)",
     "Total balance sheet. Rising = QE/expansion. Falling = QT/drain."),
    ("WRESBAL",          "balance",  "weekly",  +1,
     "Reserve Balances",
     "Bank reserves at the Fed. Rising = liquidity flush. Falling fast = drain stress."),
    ("WTREGEN",          "balance",  "weekly",  -1,
     "Treasury General Account",
     "TGA (Treasury's checking account at NY Fed). TGA UP = drain on system. TGA DOWN = liquidity release."),
    ("RESPPALGUONNWW",   "balance",  "weekly",  +1,
     "UST Notes & Bonds Held Outright",
     "Direct measure of Fed coupon Treasury holdings. Falling = QT runoff."),
    ("RESPPNTEPNWW",     "balance",  "weekly",  +1,
     "UST/Agency/MBS Eligible to be Pledged",
     "Memo line: collateral the Fed pledges for currency. Tells you about back-book pledging dynamics."),
    # EMERGENCY FACILITIES
    ("OTHL1690",         "facility", "weekly",  -1,
     "Primary Credit Loans 16-90d",
     "Loans extended via discount window 16-90 days out. NON-ZERO = banks being forced to use last resort = REAL-TIME CRISIS SIGNAL."),
    ("SWP1690",          "facility", "weekly",  -1,
     "Central Bank Liquidity Swaps 16-90d",
     "Dollars dispensed to foreign CBs maturing 16-90d. NON-ZERO = offshore USD shortage = same signal as 2008/2020/2023."),
    # CREDIT STRESS
    ("BAMLH0A3HYC",      "credit",   "daily",   -1,
     "CCC HY OAS",
     "Spread on the worst-rated corporate junk. The deepest credit-stress canary. Fed-cut anticipator when blowing out."),
    ("BAMLHE00EHYIOAS",  "credit",   "daily",   -1,
     "Euro HY OAS",
     "European HY OAS. Cross-checks US — Euro HY blowing out while US calm = imported stress imminent."),
    ("BAMLEMHBHYCRPIOAS","credit",   "daily",   -1,
     "EM HY Corp OAS",
     "Emerging-market high-yield corporate spread. Dollar funding stress + EM solvency canary."),
    ("HQMCB10YR",        "credit",   "daily",   -1,
     "HQM 10Y Corp Spot Rate",
     "10Y high-quality market corporate bond yield. Spread vs 10Y UST = IG credit risk premium."),
]

# ────────────────────────────────────────────────────────────────────────
# Signal thresholds
# ────────────────────────────────────────────────────────────────────────
THRESHOLDS = {
    # Z-score abs values for credit spread alerts
    "credit_z_watch": 1.0,
    "credit_z_elevated": 2.0,
    "credit_z_crisis": 3.0,
    # Facility-level absolute thresholds (USD millions)
    "facility_watch_usd": 100,        # any non-zero on emergency facilities
    "facility_elevated_usd": 5_000,
    "facility_crisis_usd": 50_000,
    # Balance sheet drain rate (4w pct change)
    "drain_watch_pct": -1.0,
    "drain_elevated_pct": -2.5,
    "drain_crisis_pct": -5.0,
}


# ────────────────────────────────────────────────────────────────────────
# FRED helpers
# ────────────────────────────────────────────────────────────────────────
def fetch_fred(series_id, observations=400, retries=2):
    """Pull most recent N observations of a FRED series. Returns list of
       {date, value} dicts sorted ascending by date."""
    url = (f"https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
           f"&sort_order=desc&limit={observations}")
    last_err = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-LiquidityPulse/1.0"})
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.loads(r.read().decode("utf-8"))
            obs = data.get("observations", []) or []
            cleaned = []
            for o in obs:
                v = o.get("value")
                if v in ("", ".", None):
                    continue
                try:
                    cleaned.append({"date": o["date"], "value": float(v)})
                except (TypeError, ValueError):
                    continue
            cleaned.sort(key=lambda x: x["date"])
            return cleaned
        except Exception as e:
            last_err = str(e)
            if attempt < retries:
                time.sleep(0.5)
    print(f"[liq-pulse] {series_id} fetch failed: {last_err}")
    return []


# ────────────────────────────────────────────────────────────────────────
# Multi-horizon delta + z-score
# ────────────────────────────────────────────────────────────────────────
def pct_change_at(observations, days_back):
    """Find observation closest to N days before latest, compute pct change."""
    if len(observations) < 2:
        return None
    latest = observations[-1]
    target = datetime.fromisoformat(latest["date"]).date() - timedelta(days=days_back)
    target_str = target.isoformat()
    # Find closest observation on or before target_str
    best = None
    for o in observations[:-1]:
        if o["date"] <= target_str:
            if best is None or o["date"] > best["date"]:
                best = o
    if best is None or best["value"] == 0:
        return None
    return (latest["value"] - best["value"]) / abs(best["value"]) * 100


def abs_change_at(observations, days_back):
    """Absolute (not pct) change vs N days back. Used for spreads."""
    if len(observations) < 2:
        return None
    latest = observations[-1]
    target = datetime.fromisoformat(latest["date"]).date() - timedelta(days=days_back)
    target_str = target.isoformat()
    best = None
    for o in observations[:-1]:
        if o["date"] <= target_str:
            if best is None or o["date"] > best["date"]:
                best = o
    if best is None:
        return None
    return latest["value"] - best["value"]


def rolling_zscore(observations, days_window=365):
    """Compute z-score of latest value vs trailing rolling-window mean+std."""
    if len(observations) < 30:
        return None
    latest = observations[-1]
    cutoff = datetime.fromisoformat(latest["date"]).date() - timedelta(days=days_window)
    cutoff_str = cutoff.isoformat()
    # Use observations within window (excluding latest)
    window = [o["value"] for o in observations[:-1] if o["date"] >= cutoff_str]
    if len(window) < 10:
        return None
    m = mean(window)
    s = pstdev(window)
    if s == 0:
        return None
    return (latest["value"] - m) / s


# ────────────────────────────────────────────────────────────────────────
# Signal classification
# ────────────────────────────────────────────────────────────────────────
def classify_credit_signal(z_score):
    if z_score is None:
        return "UNKNOWN"
    az = abs(z_score)
    if z_score > THRESHOLDS["credit_z_crisis"]:
        return "CRISIS"
    if z_score > THRESHOLDS["credit_z_elevated"]:
        return "ELEVATED"
    if z_score > THRESHOLDS["credit_z_watch"]:
        return "WATCH"
    if z_score < -THRESHOLDS["credit_z_elevated"]:
        return "TIGHTENING"  # spreads compressed unusually = euphoria
    return "NORMAL"


def classify_facility_signal(value_usd_millions, group):
    """Emergency facility thresholds (in $M)."""
    if value_usd_millions is None:
        return "UNKNOWN"
    v = abs(value_usd_millions)
    if v >= THRESHOLDS["facility_crisis_usd"]:
        return "CRISIS"
    if v >= THRESHOLDS["facility_elevated_usd"]:
        return "ELEVATED"
    if v >= THRESHOLDS["facility_watch_usd"]:
        return "WATCH"
    return "NORMAL"


def classify_balance_signal(mom_pct, polarity):
    """Balance-sheet item: rising/falling vs polarity."""
    if mom_pct is None:
        return "UNKNOWN"
    # Effective change accounting for polarity
    # +1 polarity: drop = bad
    # -1 polarity: rise = bad
    eff = mom_pct * polarity
    if eff < THRESHOLDS["drain_crisis_pct"]:
        return "CRISIS"
    if eff < THRESHOLDS["drain_elevated_pct"]:
        return "ELEVATED"
    if eff < THRESHOLDS["drain_watch_pct"]:
        return "WATCH"
    if eff > 2.5:
        return "EXPANDING"  # liquidity adding rapidly (good)
    return "NORMAL"


def interpret(signal, group, label, polarity, latest, deltas):
    """Plain-English interpretation per series."""
    wow = deltas.get("wow_pct")
    mom = deltas.get("mom_pct")
    yoy = deltas.get("yoy_pct")

    if group == "credit":
        if signal == "CRISIS":
            return (f"{label} blowing out — z-score in 99th-pct of past year. "
                     "Historically precedes Fed liquidity response by 2-8 weeks. Risk-off positioning.")
        if signal == "ELEVATED":
            return (f"{label} elevated — credit-cycle inflection signal. "
                     "Watch for rate-cut anticipation in 2y note auctions; trim risk-on at margins.")
        if signal == "WATCH":
            return f"{label} drifting wider — early-cycle stress visible but not yet breaking out."
        if signal == "TIGHTENING":
            return f"{label} compressed unusually tight (z<-2) — credit euphoria, late-cycle complacency."
        if mom is not None and mom > 5:
            return f"{label} widening modestly. Not a crisis signal but worth tracking."
        return f"{label} stable. No credit-cycle stress signal."

    if group == "facility":
        if signal in ("CRISIS", "ELEVATED"):
            return (f"{label} ACTIVE — banks/foreign CBs drawing emergency liquidity. "
                     "This is the same signature seen in March 2008, March 2020, March 2023. Critical alert.")
        if signal == "WATCH":
            return f"{label} non-zero — light usage of emergency facility. Track week-over-week trajectory."
        return f"{label} dormant. No emergency-facility usage."

    if group == "balance":
        if signal == "CRISIS":
            direction = "draining" if polarity == 1 else "expanding"
            return (f"{label} {direction} {abs(mom):.1f}% MoM — extreme regime move. "
                     "Liquidity backdrop incompatible with sustained risk-on rally.")
        if signal == "ELEVATED":
            direction = "draining" if polarity == 1 else "expanding"
            return f"{label} {direction} {abs(mom):.1f}% MoM. Liquidity headwind for risk assets."
        if signal == "EXPANDING":
            return f"{label} +{mom:.1f}% MoM — liquidity tailwind. Historically supports BTC and risk assets."
        if signal == "WATCH":
            return f"{label} drifting; YoY {yoy:+.1f}% if available. Worth tracking but not actionable yet."
        return f"{label} steady. Liquidity backdrop neutral."

    return ""


# ────────────────────────────────────────────────────────────────────────
# Composite scores
# ────────────────────────────────────────────────────────────────────────
def composite_credit_score(series_results):
    """0-100 composite of credit-stress series (higher = more stress)."""
    credit = [r for r in series_results.values() if r.get("group") == "credit"
               and r.get("z_score") is not None]
    if not credit:
        return None, "UNKNOWN"
    # Map z-score to 0-100. z=0 → 50, z=+3 → 95, z=-3 → 5
    scores = [50 + (r["z_score"] * 15) for r in credit]
    composite = sum(scores) / len(scores)
    composite = max(0, min(100, composite))
    if composite >= 85:
        regime = "CRISIS"
    elif composite >= 70:
        regime = "ELEVATED"
    elif composite >= 60:
        regime = "WATCH"
    elif composite <= 30:
        regime = "TIGHT_EUPHORIA"  # spreads abnormally compressed
    else:
        regime = "NORMAL"
    return round(composite, 1), regime


def composite_liquidity_score(series_results):
    """0-100 of liquidity adequacy. Higher = tighter (worse for risk assets)."""
    bal = [r for r in series_results.values() if r.get("group") == "balance"]
    if not bal:
        return None, "UNKNOWN"
    # Polarity-adjusted MoM: how much liquidity is draining
    drains = []
    for r in bal:
        mom = r.get("deltas", {}).get("mom_pct")
        pol = r.get("polarity", 1)
        if mom is None:
            continue
        # Drain is when polarity*mom < 0 (e.g. WALCL down, TGA up)
        drains.append(-mom * pol)
    if not drains:
        return None, "UNKNOWN"
    avg_drain = sum(drains) / len(drains)
    # Map to 0-100: avg_drain=0 → 50, avg_drain=+5 (draining 5%) → 100
    score = 50 + (avg_drain * 10)
    score = max(0, min(100, score))
    if score >= 80:
        regime = "ACUTE_DRAIN"
    elif score >= 65:
        regime = "DRAINING"
    elif score >= 55:
        regime = "TIGHTENING"
    elif score <= 35:
        regime = "EXPANDING"
    else:
        regime = "NEUTRAL"
    return round(score, 1), regime


# ────────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────────
def load_prior():
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=OUTPUT_KEY)
        return json.loads(obj["Body"].read())
    except Exception:
        return {}


def detect_transitions(current, prior):
    """Detect signal-state transitions vs prior run."""
    prior_series = (prior.get("series") or {})
    transitions = []
    for sid, r in current.items():
        prev = (prior_series.get(sid) or {})
        new_state = r.get("signal", "UNKNOWN")
        old_state = prev.get("signal", "UNKNOWN")
        if old_state != new_state and new_state != "UNKNOWN" and old_state != "UNKNOWN":
            transitions.append({
                "series_id": sid,
                "label": r.get("label"),
                "group": r.get("group"),
                "prior_state": old_state,
                "new_state": new_state,
                "interpretation": r.get("interpretation"),
            })
    return transitions


def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[liq-pulse] start, fetching {len(SERIES)} series")

    series_results = {}
    fetch_errors = {}

    for series_id, group, freq, polarity, label, desc in SERIES:
        obs = fetch_fred(series_id, observations=400 if freq == "weekly" else 800)
        if not obs:
            fetch_errors[series_id] = "no_data"
            continue
        latest = obs[-1]
        latest_value = latest["value"]
        latest_date = latest["date"]

        # Compute multi-horizon deltas
        if group == "credit":
            # For credit spreads, also compute absolute change in pct points
            deltas = {
                "wow_pct": pct_change_at(obs, 7),
                "mom_pct": pct_change_at(obs, 30),
                "qoq_pct": pct_change_at(obs, 91),
                "yoy_pct": pct_change_at(obs, 365),
                "wow_abs": abs_change_at(obs, 7),
                "mom_abs": abs_change_at(obs, 30),
                "qoq_abs": abs_change_at(obs, 91),
                "yoy_abs": abs_change_at(obs, 365),
            }
        else:
            deltas = {
                "wow_pct": pct_change_at(obs, 7),
                "mom_pct": pct_change_at(obs, 30),
                "qoq_pct": pct_change_at(obs, 91),
                "yoy_pct": pct_change_at(obs, 365),
            }

        z = rolling_zscore(obs, 365)

        # Classify
        if group == "credit":
            signal = classify_credit_signal(z)
        elif group == "facility":
            signal = classify_facility_signal(latest_value, group)
        else:  # balance
            signal = classify_balance_signal(deltas.get("mom_pct"), polarity)

        interp = interpret(signal, group, label, polarity, latest_value, deltas)

        series_results[series_id] = {
            "series_id": series_id,
            "group": group,
            "frequency": freq,
            "polarity": polarity,
            "label": label,
            "description": desc,
            "latest_value": round(latest_value, 4),
            "latest_date": latest_date,
            "deltas": {k: (round(v, 3) if v is not None else None) for k, v in deltas.items()},
            "z_score": round(z, 3) if z is not None else None,
            "signal": signal,
            "interpretation": interp,
        }
        print(f"[liq-pulse] {series_id} {label} latest={latest_value} signal={signal}")

    # Composites
    credit_score, credit_regime = composite_credit_score(series_results)
    liquidity_score, liquidity_regime = composite_liquidity_score(series_results)

    # Transitions
    prior = load_prior()
    transitions = detect_transitions(series_results, prior)
    print(f"[liq-pulse] transitions: {len(transitions)}")

    # Composite plain-English summary
    if credit_regime == "CRISIS":
        summary = "🔴 CREDIT CRISIS — multiple credit-spread series in 99th-pct stress. Fed liquidity response likely within 2-8 weeks."
    elif liquidity_regime == "ACUTE_DRAIN":
        summary = "🔴 ACUTE LIQUIDITY DRAIN — Fed balance sheet draining at extreme pace. Risk-off backdrop confirmed."
    elif credit_regime == "ELEVATED":
        summary = "🟠 CREDIT STRESS BUILDING — junk + EM + Euro HY widening. Trim risk at the margin."
    elif liquidity_regime == "DRAINING" or credit_regime == "WATCH":
        summary = "🟡 LIQUIDITY/CREDIT WATCH — early signals of regime shift. Track weekly."
    elif credit_regime == "TIGHT_EUPHORIA":
        summary = "🟢 CREDIT EUPHORIA — spreads abnormally compressed. Late-cycle complacency, not stress."
    elif liquidity_regime == "EXPANDING":
        summary = "🟢 LIQUIDITY EXPANSION — Fed adding to balance sheet. Tailwind for risk assets."
    else:
        summary = "🟢 LIQUIDITY & CREDIT NORMAL — no notable stress signals."

    output = {
        "schema_version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_sec": round(time.time() - started, 2),
        "series": series_results,
        "fetch_errors": fetch_errors,
        "composites": {
            "credit_stress_score": credit_score,
            "credit_regime": credit_regime,
            "liquidity_score": liquidity_score,
            "liquidity_regime": liquidity_regime,
        },
        "summary": summary,
        "transitions": transitions,
        "n_series": len(SERIES),
        "n_series_ok": len(series_results),
    }

    S3.put_object(
        Bucket=BUCKET, Key=OUTPUT_KEY,
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json",
        CacheControl="public, max-age=300, s-maxage=120",
    )
    print(f"[liq-pulse] OK n={len(series_results)} credit={credit_score}/{credit_regime} "
          f"liquidity={liquidity_score}/{liquidity_regime}")

    return {"statusCode": 200, "body": json.dumps({
        "n_series_ok": len(series_results),
        "credit_score": credit_score,
        "liquidity_score": liquidity_score,
        "transitions": len(transitions),
    })}
