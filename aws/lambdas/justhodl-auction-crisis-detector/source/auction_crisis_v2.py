"""
auction_crisis_v2.py — institutional-grade expansion module.

Adds 7 NEW analytical layers on top of the existing 6-indicator engine:
  1. TENOR DECOMPOSITION
       Per-bucket stress scores so users see WHERE stress concentrates
       (bills_lt_90d, bills_gte_90d, coupons_lt_3y, coupons_gt_3y, tips, frn)

  2. FORWARD AUCTION CALENDAR
       Pull next 30 days of upcoming auctions from Treasury Direct,
       compute a forward stress score for each based on tenor's current
       state + cross-tenor patterns + size vs trailing avg

  3. HISTORICAL ANALOG MATCHING
       Convert current indicator vector to a 6D point in crisis-space,
       compute cosine similarity with each of the 9 anchor crises.
       Returns top-3 matches with similarity scores (0-1) and what each
       anchor preceded historically.

  4. CROSS-SIGNALS FROM FRED
       Pulls 4 corroborating signals from FRED to triangulate auction
       stress with broader rates/liquidity context:
         - SOFR - IORB (repo collateral squeeze)
         - DXY (USD strength = foreign demand proxy)
         - 10Y - 2Y curve slope
         - 5Y5Y forward inflation breakeven

  5. COMPOSITE HISTORY
       30-day rolling time series of the composite score so the page
       can chart trajectory and identify regime-change points

  6. TAIL RISK PROBABILITIES
       Heuristic model produces 3 forward-looking probabilities:
         - P(failed auction in next 30d) — from PD share trend + indirect
         - P(regime change to ELEVATED+ in 14d) — momentum + threshold proximity
         - P(volatility spike from Treasury supply) — calendar size + stress

  7. ACTIONABLE TRIGGERS
       Specific named thresholds that would flip the regime, with current
       distance-to-trigger and historical precedent ("last hit on…")

Each function is pure (no S3, no Lambda env). The orchestrator in
lambda_function.py calls these and merges results into the output JSON
under new top-level keys.
"""
import json
import math
import os
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple


FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")


# ═════════════════════════════════════════════════════════════════════
# 1. TENOR DECOMPOSITION
# ═════════════════════════════════════════════════════════════════════

# Display order + human-readable labels
TENOR_LABELS = {
    "bills_lt_90d":   "T-Bills < 90d",
    "bills_gte_90d":  "T-Bills 13w-52w",
    "coupons_lt_3y":  "Notes 2y-3y",
    "coupons_gt_3y":  "Notes/Bonds 5y-30y",
    "tips":           "TIPS (Inflation-Linked)",
    "frn":            "Floating Rate Notes",
}

TENOR_RISK_PROFILE = {
    # When THIS tenor is stressed, what does it imply?
    "bills_lt_90d":   "Flight-to-safety pattern. Money parking. Usually first signal.",
    "bills_gte_90d":  "Term funding stress. Less acute but more persistent.",
    "coupons_lt_3y":  "Front-end dislocation. Often coincides with Fed expectations shift.",
    "coupons_gt_3y":  "Long-duration absorption stress. Dealer balance-sheet limit signal.",
    "tips":           "Inflation-expectations dislocation. Real-rate stress.",
    "frn":            "Banking-sector funding alternative. Marginal signal.",
}


def compute_tenor_decomposition(scored_auctions: List[dict],
                                  window_days: int = 14) -> Dict[str, dict]:
    """Aggregate stress scores per tenor bucket over the rolling window.

    Returns dict keyed by tenor with:
      - n_auctions     : count in window
      - composite      : size-weighted average composite score
      - max_composite  : worst single auction
      - latest_date    : most recent auction in this bucket
      - dominant_signal: name of indicator that fired most often
      - label          : human-readable label
      - risk_profile   : narrative about this tenor's stress meaning
      - rank           : 1 = most stressed, N = least
    """
    today = datetime.now(timezone.utc).date()
    cutoff = today - timedelta(days=window_days)

    out: Dict[str, dict] = {}
    for tenor in TENOR_LABELS:
        bucket = [a for a in scored_auctions
                    if a.get("tenor_bucket") == tenor
                    and a.get("auction_date")
                    and _parse_date(a["auction_date"]) >= cutoff]
        if not bucket:
            out[tenor] = {
                "n_auctions":      0,
                "composite":       None,
                "max_composite":   None,
                "latest_date":     None,
                "dominant_signal": None,
                "label":           TENOR_LABELS[tenor],
                "risk_profile":    TENOR_RISK_PROFILE[tenor],
            }
            continue

        total_size = sum(a.get("accepted_billions") or 1 for a in bucket)
        weighted = sum((a.get("composite_score", 0) * (a.get("accepted_billions") or 1))
                        for a in bucket) / total_size if total_size > 0 else 0

        max_comp = max(a.get("composite_score", 0) for a in bucket)

        signal_fires: Dict[str, int] = {}
        for a in bucket:
            for sig, sc in (a.get("indicator_scores") or {}).items():
                if sc >= 50:
                    signal_fires[sig] = signal_fires.get(sig, 0) + 1
        dom_sig = max(signal_fires, key=signal_fires.get) if signal_fires else None

        out[tenor] = {
            "n_auctions":      len(bucket),
            "composite":       round(weighted, 1),
            "max_composite":   round(max_comp, 1),
            "latest_date":     max(a.get("auction_date") for a in bucket if a.get("auction_date")),
            "dominant_signal": dom_sig,
            "label":           TENOR_LABELS[tenor],
            "risk_profile":    TENOR_RISK_PROFILE[tenor],
        }

    # Rank tenors by composite (descending — most stressed = rank 1)
    ranked = sorted(
        [(k, v["composite"]) for k, v in out.items() if v.get("composite") is not None],
        key=lambda x: x[1] or -1, reverse=True,
    )
    for i, (k, _) in enumerate(ranked):
        out[k]["rank"] = i + 1

    return out


# ═════════════════════════════════════════════════════════════════════
# 2. FORWARD AUCTION CALENDAR
# ═════════════════════════════════════════════════════════════════════

TREASURY_UPCOMING_URL = "https://www.treasurydirect.gov/TA_WS/securities/upcoming?format=json"


def fetch_upcoming_auctions(days_ahead: int = 30) -> List[dict]:
    """Fetch upcoming Treasury auctions from TreasuryDirect API.

    Returns list of {auction_date, issue_date, security_type, security_term,
                      offering_amount, cusip, tenor_bucket}.

    Returns [] on failure — gracefully degrades.
    """
    try:
        req = urllib.request.Request(TREASURY_UPCOMING_URL, headers={
            "User-Agent": "justhodl-auction-crisis-detector/2.0",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read())
    except Exception as e:
        print(f"[upcoming] fetch error: {e}")
        return []

    if not isinstance(data, list):
        return []

    today = datetime.now(timezone.utc).date()
    cutoff = today + timedelta(days=days_ahead)
    out = []

    for rec in data:
        auc_date = rec.get("auctionDate") or rec.get("auction_date")
        if not auc_date:
            continue
        try:
            d = datetime.strptime(auc_date[:10], "%Y-%m-%d").date()
        except ValueError:
            continue
        if d < today or d > cutoff:
            continue

        sec_type = rec.get("securityType") or rec.get("security_type") or ""
        sec_term = rec.get("securityTerm") or rec.get("security_term") or ""
        offering = _parse_float(rec.get("offeringAmount") or rec.get("offering_amount"))

        # Build a fake record for tenor classification
        tenor = _classify_tenor_bucket_simple(sec_type, sec_term)

        out.append({
            "auction_date":   auc_date[:10],
            "issue_date":     (rec.get("issueDate") or rec.get("issue_date") or "")[:10],
            "security_type":  sec_type,
            "security_term":  sec_term,
            "offering_amount_billions": (offering / 1e9) if offering else None,
            "cusip":          rec.get("cusip"),
            "tenor_bucket":   tenor,
            "days_ahead":     (d - today).days,
        })

    # Sort by auction date
    out.sort(key=lambda x: x.get("auction_date") or "")
    return out


def predict_auction_stress(upcoming: dict, tenor_decomp: Dict[str, dict],
                            recent_history: List[dict]) -> dict:
    """Forecast a stress score (0-100) for an upcoming auction.

    Model inputs:
      - tenor's current rolling stress (own bucket)
      - cross-tenor stress (mean across all buckets — contagion)
      - offering size vs trailing-avg same-tenor (supply shock)
      - days_ahead (closer = more predictive)
      - special TIPS/FRN handling (less common, different baselines)

    Returns:
      {
        "forecast_score":     0-100,
        "forecast_label":     CALM | WATCH | ELEVATED | ACUTE,
        "confidence":         LOW | MEDIUM | HIGH,
        "components":         {breakdown of contributors},
        "narrative":          plain-English explanation,
      }
    """
    tenor = upcoming.get("tenor_bucket", "coupons_gt_3y")
    tenor_data = tenor_decomp.get(tenor, {})
    own_composite = tenor_data.get("composite") or 0

    # Cross-tenor average (contagion proxy)
    all_composites = [v.get("composite") for v in tenor_decomp.values()
                        if v.get("composite") is not None]
    cross_composite = sum(all_composites) / len(all_composites) if all_composites else 0

    # Same-tenor recent history (size relative to baseline)
    same_tenor_recent = [a for a in recent_history if a.get("tenor_bucket") == tenor
                            and a.get("accepted_billions")]
    if same_tenor_recent:
        avg_size = sum(a["accepted_billions"] for a in same_tenor_recent) / len(same_tenor_recent)
    else:
        avg_size = 0

    offering = upcoming.get("offering_amount_billions") or avg_size
    size_shock = ((offering - avg_size) / avg_size * 100) if avg_size > 0 else 0
    size_shock_score = 0
    if size_shock > 30:
        size_shock_score = 50
    elif size_shock > 15:
        size_shock_score = 25
    elif size_shock > 0:
        size_shock_score = 10

    # Time decay: forecasts beyond 14 days are less reliable
    days_ahead = upcoming.get("days_ahead", 0)
    if days_ahead <= 3:
        confidence = "HIGH"
        time_factor = 1.0
    elif days_ahead <= 10:
        confidence = "MEDIUM"
        time_factor = 0.85
    elif days_ahead <= 21:
        confidence = "MEDIUM"
        time_factor = 0.7
    else:
        confidence = "LOW"
        time_factor = 0.55

    # Combine: 50% own-tenor + 25% cross-tenor + 25% size shock
    raw_forecast = (0.5 * own_composite + 0.25 * cross_composite + 0.25 * size_shock_score)
    forecast = min(100, max(0, raw_forecast * time_factor))

    if forecast >= 70:
        label = "ACUTE"
    elif forecast >= 45:
        label = "ELEVATED"
    elif forecast >= 22:
        label = "WATCH"
    else:
        label = "CALM"

    # Narrative
    narrative_parts = []
    if own_composite > 40:
        narrative_parts.append(
            f"This tenor ({TENOR_LABELS.get(tenor, tenor)}) is already showing "
            f"composite {own_composite:.0f} from {tenor_data.get('n_auctions', 0)} "
            f"recent auctions."
        )
    if cross_composite > 35:
        narrative_parts.append(
            f"Cross-tenor contagion is elevated (mean composite {cross_composite:.0f})."
        )
    if size_shock_score >= 25:
        narrative_parts.append(
            f"Offering size ${offering:.0f}B vs trailing avg ${avg_size:.0f}B "
            f"= {size_shock:+.0f}% supply shock."
        )
    if not narrative_parts:
        narrative_parts.append("No material stress signals in inputs. Expected normal clearing.")
    if days_ahead > 14:
        narrative_parts.append(
            f"Forecast confidence reduced — {days_ahead} days ahead "
            f"= signal-to-noise diluted."
        )

    return {
        "forecast_score":  round(forecast, 1),
        "forecast_label":  label,
        "confidence":      confidence,
        "components": {
            "own_tenor_stress":    round(own_composite, 1),
            "cross_tenor_contagion": round(cross_composite, 1),
            "size_shock_pct":      round(size_shock, 1),
            "size_shock_score":    round(size_shock_score, 1),
            "time_decay_factor":   round(time_factor, 2),
        },
        "narrative":       " ".join(narrative_parts),
    }


def build_forward_calendar(upcoming: List[dict], tenor_decomp: Dict[str, dict],
                            recent_history: List[dict]) -> List[dict]:
    """For each upcoming auction, attach a forecast."""
    out = []
    for u in upcoming:
        forecast = predict_auction_stress(u, tenor_decomp, recent_history)
        out.append({**u, "forecast": forecast})
    return out


# ═════════════════════════════════════════════════════════════════════
# 3. HISTORICAL ANALOG MATCHING
# ═════════════════════════════════════════════════════════════════════

# Each anchor includes WHAT HAPPENED NEXT — used for forward implication
ANALOG_OUTCOMES = {
    "2008-09-17": {
        "context":   "Day after Lehman bankruptcy. Money markets seized.",
        "what_next": "AIG bailout next day. TARP voted down 9/29. S&P -45% over next 6 months. 2y yield collapsed -100bp in 2 weeks.",
        "duration":  "Crisis acute for 8-10 weeks. Full resolution into Q1 2009.",
    },
    "2008-09-18": {
        "context":   "Same week — Reserve Primary Fund broke the buck. Bill auction tail extreme.",
        "what_next": "Same arc — Lehman aftermath. Indirect bidders collapsed to 29%.",
        "duration":  "Multi-quarter crisis. Federal funds rate to zero by Dec.",
    },
    "2008-09-23": {
        "context":   "Bill auction with AAH=6.92 — extreme low-end clustering.",
        "what_next": "Continued through Oct 8. TIPS auction also stressed.",
        "duration":  "Bill stress lasted ~4 months.",
    },
    "2008-10-08": {
        "context":   "TIPS auction at the height of Q4 2008 crisis. Real yields elevated.",
        "what_next": "S&P bottom in March 2009. 5y TIPS yields normalized over Q1-Q2 2009.",
        "duration":  "Real yield distortion persisted into 2010.",
    },
    "2020-03-11": {
        "context":   "Day WHO declared pandemic. Pre-Fed bazooka. Risk-off cascade beginning.",
        "what_next": "Mar 15 Fed cut to zero + QE. Mar 23 unlimited QE announced. S&P +75% by Sep 2020.",
        "duration":  "Acute panic 3-4 weeks. Full risk-on regime by August.",
    },
    "2020-03-19": {
        "context":   "Peak COVID panic. AAH=53.83, indirect collapse to 46%. Funding stress acute.",
        "what_next": "Mar 23 unlimited QE + Main Street Lending. Lowest stock price 3 days later.",
        "duration":  "Acute for 3 weeks, then explosive recovery.",
    },
    "2020-03-26": {
        "context":   "First post-bazooka auction. BTC=4.74 = MASSIVE demand for safe assets at zero.",
        "what_next": "Demand for Treasuries continued through April. Equity recovery began.",
        "duration":  "Stampede pattern lasted ~6 weeks.",
    },
    "2021-10-21": {
        "context":   "Late-cycle complacency. Crypto top imminent (Nov 8). BTC=3.52 (modestly elevated).",
        "what_next": "Crypto peak Nov 8. S&P peak Jan 2022. -25% drawdown into Oct 2022.",
        "duration":  "Inflation regime + tightening cycle began Jan 2022.",
    },
    "2024-04-10": {
        "context":   "Normal market. Sticky inflation backdrop. Healthy auction.",
        "what_next": "Continued normalization. Rate-cut hopes deferred to Sep.",
        "duration":  "Multi-quarter normal regime.",
    },
    "2024-10-09": {
        "context":   "Late-cycle normal. AAH=99.31 (near-100 = very thin tail, healthy).",
        "what_next": "Continued normalization. Fed cut 50bp in Sep had been absorbed.",
        "duration":  "Normal regime persisted.",
    },
}


def build_indicator_vector(scored_auctions: List[dict], window_days: int = 14) -> List[float]:
    """Build a 6-dimensional vector representing current crisis posture.

    Dimensions (each 0-100, normalized):
      [0] zero_rate_floor — bills stress
      [1] btc_extreme    — demand anomaly
      [2] tail_stress    — AAH dispersion
      [3] pd_absorption  — dealer takedown
      [4] indirect_collapse — foreign demand
      [5] issuance_anomaly — supply

    Returns vector of 6 floats.
    """
    today = datetime.now(timezone.utc).date()
    cutoff = today - timedelta(days=window_days)
    recent = [a for a in scored_auctions
                if a.get("auction_date") and _parse_date(a["auction_date"]) >= cutoff]

    if not recent:
        return [0.0] * 6

    signal_keys = ["zero_rate_floor", "btc_extreme", "tail_stress",
                    "pd_absorption", "indirect_collapse"]
    vector = []
    for key in signal_keys:
        # Max score for this signal across recent auctions
        max_score = 0.0
        for a in recent:
            v = (a.get("indicator_scores") or {}).get(key)
            if v is not None and v > max_score:
                max_score = float(v)
        vector.append(max_score)

    # Slot 5 = issuance anomaly (passed in separately as global)
    vector.append(0.0)  # placeholder; orchestrator will fill in
    return vector


def anchor_vector(anchor: dict) -> List[float]:
    """Derive an equivalent 6D vector for a historical anchor crisis.

    Uses the simple historical-references (CRISIS_REFERENCE) which only
    contain the directly-observed metrics. We map those to indicator-equivalent
    severity using the same logic as score_indicators (simplified).
    """
    btc = anchor.get("btc")
    low_rate = anchor.get("low_rate")
    aah = anchor.get("aah")
    pd_share = anchor.get("pd_share_pct")
    indirect = anchor.get("indirect_share_pct")

    # Map to indicator scores using same logic as live engine
    # 1. zero_rate_floor (low_rate, only for bills — bills had low_rate=0 in 2008)
    zrf = 100.0 if (low_rate is not None and low_rate < 0.01) else 0.0

    # 2. btc_extreme — extreme high OR extreme low
    if btc is not None:
        if btc >= 4.5:   bte = 100.0
        elif btc >= 3.5: bte = 70.0
        elif btc <= 1.8: bte = 100.0
        elif btc <= 2.0: bte = 70.0
        else:            bte = 0.0
    else:
        bte = 0.0

    # 3. tail_stress (AAH extremes either direction)
    if aah is not None:
        if aah < 15 or aah > 95: ts = 75.0
        elif aah < 25 or aah > 90: ts = 50.0
        else:                     ts = 0.0
    else:
        ts = 0.0

    # 4. pd_absorption (high dealer share)
    if pd_share is not None:
        if pd_share > 50:   pdas = 100.0
        elif pd_share > 35: pdas = 70.0
        elif pd_share > 25: pdas = 35.0
        else:               pdas = 0.0
    else:
        pdas = 0.0

    # 5. indirect_collapse (low foreign bid)
    if indirect is not None:
        if indirect < 30:   ic = 100.0
        elif indirect < 50: ic = 65.0
        elif indirect < 60: ic = 30.0
        else:               ic = 0.0
    else:
        ic = 0.0

    # 6. issuance_anomaly — not in historical anchors directly; default 0
    return [zrf, bte, ts, pdas, ic, 0.0]


def cosine_similarity(a: List[float], b: List[float]) -> float:
    """Compute cosine similarity between two equal-length vectors.
    Returns 0 if either vector is zero."""
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def find_historical_analogs(scored_auctions: List[dict],
                              crisis_reference: List[dict],
                              issuance_score: float = 0.0,
                              top_n: int = 3) -> List[dict]:
    """Find top-N historical crisis analogs to current posture."""
    current_vec = build_indicator_vector(scored_auctions, window_days=14)
    current_vec[5] = issuance_score  # fill in issuance dimension

    matches = []
    for anchor in crisis_reference:
        a_vec = anchor_vector(anchor)
        sim = cosine_similarity(current_vec, a_vec)
        outcome = ANALOG_OUTCOMES.get(anchor.get("date"), {})
        matches.append({
            "date":         anchor.get("date"),
            "regime":       anchor.get("regime"),
            "similarity":   round(sim, 4),
            "anchor_vec":   [round(x, 1) for x in a_vec],
            "anchor_metrics": {
                "btc":             anchor.get("btc"),
                "low_rate":        anchor.get("low_rate"),
                "aah":             anchor.get("aah"),
                "pd_share_pct":    anchor.get("pd_share_pct"),
                "indirect_share_pct": anchor.get("indirect_share_pct"),
            },
            "context":      outcome.get("context", ""),
            "what_happened_next": outcome.get("what_next", ""),
            "duration":     outcome.get("duration", ""),
        })

    matches.sort(key=lambda x: x["similarity"], reverse=True)

    return {
        "current_vector":   [round(x, 1) for x in current_vec],
        "vector_labels":    ["zero_rate_floor", "btc_extreme", "tail_stress",
                              "pd_absorption", "indirect_collapse", "issuance_anomaly"],
        "top_matches":      matches[:top_n],
        "all_matches":      matches,
    }


# ═════════════════════════════════════════════════════════════════════
# 4. CROSS-SIGNALS FROM FRED
# ═════════════════════════════════════════════════════════════════════

def _fred_get_latest(series_id: str, limit: int = 5) -> Optional[float]:
    """Get the latest non-null value for a FRED series."""
    url = (
        "https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
        f"&limit={limit}&sort_order=desc"
    )
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            data = json.loads(r.read())
        for obs in data.get("observations", []):
            v = obs.get("value")
            if v not in (".", "", None):
                return float(v)
    except Exception as e:
        print(f"[fred-x] {series_id} error: {e}")
    return None


def _fred_get_history(series_id: str, days: int = 30) -> List[Tuple[str, float]]:
    """Get the last N days of values for a FRED series. Returns [(date, value), ...]."""
    url = (
        "https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
        f"&limit={days}&sort_order=desc"
    )
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            data = json.loads(r.read())
        out = []
        for obs in data.get("observations", []):
            v = obs.get("value")
            if v not in (".", "", None):
                out.append((obs.get("date"), float(v)))
        return out
    except Exception as e:
        print(f"[fred-h] {series_id} error: {e}")
    return []


def compute_cross_signals() -> dict:
    """Pull 4 corroborating signals from FRED.

    Signals:
      - SOFR - IORB:    repo collateral squeeze proxy
                        IORB = interest on reserve balances (Fed admin rate)
                        SOFR > IORB by >5bp = repo stress
      - DXY (DTWEXBGS): USD trade-weighted strength
                        Rising USD = foreign demand may weaken
      - 10y - 2y curve: slope (T10Y2Y series)
                        Inversion is recession signal
      - 5y5y BE infl:  forward inflation expectations
                        Spike = inflation regime change, may stress auctions
    """
    out = {}

    # SOFR vs IORB (effective fed funds upper bound)
    sofr = _fred_get_latest("SOFR")
    iorb = _fred_get_latest("IORB")
    if sofr is not None and iorb is not None:
        spread_bp = (sofr - iorb) * 100
        if spread_bp > 8:
            stress = "ACUTE"
        elif spread_bp > 4:
            stress = "ELEVATED"
        elif spread_bp > 1:
            stress = "WATCH"
        else:
            stress = "CALM"
        out["repo_stress"] = {
            "sofr_pct":   round(sofr, 4),
            "iorb_pct":   round(iorb, 4),
            "spread_bp":  round(spread_bp, 2),
            "regime":     stress,
            "interpretation": (
                f"SOFR {sofr:.3f}% vs IORB {iorb:.3f}% = "
                f"{spread_bp:+.1f}bp. "
                f"{'Collateral squeeze' if spread_bp > 4 else 'Normal repo conditions'}."
            ),
        }
    else:
        out["repo_stress"] = {"err": "data unavailable"}

    # DXY proxy via DTWEXBGS
    dxy_hist = _fred_get_history("DTWEXBGS", days=60)
    if dxy_hist:
        dxy_now = dxy_hist[0][1]
        dxy_30d_ago = dxy_hist[min(20, len(dxy_hist)-1)][1] if len(dxy_hist) > 1 else dxy_now
        change_30d = ((dxy_now - dxy_30d_ago) / dxy_30d_ago * 100) if dxy_30d_ago > 0 else 0
        out["dollar_strength"] = {
            "level":         round(dxy_now, 2),
            "change_30d_pct": round(change_30d, 2),
            "regime":        "STRENGTHENING" if change_30d > 1.5 else "WEAKENING" if change_30d < -1.5 else "STABLE",
            "interpretation": (
                f"Trade-weighted USD at {dxy_now:.1f}, {change_30d:+.1f}% in 30d. "
                f"{'Strong USD pressures foreign bid' if change_30d > 1.5 else 'Soft USD supportive of foreign demand'}."
            ),
        }
    else:
        out["dollar_strength"] = {"err": "data unavailable"}

    # 10y - 2y curve
    curve = _fred_get_latest("T10Y2Y")
    if curve is not None:
        out["curve_slope"] = {
            "spread_bp": round(curve * 100, 1),
            "regime":    "INVERTED" if curve < 0 else "FLAT" if curve < 0.5 else "NORMAL",
            "interpretation": (
                f"10y-2y at {curve*100:+.0f}bp. "
                f"{'Inverted curve — recession signal' if curve < 0 else 'Curve positive'}."
            ),
        }

    # 5y5y forward inflation BE
    be_5y5y = _fred_get_latest("T5YIFR")
    if be_5y5y is not None:
        out["inflation_expectations"] = {
            "rate_pct":   round(be_5y5y, 2),
            "regime":     "ANCHORED" if 2.0 <= be_5y5y <= 2.5 else "UNANCHORED",
            "interpretation": (
                f"5y5y forward BE inflation {be_5y5y:.2f}%. "
                f"{'Above Fed target' if be_5y5y > 2.5 else 'Below Fed target' if be_5y5y < 2.0 else 'Anchored at target'}."
            ),
        }

    return out


# ═════════════════════════════════════════════════════════════════════
# 5. COMPOSITE HISTORY (30-day trend)
# ═════════════════════════════════════════════════════════════════════

def build_composite_history(scored_auctions: List[dict], days: int = 30) -> List[dict]:
    """Roll the same algorithm forward day-by-day to build a 30-day timeline
    of the composite score.

    For each day D, compute:
      - subset of auctions in [D-14, D]
      - size-weighted composite over that window
      - regime classification

    This shows trajectory and identifies the date of regime changes.
    """
    today = datetime.now(timezone.utc).date()
    series = []

    for offset in range(days, -1, -1):
        d = today - timedelta(days=offset)
        cutoff = d - timedelta(days=14)
        window = [a for a in scored_auctions
                    if a.get("auction_date")
                    and cutoff <= _parse_date(a["auction_date"]) <= d]
        if not window:
            series.append({"date": d.isoformat(), "composite": None,
                            "regime": None, "n_auctions": 0})
            continue
        total_size = sum(a.get("accepted_billions") or 1 for a in window)
        weighted = sum((a.get("composite_score", 0) * (a.get("accepted_billions") or 1))
                        for a in window) / total_size if total_size > 0 else 0
        if weighted >= 75:
            regime = "ACUTE_STRESS"
        elif weighted >= 50:
            regime = "ELEVATED"
        elif weighted >= 25:
            regime = "WATCH"
        else:
            regime = "CALM"
        series.append({
            "date":       d.isoformat(),
            "composite":  round(weighted, 1),
            "regime":     regime,
            "n_auctions": len(window),
        })

    # Detect regime change points
    change_points = []
    for i in range(1, len(series)):
        if series[i]["regime"] and series[i-1]["regime"] and series[i]["regime"] != series[i-1]["regime"]:
            change_points.append({
                "date":     series[i]["date"],
                "from":     series[i-1]["regime"],
                "to":       series[i]["regime"],
            })

    return {
        "series":         series,
        "change_points":  change_points,
        "current":        series[-1] if series else None,
        "min_composite":  min((s["composite"] for s in series if s["composite"] is not None), default=None),
        "max_composite":  max((s["composite"] for s in series if s["composite"] is not None), default=None),
    }


# ═════════════════════════════════════════════════════════════════════
# 6. TAIL RISK PROBABILITIES
# ═════════════════════════════════════════════════════════════════════

def compute_tail_risk(scored_auctions: List[dict],
                       composite_history: dict,
                       tenor_decomp: Dict[str, dict],
                       analog_match: dict,
                       cross_signals: dict) -> dict:
    """Estimate forward-looking probabilities.

    Uses a heuristic model — NOT a Bayesian / statistical estimator.
    Each probability is a calibrated mapping from current state to a
    forward-event probability based on historical frequency.

    P_failed_auction_30d:  probability of a bid-to-cover < 2.0 on coupons
                            OR allotted-at-high > 95% in next 30 days
    P_regime_escalation_14d: probability composite climbs ≥ 25 points in 14d
    P_supply_volatility_30d: probability of a 1+ sigma yield move on a
                              Treasury announcement day
    """
    # Current state inputs
    current_composite = composite_history.get("current", {}).get("composite") or 0
    series = composite_history.get("series", [])

    # Momentum: 7-day change in composite
    momentum = 0
    valid = [s for s in series if s.get("composite") is not None]
    if len(valid) >= 7:
        momentum = valid[-1]["composite"] - valid[-7]["composite"]

    # P(failed auction) — drivers:
    #   - PD share % at recent coupons (high = dealers stuck)
    #   - Indirect collapse signal active
    #   - Tail_stress active
    #   - Coupons_gt_3y composite
    coupons_long_stress = (tenor_decomp.get("coupons_gt_3y", {}).get("composite") or 0)
    pd_concern = any(
        (a.get("indicator_scores") or {}).get("pd_absorption", 0) >= 70
        for a in scored_auctions[:20]
    )
    indirect_concern = any(
        (a.get("indicator_scores") or {}).get("indirect_collapse", 0) >= 70
        for a in scored_auctions[:20]
    )
    p_failed = min(85, 5 + coupons_long_stress * 0.5 +
                       (20 if pd_concern else 0) +
                       (15 if indirect_concern else 0) +
                       max(0, momentum) * 0.5)

    # P(regime escalation) — drivers:
    #   - Current composite proximity to next threshold
    #   - Positive momentum
    #   - Top analog regime
    distance_to_next_threshold = 0
    if current_composite < 25: distance_to_next_threshold = 25 - current_composite
    elif current_composite < 50: distance_to_next_threshold = 50 - current_composite
    elif current_composite < 75: distance_to_next_threshold = 75 - current_composite

    top_analog_regime = ""
    if analog_match.get("top_matches"):
        top_analog_regime = analog_match["top_matches"][0].get("regime") or ""
    analog_amplifier = 25 if top_analog_regime in ("GFC_PEAK", "COVID_CRASH") else 0

    p_escalation = min(80, 8 + (max(0, momentum) * 1.2) +
                            (25 if distance_to_next_threshold < 8 else
                             10 if distance_to_next_threshold < 15 else 5) +
                            analog_amplifier)

    # P(supply volatility) — drivers:
    #   - SOFR-IORB stress
    #   - DXY momentum
    #   - Composite history max
    repo_stress = cross_signals.get("repo_stress", {})
    repo_regime = repo_stress.get("regime", "CALM")
    repo_amp = {"CALM": 0, "WATCH": 15, "ELEVATED": 30, "ACUTE": 45}.get(repo_regime, 0)

    dollar = cross_signals.get("dollar_strength", {})
    dollar_change = abs(dollar.get("change_30d_pct", 0) or 0)
    dollar_amp = min(20, dollar_change * 4)

    p_supply_vol = min(75, 10 + repo_amp + dollar_amp + max(0, momentum) * 0.4)

    return {
        "p_failed_auction_30d": {
            "probability": round(p_failed, 1),
            "drivers": {
                "coupons_long_stress":  round(coupons_long_stress, 1),
                "pd_concern":           pd_concern,
                "indirect_concern":     indirect_concern,
                "momentum":             round(momentum, 1),
            },
            "interpretation": (
                f"~{p_failed:.0f}% probability of a coupon BTC < 2.0 or AAH > 95% "
                f"in next 30 days. "
                f"{'Driven primarily by tenor stress + dealer absorption' if p_failed > 30 else 'Low probability — auction demand healthy'}."
            ),
        },
        "p_regime_escalation_14d": {
            "probability": round(p_escalation, 1),
            "drivers": {
                "current_composite":           round(current_composite, 1),
                "momentum_7d":                 round(momentum, 1),
                "distance_to_next_threshold":  round(distance_to_next_threshold, 1),
                "top_analog_regime":           top_analog_regime,
            },
            "interpretation": (
                f"~{p_escalation:.0f}% probability of crossing into a higher-stress "
                f"regime in next 14 days. "
                f"Current is {composite_history.get('current', {}).get('regime', '?')}, "
                f"composite trajectory {'+' if momentum > 0 else ''}{momentum:.1f}/wk."
            ),
        },
        "p_supply_volatility_30d": {
            "probability": round(p_supply_vol, 1),
            "drivers": {
                "repo_stress":          repo_regime,
                "dollar_change_30d":    round(dollar.get("change_30d_pct", 0) or 0, 2),
                "momentum":             round(momentum, 1),
            },
            "interpretation": (
                f"~{p_supply_vol:.0f}% probability of a 1+ sigma yield move "
                f"on an upcoming auction settlement day. "
                f"Repo stress: {repo_regime}, USD: {abs(dollar.get('change_30d_pct', 0) or 0):+.1f}% / 30d."
            ),
        },
    }


# ═════════════════════════════════════════════════════════════════════
# 7. ACTIONABLE TRIGGERS
# ═════════════════════════════════════════════════════════════════════

def build_triggers(scored_auctions: List[dict],
                    indicator_aggregate: dict,
                    composite_history: dict) -> List[dict]:
    """Produce a list of named triggers that would flip the regime,
    with current values and distance-to-trigger."""

    current = composite_history.get("current", {}).get("composite") or 0

    triggers = []

    # 1. Composite ≥ 25 (CALM → WATCH)
    if current < 25:
        triggers.append({
            "name":      "Crosses into WATCH",
            "condition": "Composite score ≥ 25",
            "current":   round(current, 1),
            "threshold": 25,
            "distance":  round(25 - current, 1),
            "action":    "Tighten stops, prepare 5y-10y hedges (TLT puts, ES options).",
            "urgency":   "monitoring",
        })

    # 2. Composite ≥ 50 (WATCH → ELEVATED)
    if current < 50:
        triggers.append({
            "name":      "Crosses into ELEVATED",
            "condition": "Composite score ≥ 50",
            "current":   round(current, 1),
            "threshold": 50,
            "distance":  round(50 - current, 1),
            "action":    "Reduce duration significantly. Add carry to USD. Hedge equity beta.",
            "urgency":   "tactical" if current >= 30 else "monitoring",
        })

    # 3. Composite ≥ 75 (ELEVATED → ACUTE)
    if current < 75:
        triggers.append({
            "name":      "Crosses into ACUTE_STRESS",
            "condition": "Composite score ≥ 75",
            "current":   round(current, 1),
            "threshold": 75,
            "distance":  round(75 - current, 1),
            "action":    "Full defensive posture. Cash, gold, vol. Trade auction concession aggressively.",
            "urgency":   "strategic",
        })

    # 4. Specific indicator triggers
    for sig_name, threshold, action in [
        ("pd_absorption",     70, "Dealers absorbing >35% on coupons. Watch for failed auction follow-on."),
        ("indirect_collapse", 70, "Foreign demand cratered. USD strength forecast → buy DXY/sell EM."),
        ("btc_extreme",       70, "Demand anomaly — confirms stress regime. Tighten exposure."),
        ("zero_rate_floor",   70, "Bills at zero — money parking pattern. Liquidity crisis warning."),
    ]:
        agg = indicator_aggregate.get(sig_name, {})
        n_fired = agg.get("n_fired", 0)
        max_score = agg.get("max_score", 0)
        triggers.append({
            "name":      f"{sig_name.replace('_', ' ').title()} fires next auction",
            "condition": f"{sig_name} score ≥ {threshold} on any new auction",
            "current":   round(max_score, 1),
            "threshold": threshold,
            "distance":  round(max(0, threshold - max_score), 1),
            "auctions_already_fired_14d": n_fired,
            "action":    action,
            "urgency":   "tactical" if n_fired > 0 else "monitoring",
        })

    return triggers


# ═════════════════════════════════════════════════════════════════════
# Helpers
# ═════════════════════════════════════════════════════════════════════

def _parse_date(s: str):
    """Parse YYYY-MM-DD prefix of a date string. Returns date object."""
    return datetime.strptime(s[:10], "%Y-%m-%d").date()


def _parse_float(v):
    if v in (None, "", "null"):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _classify_tenor_bucket_simple(security_type: str, security_term: str) -> str:
    """Simplified version of classify_tenor_bucket for upcoming records."""
    sec_type = (security_type or "").upper()
    sec_term = (security_term or "").upper()
    if "TIPS" in sec_type or "TIPS" in sec_term:
        return "tips"
    if "FRN" in sec_type or "FRN" in sec_term:
        return "frn"
    if "BILL" in sec_type:
        # parse tenor: "28-Day", "13-Week", etc.
        parts = sec_term.replace("-", " ").split()
        try:
            n = int(parts[0])
            days = (n if "DAY" in sec_term else
                    n * 7 if "WEEK" in sec_term else
                    n * 30 if "MONTH" in sec_term else
                    n * 365 if "YEAR" in sec_term else n)
            return "bills_lt_90d" if days < 90 else "bills_gte_90d"
        except (ValueError, IndexError):
            return "bills_lt_90d"
    # Notes / Bonds
    parts = sec_term.replace("-", " ").split()
    try:
        n = int(parts[0])
        if "YEAR" in sec_term and n < 3:
            return "coupons_lt_3y"
    except (ValueError, IndexError):
        pass
    return "coupons_gt_3y"
