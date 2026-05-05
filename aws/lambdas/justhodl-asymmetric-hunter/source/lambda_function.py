"""
justhodl-asymmetric-hunter  (Layer 4 of nobrainer hunter)
=========================================================
The fusion engine. Reads outputs from Layers 1, 2, 3 and computes a unified
5-factor "asymmetric opportunity" score for every (ticker, theme) candidate.

The 5 factors (the "no-brainer DNA"):

  1. theme_attribution_score (20%)
       Is the underlying theme actually alive? Use Layer 1 phase + phase_score.
       EXTENDED/ACCELERATING = the theme is moving.

  2. primary_leg_inflated_score (15%)
       Are the obvious tier-1 picks already over-paid?
       Use Layer 3 theme median P/S vs SOX/SPX baseline. High = inflated.
       This signals "market believes, money has rotated in, look at the unloved
       second-order names."

  3. supply_inflection_score (30% — heaviest weight)
       Is there hard supply tightness in the inputs feeding this theme?
       Use Layer 2 by_theme[etf].composite_inflection_score.
       This is the part the market structurally misses (DRAM tightness for
       MU, lithium for LIT, HBM for memory).

  4. valuation_asymmetry_score (25%)
       Is THIS specific ticker cheap vs theme median?
       Use Layer 3 per-ticker asymmetry_score.

  5. catalyst_proximity_score (10%)
       Is there an event in 30-90 days that forces the market to look?
       Pull next earnings date from FMP earnings calendar.

Final score multiplied by crowdedness_multiplier:
  • Tier-1: 0.7×  (penalty — already crowded by definition)
  • Tier-2: 1.0×  (sweet spot)
  • Tier-3: 1.1×  (deepest asymmetry)

Plus PHASE multiplier:
  • EXTENDED:     1.10× (the user's preferred hunt ground)
  • ACCELERATING: 1.05×
  • EMERGING:     1.00×
  • PEAKING:      0.85× (theme already topping)
  • COOLING:      0.50× (stay away)
  • DYING:        0.20× (do not touch)

Schedule: cron(30 13 * * ? *) — daily 13:30 UTC after Layer 1 (06:00), Layer 2
                                (07:00), Layer 3 (08:00).

Inputs:
  s3://justhodl-dashboard-live/data/themes-detected.json     (Layer 1)
  s3://justhodl-dashboard-live/data/supply-inflection.json   (Layer 2)
  s3://justhodl-dashboard-live/data/theme-tiers.json         (Layer 3)
  FMP earnings calendar API                                  (catalyst data)

Output:
  s3://justhodl-dashboard-live/data/nobrainers.json
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FMP_BASE = "https://financialmodelingprep.com/stable"

S3 = boto3.client("s3", region_name=REGION)

# ─────────────────────────────────────────────────────────────────────────────
# WEIGHTS (the 5-factor scorecard)
# ─────────────────────────────────────────────────────────────────────────────
W_THEME_ATTRIBUTION = 0.20
W_PRIMARY_INFLATED  = 0.15
W_SUPPLY_INFLECTION = 0.30
W_VALUATION_ASYM    = 0.25
W_CATALYST_PROX     = 0.10

# Crowdedness multipliers
TIER_MULTIPLIER = {1: 0.7, 2: 1.0, 3: 1.1}

# Phase multipliers
PHASE_MULTIPLIER = {
    "EXTENDED":     1.10,
    "ACCELERATING": 1.05,
    "EMERGING":     1.00,
    "PEAKING":      0.85,
    "COOLING":      0.50,
    "DYING":        0.20,
    "DORMANT":      0.50,
}

# Theme median P/S baseline for "primary leg inflated" detection
# A theme with median P/S > 8 is officially expensive, > 12 is bubble territory.
PS_INFLATION_BANDS = [
    (4.0,  20.0),   # cheap
    (8.0,  50.0),   # fair
    (12.0, 75.0),   # expensive
    (20.0, 90.0),   # bubbly
    (999,  100.0),  # absurd
]

# Catalyst horizons
CATALYST_DAYS_HOT = 14    # earnings within 14d = max score
CATALYST_DAYS_WARM = 45   # within 45d = good
CATALYST_DAYS_COOL = 90   # within 90d = okay

# Keep top-N candidates in final output
TOP_N_FINAL = 25
TOP_N_TIER2 = 15
TOP_N_TIER3 = 10
LEADERBOARD_MIN_SCORE = 55

# Concurrency for FMP earnings calls
MAX_WORKERS = 6

_EARNINGS_CACHE = {}
_CACHE_LOCK = Lock()


# ─────────────────────────────────────────────────────────────────────────────
# INPUT LOADERS
# ─────────────────────────────────────────────────────────────────────────────
def load_layer(key):
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[hunter] FAIL load {key}: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# FMP earnings calendar (for catalyst proximity)
# ─────────────────────────────────────────────────────────────────────────────
def fetch_earnings_for(ticker):
    """Get next earnings date for ticker. Returns ISO date string or None."""
    with _CACHE_LOCK:
        if ticker in _EARNINGS_CACHE:
            return _EARNINGS_CACHE[ticker]

    url = f"{FMP_BASE}/earnings?symbol={ticker}&limit=4&apikey={FMP_KEY}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-asymmetric-hunter/1.0"})
        with urllib.request.urlopen(req, timeout=12) as r:
            if r.status != 200:
                with _CACHE_LOCK:
                    _EARNINGS_CACHE[ticker] = None
                return None
            data = json.loads(r.read().decode("utf-8"))
            # Find next earnings (date >= today)
            today = datetime.now(timezone.utc).date()
            future_dates = []
            for row in (data if isinstance(data, list) else []):
                ds = row.get("date")
                if not ds:
                    continue
                try:
                    d = datetime.strptime(ds, "%Y-%m-%d").date()
                    if d >= today:
                        future_dates.append(d)
                except Exception:
                    continue
            future_dates.sort()
            next_d = future_dates[0].isoformat() if future_dates else None
            with _CACHE_LOCK:
                _EARNINGS_CACHE[ticker] = next_d
            return next_d
    except Exception as e:
        # Fail soft — catalyst score will default to neutral
        with _CACHE_LOCK:
            _EARNINGS_CACHE[ticker] = None
        return None


# ─────────────────────────────────────────────────────────────────────────────
# 5-FACTOR SCORERS
# ─────────────────────────────────────────────────────────────────────────────
def score_theme_attribution(theme):
    """Layer 1: phase + phase_score → 0-100 score."""
    phase = theme.get("phase", "DORMANT")
    phase_score = theme.get("phase_score", 0)
    base = {
        "EXTENDED":     85,
        "ACCELERATING": 80,
        "EMERGING":     65,
        "PEAKING":      55,
        "COOLING":      30,
        "DYING":        10,
        "DORMANT":      35,
    }.get(phase, 30)
    # Adjust by phase_score (0-100 within phase confidence)
    return round(base * 0.7 + phase_score * 0.3, 1)


def score_primary_inflated(theme_stats):
    """Layer 3 theme stats: median P/S → 0-100 inflation score.
    High = primary leg expensive, money will rotate to tier-2/3."""
    if not theme_stats:
        return 50.0
    median_ps = theme_stats.get("median_p_s") or theme_stats.get("p_s_median")
    if median_ps is None:
        return 50.0
    for cap, score in PS_INFLATION_BANDS:
        if median_ps <= cap:
            return float(score)
    return 100.0


def score_supply_inflection(theme_etf, supply_data):
    """Layer 2 by_theme[etf].composite_inflection_score (already 0-100)."""
    if not supply_data:
        return 0.0
    by_theme = (supply_data.get("by_theme") or {})
    rec = by_theme.get(theme_etf)
    if not rec:
        return 0.0
    return float(rec.get("composite_inflection_score", 0.0))


def score_valuation_asymmetry(ticker_data):
    """Layer 3 ticker.asymmetry_score (already 0-100)."""
    if not ticker_data:
        return 50.0
    return float(ticker_data.get("asymmetry_score", 50.0))


def score_catalyst_proximity(next_earnings_iso):
    """Days until next earnings → 0-100 score.

    <14d:  100 (immediate catalyst)
    14-45: 75
    45-90: 50
    >90:   30
    None:  40 (no data, neutral-low)
    """
    if not next_earnings_iso:
        return 40.0
    try:
        d = datetime.strptime(next_earnings_iso, "%Y-%m-%d").date()
        days = (d - datetime.now(timezone.utc).date()).days
    except Exception:
        return 40.0
    if days <= CATALYST_DAYS_HOT:
        return 100.0
    if days <= CATALYST_DAYS_WARM:
        return 75.0
    if days <= CATALYST_DAYS_COOL:
        return 50.0
    return 30.0


# ─────────────────────────────────────────────────────────────────────────────
# CANDIDATE BUILDER
# ─────────────────────────────────────────────────────────────────────────────
def build_candidates(themes_data, supply_data, tiers_data):
    """Yield (ticker, theme_etf, all_data) tuples for every classified ticker."""
    if not tiers_data:
        return

    # Build theme phase lookup from Layer 1
    theme_phase = {}
    for t in (themes_data.get("themes", []) if themes_data else []):
        theme_phase[t["etf"]] = t

    for etf, theme_block in (tiers_data.get("themes") or {}).items():
        if not theme_block:
            continue
        phase = theme_block.get("phase", "DORMANT")
        # Skip theme phases we don't want to hunt in
        if phase in ("DYING", "COOLING"):
            continue

        theme_stats = theme_block.get("theme_stats") or {}
        layer1_theme = theme_phase.get(etf, {})

        for tk, td in (theme_block.get("tickers") or {}).items():
            yield {
                "ticker": tk,
                "theme_etf": etf,
                "theme_name": theme_block.get("name"),
                "theme_phase": phase,
                "tier": td.get("tier", 2),
                "ticker_data": td,
                "theme_stats": theme_stats,
                "layer1_theme": layer1_theme,
            }


def score_candidate(cand, supply_data, fetch_earnings=True):
    """Compute the 5-factor composite score for a candidate."""
    f1 = score_theme_attribution(cand["layer1_theme"])
    f2 = score_primary_inflated(cand["theme_stats"])
    f3 = score_supply_inflection(cand["theme_etf"], supply_data)
    f4 = score_valuation_asymmetry(cand["ticker_data"])

    next_earnings = None
    if fetch_earnings:
        next_earnings = fetch_earnings_for(cand["ticker"])
    f5 = score_catalyst_proximity(next_earnings)

    raw = (
        W_THEME_ATTRIBUTION * f1
        + W_PRIMARY_INFLATED * f2
        + W_SUPPLY_INFLECTION * f3
        + W_VALUATION_ASYM * f4
        + W_CATALYST_PROX * f5
    )

    tier_mult = TIER_MULTIPLIER.get(cand["tier"], 1.0)
    phase_mult = PHASE_MULTIPLIER.get(cand["theme_phase"], 0.5)

    final = raw * tier_mult * phase_mult
    final = round(max(0.0, min(100.0, final)), 1)

    factors = {
        "theme_attribution": round(f1, 1),
        "primary_inflated":  round(f2, 1),
        "supply_inflection": round(f3, 1),
        "valuation_asym":    round(f4, 1),
        "catalyst_prox":     round(f5, 1),
        "tier_multiplier":   tier_mult,
        "phase_multiplier":  phase_mult,
        "raw_pre_mult":      round(raw, 1),
    }

    if final >= 80:
        flag = "TIER_A_NOBRAINER"
    elif final >= 70:
        flag = "TIER_B_HIGH_CONVICTION"
    elif final >= 60:
        flag = "TIER_C_WATCHLIST"
    elif final >= 50:
        flag = "TIER_D_MONITOR"
    else:
        flag = "PASS"

    return {
        "ticker": cand["ticker"],
        "name": cand["ticker_data"].get("name"),
        "theme_etf": cand["theme_etf"],
        "theme_name": cand["theme_name"],
        "theme_phase": cand["theme_phase"],
        "tier": cand["tier"],
        "asymmetric_score": final,
        "flag": flag,
        "factors": factors,
        "fundamentals": cand["ticker_data"].get("fundamentals", {}),
        "valuation_components": cand["ticker_data"].get("asymmetry_components", {}),
        "next_earnings": next_earnings,
        "supply_signals": _supply_detail(cand["theme_etf"], supply_data),
    }


def _supply_detail(theme_etf, supply_data):
    if not supply_data:
        return []
    by_theme = (supply_data.get("by_theme") or {})
    rec = by_theme.get(theme_etf)
    if not rec:
        return []
    # Top 3 driving signals for this theme
    sigs = rec.get("signals", [])
    return [
        {
            "signal": s.get("signal"),
            "score": s.get("score"),
            "flag": s.get("flag"),
            "description": s.get("description"),
        }
        for s in sigs[:3]
    ]


# ─────────────────────────────────────────────────────────────────────────────
# HANDLER
# ─────────────────────────────────────────────────────────────────────────────
def lambda_handler(event=None, context=None):
    started = time.time()
    print("[hunter] Layer 4 — asymmetric-hunter starting")

    # 1. Load all 3 input layers
    themes_data = load_layer("data/themes-detected.json")
    supply_data = load_layer("data/supply-inflection.json")
    tiers_data  = load_layer("data/theme-tiers.json")

    layers_loaded = {
        "themes_detected":    bool(themes_data),
        "supply_inflection":  bool(supply_data),
        "theme_tiers":        bool(tiers_data),
    }
    print(f"[hunter] inputs loaded: {layers_loaded}")
    if not tiers_data:
        msg = "FATAL — theme-tiers.json missing, cannot score candidates"
        print(f"[hunter] {msg}")
        return {"statusCode": 500, "body": json.dumps({"error": msg})}

    # 2. Build candidate universe (every classified ticker × theme)
    candidates = list(build_candidates(themes_data, supply_data, tiers_data))
    print(f"[hunter] candidates: {len(candidates)}")

    # 3. Pre-fetch earnings concurrently to save Lambda time
    unique_tickers = list({c["ticker"] for c in candidates})
    print(f"[hunter] fetching earnings for {len(unique_tickers)} unique tickers (max_workers={MAX_WORKERS})")
    earn_started = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        list(ex.map(fetch_earnings_for, unique_tickers))
    print(f"[hunter] earnings fetched in {round(time.time() - earn_started, 1)}s "
          f"(cache size: {len(_EARNINGS_CACHE)})")

    # 4. Score each candidate (earnings already cached so fetch_earnings=True is fast)
    scored = []
    for c in candidates:
        scored.append(score_candidate(c, supply_data, fetch_earnings=True))

    # 5. Sort and partition
    scored.sort(key=lambda x: -x["asymmetric_score"])

    leaderboard = [x for x in scored if x["asymmetric_score"] >= LEADERBOARD_MIN_SCORE]
    tier2 = [x for x in scored if x["tier"] == 2 and x["asymmetric_score"] >= LEADERBOARD_MIN_SCORE]
    tier3 = [x for x in scored if x["tier"] == 3 and x["asymmetric_score"] >= LEADERBOARD_MIN_SCORE]

    # MU-grade specifically — high score AND mcap_to_rev <= 3 AND tier 2 or 3
    mu_grade = [
        x for x in leaderboard
        if x.get("fundamentals", {}).get("mcap_to_rev") is not None
        and x["fundamentals"]["mcap_to_rev"] <= 3.0
        and x["tier"] in (2, 3)
    ]

    # 6. Compose output
    n_tier_a = sum(1 for x in scored if x["asymmetric_score"] >= 80)
    n_tier_b = sum(1 for x in scored if 70 <= x["asymmetric_score"] < 80)
    n_tier_c = sum(1 for x in scored if 60 <= x["asymmetric_score"] < 70)

    output = {
        "schema_version": "1.0",
        "method": "asymmetric_hunter_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - started, 1),
        "layers_loaded": layers_loaded,
        "n_candidates_scored": len(scored),
        "n_unique_tickers": len(unique_tickers),
        "summary": {
            "n_tier_a_nobrainer": n_tier_a,
            "n_tier_b_high_conviction": n_tier_b,
            "n_tier_c_watchlist": n_tier_c,
            "n_mu_grade": len(mu_grade),
            "top_25_overall": leaderboard[:TOP_N_FINAL],
            "top_15_tier2": tier2[:TOP_N_TIER2],
            "top_10_tier3": tier3[:TOP_N_TIER3],
            "mu_grade_top_15": mu_grade[:15],
        },
        "all_scored": scored,
        "schema": {
            "description": (
                "Layer 4 of nobrainer hunter pipeline. Fuses Layer 1 (themes), "
                "Layer 2 (supply inflection), Layer 3 (tier classification + "
                "valuation) into a unified 5-factor asymmetric opportunity "
                "score, scaled by tier and phase multipliers."
            ),
            "scorecard_weights": {
                "theme_attribution": W_THEME_ATTRIBUTION,
                "primary_leg_inflated": W_PRIMARY_INFLATED,
                "supply_inflection": W_SUPPLY_INFLECTION,
                "valuation_asymmetry": W_VALUATION_ASYM,
                "catalyst_proximity": W_CATALYST_PROX,
            },
            "tier_multipliers": TIER_MULTIPLIER,
            "phase_multipliers": PHASE_MULTIPLIER,
            "flags": {
                "TIER_A_NOBRAINER": ">=80",
                "TIER_B_HIGH_CONVICTION": "70-79",
                "TIER_C_WATCHLIST": "60-69",
                "TIER_D_MONITOR": "50-59",
                "PASS": "<50",
            },
        },
    }

    body = json.dumps(output, default=str)
    S3.put_object(
        Bucket=BUCKET,
        Key="data/nobrainers.json",
        Body=body.encode("utf-8"),
        ContentType="application/json",
        CacheControl="max-age=60, public",
    )
    print(f"[hunter] wrote {len(body)}b to data/nobrainers.json")
    print(f"[hunter] tier_a={n_tier_a} tier_b={n_tier_b} tier_c={n_tier_c} mu_grade={len(mu_grade)}")
    if leaderboard:
        top5 = [(x["ticker"], x["theme_etf"], x["asymmetric_score"], x["flag"]) for x in leaderboard[:5]]
        print(f"[hunter] TOP 5: {top5}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_candidates_scored": len(scored),
            "n_tier_a_nobrainer": n_tier_a,
            "n_mu_grade": len(mu_grade),
            "duration_s": round(time.time() - started, 1),
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
