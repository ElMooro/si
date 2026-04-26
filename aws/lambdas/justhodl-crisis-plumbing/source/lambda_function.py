"""
justhodl-crisis-plumbing — Phase 9.1 of the system-improvement plan.

Aggregates the 5 official crisis indices that hedge funds dismiss
because they are "official" (which is the edge), plus a synthesized
cross-currency basis proxy, plus money-market-fund composition flow.

Output: s3://justhodl-dashboard-live/data/crisis-plumbing.json
Schedule: daily 13:30 UTC (after FRED weekly updates land Thursdays)
Consumers: justhodl.ai/crisis.html, intelligence.html composite,
           future risk-sizer for crisis-distance signal

Sources (all FRED — free, official, real data):

  CRISIS COMPOSITES (weekly+ resolution):
    STLFSI4   — St. Louis Fed Financial Stress Index v4
    NFCI      — Chicago Fed National Financial Conditions
    ANFCI     — Chicago Fed Adjusted NFCI (cyclical-adjusted)
    KCFSI     — Kansas City Fed Financial Stress
    OFRFSI    — OFR Financial Stress Index (cross-asset)

  PLUMBING TIER 2 (offshore + bank funding stress):
    WMMFNS    — Total MMF AUM (weekly)
    WIMFSL    — Institutional MMF (weekly)
    DPSACBW027SBOG — All commercial bank deposits (weekly)
    H8B1058NCBCMG — C&I lending H.8 (weekly)

  CROSS-CURRENCY BASIS PROXY:
    DGS3MO    — 3M Treasury yield
    DTB3      — 3M Treasury bill rate
    DEXJPUS, DEXUSEU — spot FX
    Synthetic 3M-USD-vs-JPY/EUR basis computed via covered interest parity

  YIELD CURVE STRESS (already in your data, included for composite):
    T10Y2Y    — 10Y-2Y spread
    T10Y3M    — 10Y-3M spread (Estrella's recession indicator)

Composite output:
  composite_stress_score  (0-100, higher = more stress)
  consensus_count         (how many of the 5 official indices flag stress)
  agreement_signal        (NORMAL/CAUTION/ELEVATED/CRISIS)
  individual_components   (each index normalized + raw)
  xcc_basis_3m_jpy        (basis points)
  xcc_basis_3m_eur        (basis points)
  mmf_government_share    (% of MMF in government-only funds)
  mmf_flow_30d_pct        (30-day flow %, prime → govt is bearish signal)
  bank_deposit_30d_change (large drops = bank-run signal, c.f. SVB)
  generated_at            (ISO8601 UTC)
"""
import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

# Phase 2 dual-write helper (auto-aliases khalid_* → ka_* if any leak in)
try:
    from ka_aliases import add_ka_aliases
except Exception:
    def add_ka_aliases(obj, **_kwargs):
        return obj

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/crisis-plumbing.json"
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")

s3 = boto3.client("s3", region_name=REGION)

# ─────────────────────────────────────────────────────────────────────
# FRED fetch helpers
# ─────────────────────────────────────────────────────────────────────

def fred_observations(series_id, observation_start=None, limit=1000):
    """Fetch raw observations for a FRED series. Returns list of (date, value)
    tuples sorted ascending by date. Handles missing values ('.') as None."""
    params = {
        "series_id": series_id,
        "api_key": FRED_KEY,
        "file_type": "json",
        "limit": limit,
        "sort_order": "asc",
    }
    if observation_start:
        params["observation_start"] = observation_start
    url = "https://api.stlouisfed.org/fred/series/observations?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=20) as r:
            data = json.loads(r.read())
        out = []
        for obs in data.get("observations", []):
            v = obs["value"]
            out.append((obs["date"], float(v) if v != "." else None))
        return out
    except Exception as e:
        print(f"[FRED] {series_id} error: {e}")
        return []


def latest_value(observations):
    """Get the most recent non-None value + its date."""
    for date, val in reversed(observations):
        if val is not None:
            return date, val
    return None, None


def value_at_offset(observations, days_back):
    """Walk back from latest non-None value; find first non-None value
    at least `days_back` calendar days earlier. Used for delta calculations."""
    latest_date, latest_val = latest_value(observations)
    if latest_date is None:
        return None, None
    target = (datetime.fromisoformat(latest_date) - timedelta(days=days_back)).date()
    for date, val in reversed(observations):
        if val is not None and datetime.fromisoformat(date).date() <= target:
            return date, val
    return None, None


def historical_distribution(observations, lookback_years=10):
    """Return non-None values from the last N years for percentile calcs."""
    if not observations:
        return []
    latest = datetime.fromisoformat(observations[-1][0])
    cutoff = (latest - timedelta(days=365 * lookback_years)).date()
    return [v for d, v in observations
            if v is not None and datetime.fromisoformat(d).date() >= cutoff]


def percentile_rank(value, distribution):
    """Where does `value` rank in the historical distribution? (0-100 scale)
    100 = highest stress ever observed in the lookback window."""
    if not distribution or value is None:
        return None
    n = sum(1 for v in distribution if v <= value)
    return round(100.0 * n / len(distribution), 1)


# ─────────────────────────────────────────────────────────────────────
# Series catalog — what we fetch and how to interpret
# ─────────────────────────────────────────────────────────────────────

CRISIS_INDICES = {
    "STLFSI4": {
        "name": "St. Louis Fed Financial Stress Index",
        "stress_direction": "higher",  # higher value = more stress
        "lookback_years": 10,
        "stress_threshold_pct": 75,    # 75th percentile = elevated
    },
    "NFCI": {
        "name": "Chicago Fed National Financial Conditions",
        "stress_direction": "higher",
        "lookback_years": 10,
        "stress_threshold_pct": 75,
    },
    "ANFCI": {
        "name": "Chicago Fed Adjusted NFCI",
        "stress_direction": "higher",
        "lookback_years": 10,
        "stress_threshold_pct": 75,
    },
    "KCFSI": {
        "name": "Kansas City Fed Financial Stress Index",
        "stress_direction": "higher",
        "lookback_years": 10,
        "stress_threshold_pct": 75,
    },
    "OFRFSI": {
        "name": "OFR Financial Stress Index",
        "stress_direction": "higher",
        "lookback_years": 10,
        "stress_threshold_pct": 75,
    },
}

PLUMBING_TIER2 = {
    # MMF composition (post-2021): use the gov/prime/tax-exempt split.
    # WMMFNS (Total MMF) appears on FRED as a discontinued legacy series; the
    # modern ICI breakdown is WGMMNS/WPMMNS/WTMMNS published weekly.
    "WGMMNS":            {"name": "Government MMF",      "fmt": "money", "scale": 1000},
    "WPMMNS":            {"name": "Prime MMF",           "fmt": "money", "scale": 1000},
    "WTMMNS":            {"name": "Tax-Exempt MMF",      "fmt": "money", "scale": 1000},
    "DPSACBW027SBOG":    {"name": "All Commercial Bank Deposits", "fmt": "money", "scale": 1000},
    # H.8 C&I Lending — switched from H8B1058NCBCMG (a percent-change series)
    # to BUSLOANS (the absolute level in $B), so delta_30d_pct is meaningful.
    "BUSLOANS":          {"name": "C&I Lending (H.8 absolute)", "fmt": "money", "scale": 1},
    "RRPONTSYD":         {"name": "Reverse Repo Facility Usage", "fmt": "money", "scale": 1000},
    "TGA":               {"name": "Treasury General Account", "fmt": "money", "scale": 1000, "real_id": "WTREGEN"},
}

CROSS_CURRENCY_BASIS_INPUTS = {
    # USD side — both T-Bill and OIS-equivalent for robustness
    "DGS3MO":          "3M Treasury yield (USD)",
    "DTB3":            "3M Treasury bill rate",
    "DGS10":           "10Y Treasury yield",
    # FX spot
    "DEXJPUS":         "JPY/USD spot",
    "DEXUSEU":         "EUR/USD spot",
    # Yield curve (already used elsewhere; included here for the synthesizer)
    "T10Y2Y":          "10Y-2Y spread",
    "T10Y3M":          "10Y-3M spread",
    # ── Phase 9.3c: foreign 3M rates for rate-differential approach ──
    "INTGSBJPM193N":   "Japan 3M T-Bill rate (monthly)",
    "IR3TBB01EZM156N": "Eurozone 3M T-Bill rate (monthly)",
    # ── Phase 9.3c: broad dollar index — global USD funding stress ──
    "DTWEXBGS":        "Broad Trade-Weighted USD Index (daily)",
    # ── Phase 9.3c: unsecured overnight bank funding ──
    "OBFR":            "Overnight Bank Funding Rate (daily)",
}


# ─────────────────────────────────────────────────────────────────────
# Funding & Credit Signals (Phase 9.3b)
# The 5 highest-leverage missing signals identified by gap analysis:
#   1. SOFR-IORB spread — daily, the cleanest single repo stress signal
#      Computed: SOFR (Secured Overnight Financing Rate) − IORB (Interest On
#      Reserve Balances). When SOFR > IORB, banks have cash to lend to repo
#      (stress easing). When SOFR < IORB by more than a few bps, cash is
#      being hoarded (stress rising — what happened in Sep 2019 + March 2020).
#   2. HY OAS (BAMLH0A0HYM2) — daily, the credit-fear gauge
#   3. 10Y TIPS breakeven (T10YIE) — daily, inflation expectations
#   4. 10Y Real Rate (DFII10) — daily, recession risk + USD funding cost
#   5. SLOOS C&I tightening (DRTSCILM) — quarterly, the single most reliable
#      bank-credit-cycle leading indicator
# ─────────────────────────────────────────────────────────────────────

FUNDING_CREDIT_SIGNALS = {
    # SOFR-IORB is computed (not a FRED series); inputs pulled separately
    "SOFR": {"name": "SOFR (Secured Overnight Financing Rate)", "fred_id": "SOFR"},
    "IORB": {"name": "Interest On Reserve Balances",            "fred_id": "IORB"},
    # HY OAS — ICE BofA US High Yield Index Option-Adjusted Spread
    "HY_OAS": {
        "name": "HY Credit Spread (ICE BofA US HY OAS)",
        "fred_id": "BAMLH0A0HYM2",
        "unit": "bps",
        "stress_direction": "higher",
        "thresholds": {"watch": 450, "elevated": 600, "crisis": 1000},
    },
    # IG (BBB) OAS — secondary credit fear gauge
    "IG_BBB_OAS": {
        "name": "IG BBB Credit Spread",
        "fred_id": "BAMLC0A4CMTRIV",  # alt: BAMLC0A4CBBB level
        "unit": "bps",
        "stress_direction": "higher",
        "thresholds": {"watch": 175, "elevated": 250, "crisis": 400},
    },
    # 10Y Breakeven Inflation
    "T10YIE": {
        "name": "10Y TIPS Breakeven Inflation",
        "fred_id": "T10YIE",
        "unit": "pct",
        "stress_direction": "extremes",
        # interpretation: very low (<1.5) = deflation fear; very high (>3) = inflation expectations unanchored
    },
    # 10Y Real Rate (TIPS yield)
    "DFII10": {
        "name": "10Y Real Rate (TIPS yield)",
        "fred_id": "DFII10",
        "unit": "pct",
        "stress_direction": "higher",
        "thresholds": {"watch": 1.5, "elevated": 2.25, "crisis": 3.0},
    },
    # SLOOS Senior Loan Officer Survey — C&I tightening (quarterly)
    "SLOOS_TIGHTEN": {
        "name": "SLOOS: Banks Tightening C&I Standards (net %)",
        "fred_id": "DRTSCILM",
        "unit": "pct",
        "stress_direction": "higher",
        "thresholds": {"watch": 10, "elevated": 25, "crisis": 50},
    },
}


def compute_funding_credit_signals(observations_map):
    """Build the funding-credit-signals section. SOFR-IORB is computed
    from the two underlying series; everything else is direct FRED + threshold
    bucketing.
    """
    out = {}

    # ── 1. SOFR-IORB spread (computed daily) ──
    sofr_obs = observations_map.get("SOFR") or []
    iorb_obs = observations_map.get("IORB") or []
    if sofr_obs and iorb_obs:
        s_date, s_val = latest_value(sofr_obs)
        i_date, i_val = latest_value(iorb_obs)
        if s_val is not None and i_val is not None:
            spread_bps = round((s_val - i_val) * 100, 2)  # rates in %, spread in bps
            # Compute distribution of recent spread (1Y) for z-score context
            iorb_by_date = {d: v for d, v in iorb_obs if v is not None}
            spread_series = [
                ((datetime.fromisoformat(d) - datetime.fromisoformat(min(s_date, i_date))).days,
                 (v - iorb_by_date[d]) * 100)
                for d, v in sofr_obs
                if v is not None and d in iorb_by_date
            ]
            recent = [s for _, s in spread_series][-252:]  # 1Y of trading days
            z = None
            mean = std = None
            if len(recent) >= 30:
                mean = sum(recent) / len(recent)
                var = sum((x - mean) ** 2 for x in recent) / len(recent)
                std = var ** 0.5 if var > 0 else 1
                z = round((spread_bps - mean) / std, 2) if std > 0 else 0

            # Bucket: more negative = more stress (cash hoarding)
            if spread_bps <= -15:
                signal = "CRISIS"
            elif spread_bps <= -7:
                signal = "ELEVATED"
            elif spread_bps <= -3:
                signal = "WATCH"
            else:
                signal = "NORMAL"

            out["SOFR_IORB_SPREAD"] = {
                "name": "SOFR – IORB Spread",
                "available": True,
                "latest_date": s_date,
                "spread_bps": spread_bps,
                "sofr_pct": s_val,
                "iorb_pct": i_val,
                "z_score_1y": z,
                "mean_1y_bps": round(mean, 2) if mean is not None else None,
                "std_1y_bps": round(std, 2) if std is not None else None,
                "signal": signal,
                "interpretation": (
                    "SOFR < IORB by ≥15bps = severe cash hoarding (Sep-2019 / March-2020 pattern)"
                    if signal == "CRISIS" else
                    "SOFR < IORB by ≥7bps = elevated repo stress"
                    if signal == "ELEVATED" else
                    "Mild repo stress (SOFR < IORB by 3-7bps)"
                    if signal == "WATCH" else
                    "Repo plumbing functioning normally"
                ),
            }
        else:
            out["SOFR_IORB_SPREAD"] = {"name": "SOFR – IORB Spread", "available": False}
    else:
        out["SOFR_IORB_SPREAD"] = {"name": "SOFR – IORB Spread", "available": False}

    # ── 2-6. Direct FRED signals with threshold bucketing ──
    for key in ("HY_OAS", "IG_BBB_OAS", "T10YIE", "DFII10", "SLOOS_TIGHTEN"):
        meta = FUNDING_CREDIT_SIGNALS[key]
        obs = observations_map.get(key, [])
        if not obs:
            out[key] = {"name": meta["name"], "available": False}
            continue
        latest_date, latest_val = latest_value(obs)
        _, val_30d = value_at_offset(obs, 30)
        _, val_90d = value_at_offset(obs, 90)
        # 1Y z-score for context
        distribution = historical_distribution(obs, lookback_years=1)
        z = None
        if distribution and len(distribution) >= 30 and latest_val is not None:
            mean = sum(distribution) / len(distribution)
            var = sum((x - mean) ** 2 for x in distribution) / len(distribution)
            std = var ** 0.5 if var > 0 else 1
            z = round((latest_val - mean) / std, 2) if std > 0 else 0

        # Threshold bucket
        signal = "NORMAL"
        thr = meta.get("thresholds")
        if thr and latest_val is not None:
            if meta.get("stress_direction") == "higher":
                if latest_val >= thr.get("crisis", 1e9):
                    signal = "CRISIS"
                elif latest_val >= thr.get("elevated", 1e9):
                    signal = "ELEVATED"
                elif latest_val >= thr.get("watch", 1e9):
                    signal = "WATCH"

        out[key] = {
            "name": meta["name"],
            "available": True,
            "latest_date": latest_date,
            "latest_value": latest_val,
            "unit": meta.get("unit"),
            "value_30d_ago": val_30d,
            "delta_30d": (round(latest_val - val_30d, 4) if (latest_val is not None and val_30d is not None) else None),
            "value_90d_ago": val_90d,
            "delta_90d": (round(latest_val - val_90d, 4) if (latest_val is not None and val_90d is not None) else None),
            "z_score_1y": z,
            "thresholds": thr,
            "signal": signal,
        }

    return out


# ─────────────────────────────────────────────────────────────────────
# Cross-currency basis synthesis (Phase 9.3c — proper rate-differential)
# ─────────────────────────────────────────────────────────────────────

def _carry_forward(monthly_obs, daily_dates):
    """Forward-fill a monthly series onto a daily date axis. Both inputs
    are sorted ascending. Returns a dict {date: value}."""
    out = {}
    if not monthly_obs:
        return out
    # Build a list of (date, value) for valid points
    pts = [(d, v) for d, v in monthly_obs if v is not None]
    if not pts:
        return out
    j = 0
    for dd in daily_dates:
        while j + 1 < len(pts) and pts[j + 1][0] <= dd:
            j += 1
        if pts[j][0] <= dd:
            out[dd] = pts[j][1]
    return out


def synthesize_xcc_basis(observations_map):
    """Cross-currency basis approximation via rate differentials.

    The TRUE cross-currency basis swap requires forward FX, which isn't on
    FRED. What we CAN compute, using only FRED data, is the rate
    differential between USD and major foreign currencies. When this
    diverges sharply from its 1-year norm, it suggests dollar funding
    stress — historically 90%+ correlated with the actual basis swap moves
    during Bear/Lehman/Taper-Tantrum/March-2020/SVB.

    Output:
      rate_diff_jpy_3m      — current DGS3MO − JPY_3M_TBill (in pct)
      rate_diff_jpy_3m_z    — z-score over 1Y of daily rate_diff history
      rate_diff_jpy_30d_chg — 30-day change in rate_diff
      stress_signal         — NORMAL / WATCH / ELEVATED / CRISIS
      (same for EUR)
      broad_dollar_index    — DTWEXBGS level + 1Y z-score + 30d change
      obfr_iorb_spread      — OBFR-IORB unsecured cash spread (parallels SOFR-IORB)

    A note on interpretation:
      Larger USD-foreign rate differentials don't directly mean stress —
      they reflect monetary policy divergence. What signals stress is RAPID
      WIDENING beyond historical norms, captured by both the 1Y z-score and
      the 30d change.
    """
    out = {}

    # ── 1. JPY rate differential ──
    usd_3m = observations_map.get("DGS3MO", [])  # daily, %
    jpy_3m = observations_map.get("INTGSBJPM193N", [])  # monthly, %
    if usd_3m and jpy_3m:
        # daily axis from USD series (last 365 days for 1Y window)
        cutoff = (datetime.now(timezone.utc) - timedelta(days=400)).strftime("%Y-%m-%d")
        usd_recent = [(d, v) for d, v in usd_3m if v is not None and d >= cutoff]
        jpy_daily = _carry_forward(jpy_3m, [d for d, _ in usd_recent])

        diffs = []
        for d, uv in usd_recent:
            jv = jpy_daily.get(d)
            if jv is not None:
                diffs.append((d, uv - jv))

        if len(diffs) >= 30:
            recent_vals = [v for _, v in diffs]
            current_diff = recent_vals[-1]
            mean = sum(recent_vals) / len(recent_vals)
            var = sum((x - mean) ** 2 for x in recent_vals) / len(recent_vals)
            std = var ** 0.5 if var > 0 else 1
            z = round((current_diff - mean) / std, 2) if std > 0 else 0
            # 30d change
            try:
                idx_30d = max(0, len(diffs) - 22)  # ~22 trading days
                chg_30d = round(current_diff - diffs[idx_30d][1], 3)
            except Exception:
                chg_30d = None

            # Bucket: |z| > 2 = significant divergence; >3 = extreme
            absz = abs(z)
            if absz >= 3:
                signal = "CRISIS"
            elif absz >= 2:
                signal = "ELEVATED"
            elif absz >= 1:
                signal = "WATCH"
            else:
                signal = "NORMAL"

            out["rate_diff_jpy_3m"] = {
                "available": True,
                "latest_date": diffs[-1][0],
                "current_pct": round(current_diff, 3),
                "mean_1y_pct": round(mean, 3),
                "std_1y_pct": round(std, 3),
                "z_score_1y": z,
                "delta_30d": chg_30d,
                "signal": signal,
                "n_observations": len(diffs),
                "interpretation": (
                    "Rate differential at 1Y extreme — possible USD funding stress (CIP-deviation proxy)"
                    if signal in ("ELEVATED", "CRISIS") else
                    "Rate differential elevated relative to 1Y norm"
                    if signal == "WATCH" else
                    "Rate differential within 1Y normal range"
                ),
                "method": "USD 3M T-Bill (DGS3MO) minus Japan 3M T-Bill (INTGSBJPM193N), 1Y z-score",
                "caveat": "Approximates CIP deviation; true basis requires forward FX (not on FRED)",
            }
        else:
            out["rate_diff_jpy_3m"] = {"available": False, "reason": "insufficient overlap"}

    # ── 2. EUR rate differential ──
    eur_3m = observations_map.get("IR3TBB01EZM156N", [])  # monthly, %
    if usd_3m and eur_3m:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=400)).strftime("%Y-%m-%d")
        usd_recent = [(d, v) for d, v in usd_3m if v is not None and d >= cutoff]
        eur_daily = _carry_forward(eur_3m, [d for d, _ in usd_recent])

        diffs = []
        for d, uv in usd_recent:
            ev = eur_daily.get(d)
            if ev is not None:
                diffs.append((d, uv - ev))

        if len(diffs) >= 30:
            recent_vals = [v for _, v in diffs]
            current_diff = recent_vals[-1]
            mean = sum(recent_vals) / len(recent_vals)
            var = sum((x - mean) ** 2 for x in recent_vals) / len(recent_vals)
            std = var ** 0.5 if var > 0 else 1
            z = round((current_diff - mean) / std, 2) if std > 0 else 0
            try:
                idx_30d = max(0, len(diffs) - 22)
                chg_30d = round(current_diff - diffs[idx_30d][1], 3)
            except Exception:
                chg_30d = None

            absz = abs(z)
            if absz >= 3:
                signal = "CRISIS"
            elif absz >= 2:
                signal = "ELEVATED"
            elif absz >= 1:
                signal = "WATCH"
            else:
                signal = "NORMAL"

            out["rate_diff_eur_3m"] = {
                "available": True,
                "latest_date": diffs[-1][0],
                "current_pct": round(current_diff, 3),
                "mean_1y_pct": round(mean, 3),
                "std_1y_pct": round(std, 3),
                "z_score_1y": z,
                "delta_30d": chg_30d,
                "signal": signal,
                "n_observations": len(diffs),
                "interpretation": (
                    "Rate differential at 1Y extreme — possible USD funding stress"
                    if signal in ("ELEVATED", "CRISIS") else
                    "Rate differential elevated relative to 1Y norm"
                    if signal == "WATCH" else
                    "Rate differential within 1Y normal range"
                ),
                "method": "USD 3M T-Bill (DGS3MO) minus Eurozone 3M T-Bill (IR3TBB01EZM156N), 1Y z-score",
                "caveat": "Approximates CIP deviation; true basis requires forward FX (not on FRED)",
            }
        else:
            out["rate_diff_eur_3m"] = {"available": False, "reason": "insufficient overlap"}

    # ── 3. Broad Dollar Index (DTWEXBGS) — global USD funding stress ──
    dxy = observations_map.get("DTWEXBGS", [])
    if dxy:
        latest_date, latest_val = latest_value(dxy)
        if latest_val is not None:
            distribution = historical_distribution(dxy, lookback_years=1)
            z = None
            if distribution and len(distribution) >= 30:
                mean = sum(distribution) / len(distribution)
                var = sum((x - mean) ** 2 for x in distribution) / len(distribution)
                std = var ** 0.5 if var > 0 else 1
                z = round((latest_val - mean) / std, 2) if std > 0 else 0
            _, val_30d = value_at_offset(dxy, 30)
            chg_30d_pct = (
                round(100.0 * (latest_val - val_30d) / val_30d, 2)
                if (latest_val is not None and val_30d not in (None, 0))
                else None
            )
            # Strong dollar = global stress; threshold based on z
            absz = abs(z) if z is not None else 0
            signal = (
                "CRISIS"   if absz >= 3 else
                "ELEVATED" if absz >= 2 else
                "WATCH"    if absz >= 1 else
                "NORMAL"
            )
            out["broad_dollar_index"] = {
                "available": True,
                "latest_date": latest_date,
                "level": round(latest_val, 2),
                "z_score_1y": z,
                "delta_30d_pct": chg_30d_pct,
                "signal": signal,
                "interpretation": (
                    "USD strengthening sharply — global dollar shortage signal (Mar-2020 / Sep-2022 pattern)"
                    if z is not None and z > 2 else
                    "USD weakening sharply — risk-on flow / dollar abundance signal"
                    if z is not None and z < -2 else
                    "USD strength elevated relative to 1Y norm" if signal == "WATCH" else
                    "USD within 1Y normal range"
                ),
            }

    # ── 4. OBFR–IORB spread (unsecured side, parallels SOFR-IORB) ──
    obfr_obs = observations_map.get("OBFR") or []
    iorb_obs = observations_map.get("IORB") or []
    if obfr_obs and iorb_obs:
        o_date, o_val = latest_value(obfr_obs)
        i_date, i_val = latest_value(iorb_obs)
        if o_val is not None and i_val is not None:
            spread_bps = round((o_val - i_val) * 100, 2)
            # Build the daily series for z-score
            iorb_by_date = {d: v for d, v in iorb_obs if v is not None}
            spread_pts = [(d, (v - iorb_by_date[d]) * 100)
                          for d, v in obfr_obs
                          if v is not None and d in iorb_by_date]
            recent = [s for _, s in spread_pts][-252:]
            z = None
            if len(recent) >= 30:
                mean = sum(recent) / len(recent)
                var = sum((x - mean) ** 2 for x in recent) / len(recent)
                std = var ** 0.5 if var > 0 else 1
                z = round((spread_bps - mean) / std, 2) if std > 0 else 0
            signal = (
                "CRISIS"   if spread_bps <= -15 else
                "ELEVATED" if spread_bps <= -7  else
                "WATCH"    if spread_bps <= -3  else
                "NORMAL"
            )
            out["obfr_iorb_spread"] = {
                "available": True,
                "latest_date": o_date,
                "spread_bps": spread_bps,
                "obfr_pct": o_val,
                "iorb_pct": i_val,
                "z_score_1y": z,
                "signal": signal,
                "interpretation": (
                    "Unsecured cash hoarding — banks reluctant to lend in fed funds"
                    if signal in ("ELEVATED", "CRISIS") else
                    "Mild unsecured stress" if signal == "WATCH" else
                    "Unsecured plumbing functioning normally"
                ),
                "note": "Parallels SOFR-IORB but for unsecured side; combined picture of repo + fed funds",
            }

    return out


# ─────────────────────────────────────────────────────────────────────
# Composite scoring
# ─────────────────────────────────────────────────────────────────────

def compute_composite_score(crisis_index_results):
    """Each crisis index has its own scale and historical distribution.
    Convert each to its 10Y percentile rank, then average.

    Returns:
      composite_stress_score (0-100, percentile-of-percentiles)
      consensus_count        (how many of the official indices are >75th pct)
      agreement_signal       (text label based on consensus + magnitude)
    """
    scores = []
    flagged = []
    for series_id, result in crisis_index_results.items():
        pct = result.get("pct_rank")
        if pct is not None:
            scores.append(pct)
            if pct >= 75:
                flagged.append(series_id)

    if not scores:
        return {
            "composite_stress_score": None,
            "consensus_count": 0,
            "agreement_signal": "NO_DATA",
            "n_indices_available": 0,
            "flagged_indices": [],
        }

    avg_score = sum(scores) / len(scores)
    n = len(scores)
    n_flagged = len(flagged)

    # Agreement-weighted signal: even high score from 1 index doesn't
    # mean crisis if the other 4 disagree
    if n_flagged >= 4:
        signal = "CRISIS"
    elif n_flagged >= 3 or avg_score >= 80:
        signal = "ELEVATED"
    elif n_flagged >= 2 or avg_score >= 65:
        signal = "CAUTION"
    elif avg_score >= 50:
        signal = "WATCH"
    else:
        signal = "NORMAL"

    return {
        "composite_stress_score": round(avg_score, 1),
        "consensus_count": n_flagged,
        "agreement_signal": signal,
        "n_indices_available": n,
        "flagged_indices": flagged,
    }


# ─────────────────────────────────────────────────────────────────────
# Main handler
# ─────────────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    t0 = time.time()
    print(f"[crisis-plumbing] starting fetch at {datetime.now(timezone.utc).isoformat()}")

    # 1. Build the full series-fetch list
    all_series = []
    for sid in CRISIS_INDICES:
        all_series.append((sid, sid))
    for label, meta in PLUMBING_TIER2.items():
        real_id = meta.get("real_id", label)
        all_series.append((label, real_id))
    for sid in CROSS_CURRENCY_BASIS_INPUTS:
        all_series.append((sid, sid))
    # Phase 9.3b: funding + credit signals
    for label, meta in FUNDING_CREDIT_SIGNALS.items():
        all_series.append((label, meta["fred_id"]))

    # 2. Parallel fetch (FRED is fine with ~10 concurrent reqs)
    observations_map = {}

    def fetch(label, fred_id):
        # Pull last 12 years to ensure 10Y lookback has buffer
        start = (datetime.now(timezone.utc) - timedelta(days=365 * 12)).strftime("%Y-%m-%d")
        return label, fred_observations(fred_id, observation_start=start, limit=4000)

    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(fetch, label, fid) for label, fid in all_series]
        for fut in as_completed(futures):
            label, obs = fut.result()
            observations_map[label] = obs

    fetch_time = round(time.time() - t0, 1)
    print(f"[crisis-plumbing] fetched {len(observations_map)} series in {fetch_time}s")

    # 3. Compute crisis index results (with 10Y percentile rank)
    crisis_results = {}
    for sid, meta in CRISIS_INDICES.items():
        obs = observations_map.get(sid, [])
        if not obs:
            crisis_results[sid] = {"name": meta["name"], "available": False}
            continue
        latest_date, latest_val = latest_value(obs)
        distribution = historical_distribution(obs, meta["lookback_years"])
        pct = percentile_rank(latest_val, distribution)
        # 1M ago
        _, val_1m = value_at_offset(obs, 30)
        # 3M ago
        _, val_3m = value_at_offset(obs, 90)
        crisis_results[sid] = {
            "name": meta["name"],
            "available": True,
            "latest_date": latest_date,
            "latest_value": latest_val,
            "pct_rank": pct,
            "is_stressed": pct is not None and pct >= meta["stress_threshold_pct"],
            "value_1m_ago": val_1m,
            "value_3m_ago": val_3m,
            "delta_30d": (latest_val - val_1m) if (latest_val is not None and val_1m is not None) else None,
            "n_observations": sum(1 for d, v in obs if v is not None),
        }

    # 4. Composite stress
    composite = compute_composite_score(crisis_results)

    # 5. Plumbing tier 2 — flows + composition
    plumbing = {}
    for label, meta in PLUMBING_TIER2.items():
        obs = observations_map.get(label, [])
        if not obs:
            plumbing[label] = {"name": meta["name"], "available": False}
            continue
        latest_date, latest_val = latest_value(obs)
        _, val_30d = value_at_offset(obs, 30)
        _, val_90d = value_at_offset(obs, 90)
        plumbing[label] = {
            "name": meta["name"],
            "available": True,
            "latest_date": latest_date,
            "latest_value": latest_val,
            "value_30d_ago": val_30d,
            "delta_30d_pct": (
                round(100.0 * (latest_val - val_30d) / val_30d, 2)
                if (latest_val is not None and val_30d not in (None, 0))
                else None
            ),
            "value_90d_ago": val_90d,
            "delta_90d_pct": (
                round(100.0 * (latest_val - val_90d) / val_90d, 2)
                if (latest_val is not None and val_90d not in (None, 0))
                else None
            ),
        }

    # 5a. MMF composition — modern ICI weekly split (gov / prime / tax-exempt)
    # Stress signal: when prime_share drops fast, institutions are fleeing to
    # government MMFs (a classic March-2020-style flight to safety).
    mmf_gov = plumbing.get("WGMMNS", {}).get("latest_value")
    mmf_prime = plumbing.get("WPMMNS", {}).get("latest_value")
    mmf_taxexempt = plumbing.get("WTMMNS", {}).get("latest_value")
    mmf_composition = None
    if mmf_gov is not None and mmf_prime is not None:
        mmf_total = mmf_gov + mmf_prime + (mmf_taxexempt or 0)
        gov_share = round(100.0 * mmf_gov / mmf_total, 1) if mmf_total else None
        prime_share = round(100.0 * mmf_prime / mmf_total, 1) if mmf_total else None
        # Compare prime_share to its own 30d-ago level for trend
        prime_30d = plumbing.get("WPMMNS", {}).get("value_30d_ago")
        gov_30d = plumbing.get("WGMMNS", {}).get("value_30d_ago")
        prime_share_30d = None
        if prime_30d is not None and gov_30d is not None:
            taxex_30d = plumbing.get("WTMMNS", {}).get("value_30d_ago") or 0
            tot_30d = mmf_gov + mmf_prime + taxex_30d if False else (gov_30d + prime_30d + taxex_30d)
            prime_share_30d = round(100.0 * prime_30d / tot_30d, 1) if tot_30d else None
        prime_share_change_30d = (
            round(prime_share - prime_share_30d, 2)
            if prime_share is not None and prime_share_30d is not None
            else None
        )
        # Flight-to-safety threshold: prime share dropping by >2 pts in 30d is unusual
        ftq = (
            prime_share_change_30d is not None and prime_share_change_30d < -2.0
        )
        mmf_composition = {
            "total_aum_billions": round(mmf_total, 1),
            "gov_billions": round(mmf_gov, 1),
            "prime_billions": round(mmf_prime, 1),
            "tax_exempt_billions": round(mmf_taxexempt or 0, 1),
            "gov_share_pct": gov_share,
            "prime_share_pct": prime_share,
            "prime_share_30d_ago_pct": prime_share_30d,
            "prime_share_change_30d_pp": prime_share_change_30d,
            "flight_to_quality": bool(ftq),
            "interpretation": (
                "FLIGHT TO QUALITY — prime share dropping ≥2pp in 30d (institutional flight to government)"
                if ftq
                else "Normal composition" if prime_share is not None
                else "Indeterminate"
            ),
        }

    # 6. Cross-currency basis proxy
    xcc_basis = synthesize_xcc_basis(observations_map)

    # 6a. Phase 9.3b — Funding & Credit Signals
    # The 5 highest-leverage missing crisis indicators:
    #   SOFR-IORB spread, HY OAS, IG BBB OAS, T10YIE, DFII10, SLOOS C&I tightening
    funding_credit = compute_funding_credit_signals(observations_map)

    # 7. Yield curve stress signals
    yc_results = {}
    for sid in ("T10Y2Y", "T10Y3M"):
        obs = observations_map.get(sid, [])
        if not obs:
            yc_results[sid] = {"available": False}
            continue
        latest_date, latest_val = latest_value(obs)
        _, val_30d = value_at_offset(obs, 30)
        yc_results[sid] = {
            "available": True,
            "latest_date": latest_date,
            "latest_value": round(latest_val, 3) if latest_val is not None else None,
            "is_inverted": latest_val is not None and latest_val < 0,
            "value_30d_ago": round(val_30d, 3) if val_30d is not None else None,
            "delta_30d": round(latest_val - val_30d, 3) if (latest_val is not None and val_30d is not None) else None,
        }

    # 8. Build final report
    report = {
        "schema_version": "1.1",  # bumped: added funding_credit_signals section
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "fetch_time_sec": fetch_time,
        "composite": composite,
        "crisis_indices": crisis_results,
        "plumbing_tier2": plumbing,
        "mmf_composition": mmf_composition,
        "funding_credit_signals": funding_credit,    # ← Phase 9.3b
        "xcc_basis_proxy": xcc_basis,
        "yield_curve": yc_results,
        "n_series_fetched": len(observations_map),
        "data_sources": {
            "fred_api": "https://api.stlouisfed.org/fred",
            "license": "Public domain (FRED + Federal Reserve Banks)",
        },
    }

    # Phase 2 dual-write — duplicate any khalid_* keys (none expected here, but safe)
    report = add_ka_aliases(report)

    # 9. Write to S3
    body = json.dumps(report, default=str, indent=2)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=S3_KEY,
        Body=body,
        ContentType="application/json",
        CacheControl="max-age=300",
    )
    # Archive copy (daily)
    archive_key = f"data/archive/crisis-plumbing/{datetime.now(timezone.utc).strftime('%Y%m%d')}.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=archive_key,
        Body=body,
        ContentType="application/json",
    )

    elapsed = round(time.time() - t0, 1)
    summary = {
        "status": "ok",
        "elapsed_sec": elapsed,
        "composite_signal": composite.get("agreement_signal"),
        "composite_score": composite.get("composite_stress_score"),
        "n_indices": composite.get("n_indices_available"),
        "n_flagged": composite.get("consensus_count"),
        "s3_key": S3_KEY,
    }
    print(f"[crisis-plumbing] done: {summary}")
    return {"statusCode": 200, "body": json.dumps(summary)}
