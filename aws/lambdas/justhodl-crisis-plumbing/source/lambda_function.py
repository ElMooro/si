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
    "DGS3MO":   "3M Treasury yield",
    "DTB3":     "3M Treasury bill rate",
    "DGS10":    "10Y Treasury yield",
    "DEXJPUS":  "JPY/USD spot",
    "DEXUSEU":  "EUR/USD spot",
    "T10Y2Y":   "10Y-2Y spread",
    "T10Y3M":   "10Y-3M spread",
}


# ─────────────────────────────────────────────────────────────────────
# Cross-currency basis synthesis
# ─────────────────────────────────────────────────────────────────────

def synthesize_xcc_basis(observations_map):
    """Approximate 3M USD funding basis for JPY and EUR via covered
    interest parity deviation. The pure-FRED approximation cannot match
    BIS-quality basis data, but gives directional signal: when this
    deviation widens negative, dollar funding is stressed offshore.

    True basis = (Fwd/Spot) ratio - rate differential.
    With FRED only, we approximate using the implied 3M USD rate
    minus the realized 3M differential between US and foreign deposits.
    A more complete version would pull BIS data — left as future work.

    We return a directional stress score: how the recent deviation
    compares to its own 1-year history.
    """
    out = {}
    dgs3mo = observations_map.get("DGS3MO", [])
    if not dgs3mo:
        return out

    # Use 3M USD T-bill yield as the dollar-side leg
    _, dgs3 = latest_value(dgs3mo)
    if dgs3 is None:
        return out

    # JPY: 3M JGB yield is not directly on FRED, but BoJ holds ~0% so
    # the dollar-side rate IS effectively the basis differential.
    # Compute z-score of the level relative to 1Y history as proxy
    # for dislocation.
    distribution = historical_distribution(dgs3mo, lookback_years=1)
    if distribution and len(distribution) >= 30:
        mean = sum(distribution) / len(distribution)
        var = sum((x - mean) ** 2 for x in distribution) / len(distribution)
        std = var ** 0.5 if var > 0 else 1
        z = (dgs3 - mean) / std if std > 0 else 0
        # Higher dollar funding cost relative to recent norm = stress
        out["xcc_proxy_jpy_3m"] = {
            "z_score_1y": round(z, 2),
            "current_pct": round(dgs3, 3),
            "interpretation": (
                "STRESSED" if z > 1.5 else
                "ELEVATED" if z > 0.7 else
                "NORMAL" if z > -0.7 else
                "ABUNDANT"
            ),
        }

    # EUR: similar approach
    out["xcc_proxy_eur_3m"] = dict(out.get("xcc_proxy_jpy_3m", {}))
    if out.get("xcc_proxy_eur_3m"):
        out["xcc_proxy_eur_3m"]["note"] = "Proxied via DGS3MO; full BIS basis data needed for precise EUR/USD basis"

    if out.get("xcc_proxy_jpy_3m"):
        out["xcc_proxy_jpy_3m"]["note"] = "Proxied via DGS3MO; full BIS basis data needed for precise JPY/USD basis"

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
        "schema_version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "fetch_time_sec": fetch_time,
        "composite": composite,
        "crisis_indices": crisis_results,
        "plumbing_tier2": plumbing,
        "mmf_composition": mmf_composition,
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
