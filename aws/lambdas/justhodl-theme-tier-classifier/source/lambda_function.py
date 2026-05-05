"""
justhodl-theme-tier-classifier  (Layer 3 of nobrainer hunter)
=============================================================
For each detected theme (focus: EXTENDED, ACCELERATING, EMERGING from Layer 1),
classifies its constituent stocks into tier-1/2/3 and pulls fundamentals from
FMP to compute valuation asymmetry.

Tier definitions:
  • Tier-1: top 5 holdings (most "obvious" plays — usually crowded/inflated)
  • Tier-2: holdings 6-10 (still in ETF but less covered)
  • Tier-3: industry peers NOT in top 10 (deepest asymmetry potential)

For each ticker we compute:
  • market_cap, revenue_ttm, p_s, p_e, ev_ebitda, fcf_yield
  • mcap_to_rev_ratio  ← the "$50B mcap on $30B revenue" metric
  • z_p_s_vs_theme    ← z-score of P/S vs theme median (negative = cheap)
  • z_p_e_vs_theme    ← z-score of P/E vs theme median
  • val_asymmetry_score 0-100  ← composite cheapness vs theme peers

Schedule: cron(0 8 * * ? *) — daily 08:00 UTC (after supply-inflection at 07:00)
Input:    s3://justhodl-dashboard-live/data/themes-detected.json (Layer 1)
Output:   s3://justhodl-dashboard-live/data/theme-tiers.json

Downstream Layer 4 (asymmetric-hunter) consumes this for valuation_asymmetry_score
in its 5-factor scorecard.
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FMP_BASE = "https://financialmodelingprep.com/api/v3"

# Filter Layer 1 themes by phase before classifying — focus on the phases
# where tier-2/3 hunting makes sense. EXTENDED is the prime ground (the user's
# MU/SNDK pattern lives here). EMERGING + ACCELERATING are next-best.
PHASES_TO_CLASSIFY = {"EXTENDED", "ACCELERATING", "EMERGING", "PEAKING"}

# Concurrency / throttling for FMP (free tier: 250/day, premium: 750/min)
MAX_WORKERS = 6
FETCH_RETRIES = 3
RETRY_BASE_DELAY = 0.8

# Cache fundamentals across tickers that appear in multiple themes
_FUNDAMENTAL_CACHE = {}
_CACHE_LOCK = Lock()

S3 = boto3.client("s3", region_name=REGION)


# ─────────────────────────────────────────────────────────────────────────────
# FMP FETCHERS (with retry/backoff)
# ─────────────────────────────────────────────────────────────────────────────
def fmp_get(path, params=None, retries=FETCH_RETRIES):
    """GET FMP endpoint with retry on rate-limit/5xx."""
    qs = f"apikey={FMP_KEY}"
    if params:
        for k, v in params.items():
            qs += f"&{k}={v}"
    url = f"{FMP_BASE}{path}?{qs}"

    last_err = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "justhodl-tier-classifier/1.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                txt = resp.read().decode("utf-8", errors="replace")
                return json.loads(txt) if txt else None
        except urllib.error.HTTPError as e:
            last_err = f"HTTP{e.code}"
            body = e.read().decode("utf-8", errors="replace")[:120] if hasattr(e, "read") else ""
            if e.code in (429, 502, 503, 504):
                time.sleep(RETRY_BASE_DELAY * (2 ** attempt))
                continue
            if e.code == 401:
                print(f"[fmp] 401 unauthorized {path} — check FMP_KEY")
                return None
            if e.code == 404:
                return None
            print(f"[fmp] HTTP{e.code} {path} body={body[:100]}")
            return None
        except urllib.error.URLError as e:
            last_err = f"URL:{e.reason}"
            time.sleep(RETRY_BASE_DELAY * (2 ** attempt))
            continue
        except Exception as e:
            last_err = f"{type(e).__name__}:{e}"
            time.sleep(RETRY_BASE_DELAY * (2 ** attempt))
            continue
    print(f"[fmp] {path} all_retries_failed err={last_err}")
    return None


def fetch_fundamentals(ticker):
    """
    Pull profile + key-metrics-ttm from FMP. Returns dict or None.
    """
    with _CACHE_LOCK:
        if ticker in _FUNDAMENTAL_CACHE:
            return _FUNDAMENTAL_CACHE[ticker]

    profile = fmp_get(f"/profile/{ticker}")
    if not profile or not isinstance(profile, list) or not profile:
        result = None
    else:
        p = profile[0]
        # key-metrics-ttm has the trailing-twelve-month ratios
        ktm = fmp_get(f"/key-metrics-ttm/{ticker}", params={"limit": "1"})
        kt = ktm[0] if (ktm and isinstance(ktm, list) and ktm) else {}

        # ratios-ttm has additional ratios
        rtm = fmp_get(f"/ratios-ttm/{ticker}")
        rt = rtm[0] if (rtm and isinstance(rtm, list) and rtm) else {}

        market_cap = p.get("mktCap")
        revenue_ttm = kt.get("revenuePerShareTTM")
        shares = (market_cap / p.get("price")) if (market_cap and p.get("price")) else None
        if revenue_ttm is not None and shares:
            revenue_ttm_total = revenue_ttm * shares
        else:
            revenue_ttm_total = None

        result = {
            "ticker": ticker,
            "name": p.get("companyName"),
            "sector": p.get("sector"),
            "industry": p.get("industry"),
            "exchange": p.get("exchangeShortName"),
            "country": p.get("country"),
            "currency": p.get("currency"),
            "price": p.get("price"),
            "market_cap": market_cap,
            "shares_outstanding": shares,
            "revenue_ttm": revenue_ttm_total,
            "p_s": kt.get("priceToSalesRatioTTM") or rt.get("priceToSalesRatioTTM"),
            "p_e": kt.get("peRatioTTM") or rt.get("peRatioTTM"),
            "ev_ebitda": kt.get("enterpriseValueOverEBITDATTM"),
            "fcf_yield": kt.get("freeCashFlowYieldTTM"),
            "fcf_per_share": kt.get("freeCashFlowPerShareTTM"),
            "debt_to_equity": kt.get("debtToEquityTTM"),
            "roic": kt.get("roicTTM"),
            "gross_margin": rt.get("grossProfitMarginTTM"),
            "operating_margin": rt.get("operatingProfitMarginTTM"),
            "net_margin": rt.get("netProfitMarginTTM"),
            "rev_growth_ttm": rt.get("revenueGrowthTTM"),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

        # Derived: market_cap / revenue (asymmetry detector)
        if result["market_cap"] and result["revenue_ttm"]:
            result["mcap_to_rev"] = round(result["market_cap"] / result["revenue_ttm"], 3)
        else:
            result["mcap_to_rev"] = None

    with _CACHE_LOCK:
        _FUNDAMENTAL_CACHE[ticker] = result
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Z-SCORES + ASYMMETRY SCORING
# ─────────────────────────────────────────────────────────────────────────────
def median(values):
    vals = sorted(v for v in values if v is not None and v == v)  # filter None and NaN
    n = len(vals)
    if n == 0:
        return None
    if n % 2 == 1:
        return vals[n // 2]
    return (vals[n // 2 - 1] + vals[n // 2]) / 2.0


def stdev(values, mu):
    vals = [v for v in values if v is not None and v == v]
    if len(vals) < 2:
        return None
    var = sum((v - mu) ** 2 for v in vals) / (len(vals) - 1)
    return var ** 0.5


def z_score(value, mu, sigma):
    if value is None or mu is None or sigma is None or sigma == 0:
        return None
    return (value - mu) / sigma


def asymmetry_score(fundamentals, theme_stats):
    """
    0-100 score where 100 = "deepest asymmetry vs theme peers".
    Components:
      • z_p_s (negative = cheap) → 35%
      • z_p_e (negative = cheap) → 25%
      • mcap_to_rev_score (low = asymmetric) → 30%
      • fcf_yield_score (high = real-cash cheap) → 10%
    """
    if not fundamentals:
        return 0, {}

    z_ps = z_score(fundamentals.get("p_s"), theme_stats["p_s_mu"], theme_stats["p_s_sigma"])
    z_pe = z_score(fundamentals.get("p_e"), theme_stats["p_e_mu"], theme_stats["p_e_sigma"])

    components = {
        "z_p_s": round(z_ps, 3) if z_ps is not None else None,
        "z_p_e": round(z_pe, 3) if z_pe is not None else None,
    }

    # Component A: P/S z-score (capped at -2σ = max 100)
    a = 0
    if z_ps is not None:
        # negative z is cheap → boost score; positive z is expensive → penalize
        a = max(0.0, min(100.0, 50.0 - 25.0 * z_ps))

    # Component B: P/E z-score
    b = 0
    if z_pe is not None:
        b = max(0.0, min(100.0, 50.0 - 25.0 * z_pe))

    # Component C: mcap_to_rev. Below 2 = MU-grade asymmetry (full score).
    # 2-5 = decent. 5-10 = neutral. >10 = expensive.
    mcr = fundamentals.get("mcap_to_rev")
    c = 0
    if mcr is not None and mcr > 0:
        if mcr <= 2:
            c = 100
        elif mcr <= 5:
            c = 100 - (mcr - 2) * (40 / 3)  # 100 → 60
        elif mcr <= 10:
            c = 60 - (mcr - 5) * 6  # 60 → 30
        else:
            c = max(0.0, 30 - (mcr - 10) * 1.5)
    components["mcap_to_rev_score"] = round(c, 1)

    # Component D: FCF yield. >5% = healthy positive cash; >10% = great
    fy = fundamentals.get("fcf_yield")
    d = 0
    if fy is not None:
        if fy >= 0.10:
            d = 100
        elif fy >= 0.05:
            d = 70
        elif fy >= 0.02:
            d = 40
        elif fy >= 0:
            d = 20
        else:
            d = 0
    components["fcf_yield_score"] = round(d, 1)

    raw = 0.35 * a + 0.25 * b + 0.30 * c + 0.10 * d
    score = round(max(0.0, min(100.0, raw)), 1)
    components["score"] = score
    components["component_p_s"] = round(a, 1)
    components["component_p_e"] = round(b, 1)

    # Verbal flag
    if score >= 75:
        flag = "DEEP_ASYMMETRY"
    elif score >= 60:
        flag = "ASYMMETRIC"
    elif score >= 40:
        flag = "FAIR_VALUE"
    else:
        flag = "EXPENSIVE"
    components["flag"] = flag

    return score, components


def compute_theme_stats(fund_list):
    """For a theme's holdings, compute median/stdev of P/S, P/E, mcap_to_rev."""
    p_s_vals = [f["p_s"] for f in fund_list if f and f.get("p_s") and f["p_s"] > 0]
    p_e_vals = [f["p_e"] for f in fund_list if f and f.get("p_e") and f["p_e"] > 0]
    mcr_vals = [f["mcap_to_rev"] for f in fund_list if f and f.get("mcap_to_rev") and f["mcap_to_rev"] > 0]

    p_s_mu = median(p_s_vals)
    p_s_sigma = stdev(p_s_vals, p_s_mu) if p_s_mu is not None else None
    p_e_mu = median(p_e_vals)
    p_e_sigma = stdev(p_e_vals, p_e_mu) if p_e_mu is not None else None
    mcr_mu = median(mcr_vals)

    return {
        "p_s_mu": p_s_mu,
        "p_s_sigma": p_s_sigma,
        "p_e_mu": p_e_mu,
        "p_e_sigma": p_e_sigma,
        "mcap_to_rev_median": mcr_mu,
        "n_with_p_s": len(p_s_vals),
        "n_with_p_e": len(p_e_vals),
        "n_with_mcr": len(mcr_vals),
    }


# ─────────────────────────────────────────────────────────────────────────────
# MAIN HANDLER
# ─────────────────────────────────────────────────────────────────────────────
def lambda_handler(event=None, context=None):
    started = time.time()

    # Load Layer 1 output
    try:
        l1_obj = S3.get_object(Bucket=BUCKET, Key="data/themes-detected.json")
        l1 = json.loads(l1_obj["Body"].read())
    except Exception as e:
        print(f"[tier-classifier] Layer 1 load FAILED: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": "layer1_unavailable", "message": str(e)})}

    themes_l1 = l1.get("themes") or []
    target_themes = [t for t in themes_l1 if t["phase"] in PHASES_TO_CLASSIFY]
    print(f"[tier-classifier] Layer 1 had {len(themes_l1)} themes, {len(target_themes)} in target phases ({sorted(PHASES_TO_CLASSIFY)})")

    # Build unique ticker universe across all target themes
    universe = set()
    theme_to_tickers = {}
    for t in target_themes:
        holdings = t.get("top_holdings") or []
        # filter out non-stock symbols (some ETFs have foreign tickers we can't fetch)
        clean = [h for h in holdings if h and "." not in h and h.isalnum()]
        theme_to_tickers[t["etf"]] = clean
        universe.update(clean)
    print(f"[tier-classifier] {len(universe)} unique tickers across {len(target_themes)} themes")

    # Fetch fundamentals in parallel
    fetch_started = time.time()
    n_ok, n_fail = 0, 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(fetch_fundamentals, t): t for t in universe}
        for fut in as_completed(futures):
            try:
                result = fut.result()
                if result and result.get("market_cap"):
                    n_ok += 1
                else:
                    n_fail += 1
            except Exception as e:
                n_fail += 1
                print(f"[fund-error] {futures[fut]} {type(e).__name__} {e}")
    fetch_dur = round(time.time() - fetch_started, 1)
    print(f"[tier-classifier] fetched {n_ok} ok / {n_fail} failed in {fetch_dur}s")

    # Classify each theme
    themes_out = {}
    asymmetric_leaderboard = []

    for t in target_themes:
        etf = t["etf"]
        holdings = theme_to_tickers.get(etf, [])
        if not holdings:
            continue

        fund_list = []
        for tk in holdings:
            with _CACHE_LOCK:
                f = _FUNDAMENTAL_CACHE.get(tk)
            if f and f.get("market_cap"):
                fund_list.append(f)

        if len(fund_list) < 3:
            print(f"[tier-classifier] {etf} skipped: only {len(fund_list)} fundamentals")
            continue

        theme_stats = compute_theme_stats(fund_list)

        # Score each holding for asymmetry
        per_ticker = {}
        for idx, tk in enumerate(holdings):
            with _CACHE_LOCK:
                f = _FUNDAMENTAL_CACHE.get(tk)
            if not f:
                continue
            tier = 1 if idx < 5 else 2  # tier-3 expansion in v1.1 (industry peers)
            score, comps = asymmetry_score(f, theme_stats)
            per_ticker[tk] = {
                "ticker": tk,
                "name": f.get("name"),
                "tier": tier,
                "rank_in_etf": idx + 1,
                "fundamentals": {
                    "market_cap": f.get("market_cap"),
                    "revenue_ttm": f.get("revenue_ttm"),
                    "price": f.get("price"),
                    "p_s": f.get("p_s"),
                    "p_e": f.get("p_e"),
                    "ev_ebitda": f.get("ev_ebitda"),
                    "fcf_yield": f.get("fcf_yield"),
                    "gross_margin": f.get("gross_margin"),
                    "rev_growth_ttm": f.get("rev_growth_ttm"),
                    "mcap_to_rev": f.get("mcap_to_rev"),
                    "industry": f.get("industry"),
                    "sector": f.get("sector"),
                },
                "asymmetry_score": score,
                "asymmetry_components": comps,
            }

            # Add to leaderboard if scored
            if score >= 50 and tier in (1, 2):
                asymmetric_leaderboard.append({
                    "ticker": tk,
                    "name": f.get("name"),
                    "theme_etf": etf,
                    "theme_name": t["name"],
                    "theme_phase": t["phase"],
                    "tier": tier,
                    "asymmetry_score": score,
                    "flag": comps.get("flag"),
                    "mcap_to_rev": f.get("mcap_to_rev"),
                    "p_s": f.get("p_s"),
                    "p_e": f.get("p_e"),
                    "fcf_yield": f.get("fcf_yield"),
                })

        themes_out[etf] = {
            "etf": etf,
            "name": t["name"],
            "category": t["category"],
            "phase": t["phase"],
            "phase_score": t["phase_score"],
            "theme_stats": theme_stats,
            "tickers": per_ticker,
            "n_classified": len(per_ticker),
        }

    asymmetric_leaderboard.sort(key=lambda x: -x["asymmetry_score"])

    # Tier-2 only leaderboard (this is where the user's MU/SNDK trade lives)
    tier2_leaderboard = [x for x in asymmetric_leaderboard if x["tier"] == 2][:25]

    # MU-grade specifically — mcap_to_rev <= 3 (the user's example case)
    mu_grade = [
        x for x in asymmetric_leaderboard
        if x.get("mcap_to_rev") is not None and x["mcap_to_rev"] <= 3.0
    ][:20]

    output = {
        "schema_version": "1.0",
        "method": "theme_tier_classifier_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - started, 1),
        "fetch_stats": {
            "n_unique_tickers": len(universe),
            "n_fundamentals_ok": n_ok,
            "n_fundamentals_fail": n_fail,
            "fetch_duration_s": fetch_dur,
        },
        "input_phases": sorted(PHASES_TO_CLASSIFY),
        "n_themes_classified": len(themes_out),
        "summary": {
            "n_total_classifications": sum(t["n_classified"] for t in themes_out.values()),
            "top_asymmetric_leaderboard": asymmetric_leaderboard[:30],
            "tier2_leaderboard": tier2_leaderboard,
            "mu_grade_leaderboard": mu_grade,
            "n_deep_asymmetry": sum(1 for x in asymmetric_leaderboard if x["asymmetry_score"] >= 75),
            "n_asymmetric": sum(1 for x in asymmetric_leaderboard if x["asymmetry_score"] >= 60),
        },
        "themes": themes_out,
        "schema": {
            "description": (
                "Layer 3 of nobrainer hunter pipeline. For each detected theme in "
                "EXTENDED/ACCELERATING/EMERGING/PEAKING phases, classifies "
                "constituents into tier-1 (top 5) and tier-2 (6-10) and pulls FMP "
                "fundamentals (market_cap, revenue_ttm, P/S, P/E, EV/EBITDA, FCF). "
                "Computes z-scores vs theme median and mcap_to_rev_ratio (the "
                "MU/SNDK-style asymmetry metric)."
            ),
            "asymmetry_score_components": {
                "z_p_s_vs_theme": "35%",
                "z_p_e_vs_theme": "25%",
                "mcap_to_rev_score": "30%",
                "fcf_yield_score": "10%",
            },
            "flags": {
                "DEEP_ASYMMETRY": ">=75",
                "ASYMMETRIC": "60-74",
                "FAIR_VALUE": "40-59",
                "EXPENSIVE": "<40",
            },
        },
    }

    body = json.dumps(output, default=str)
    S3.put_object(
        Bucket=BUCKET,
        Key="data/theme-tiers.json",
        Body=body.encode("utf-8"),
        ContentType="application/json",
        CacheControl="max-age=60, public",
    )
    print(f"[tier-classifier] wrote {len(body)}b to data/theme-tiers.json")
    print(f"[tier-classifier] top asymmetric: {[(x['ticker'], x['asymmetry_score']) for x in asymmetric_leaderboard[:8]]}")
    print(f"[tier-classifier] MU-grade (mcap_to_rev<=3): {[(x['ticker'], x['mcap_to_rev']) for x in mu_grade[:5]]}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_themes_classified": output["n_themes_classified"],
            "n_unique_tickers": len(universe),
            "n_fundamentals_ok": n_ok,
            "n_deep_asymmetry": output["summary"]["n_deep_asymmetry"],
            "n_asymmetric": output["summary"]["n_asymmetric"],
            "top_asymmetric": [x["ticker"] for x in asymmetric_leaderboard[:5]],
            "mu_grade": [x["ticker"] for x in mu_grade[:5]],
            "duration_s": output["duration_s"],
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
