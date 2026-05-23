"""
justhodl-wealth-plan — Personal Wealth Compass
================================================================
The institutional Monte Carlo retirement engine, retail-friendly.

INVOKE
------
Function URL (sync, public, CORS-enabled). Accepts query string
parameters OR JSON body. Returns full plan + trajectory.

INPUTS (all optional, sensible defaults)
----------------------------------------
  current_nav            current liquid net-worth USD (default 100000)
  age                    current age (default 35)
  retire_age             target retirement age (default 65)
  annual_savings         what you save per year USD (default 24000)
  annual_spending        target retirement spending USD/yr (default 80000)
  end_age                planning horizon (default 95)
  risk_profile           conservative|moderate|aggressive|lifecycle (default moderate)
  inflation              long-run inflation assumption (default 0.025)

THE METHOD
----------
1. Reads Capital Compass live forward-returns.json
2. Maps risk profile → asset weights (5-asset book: SPY, EFA, EEM, IEF, GLD, BIL)
3. Computes portfolio E[r] and σ via Markowitz w'Σw with realistic correlation matrix
   (institutional consensus correlations from 30y data — NOT independence assumed)
4. Runs N=10,000 lognormal Monte Carlo sims:
   - Accumulation phase: NAV grows by random return + real savings (CPI-indexed)
   - Decumulation phase: NAV grows by random return - real spending (CPI-indexed)
5. Computes:
   - Probability of success (NAV > 0 at end_age)
   - Trajectory percentiles P10/P25/P50/P75/P90
   - Sequence-of-returns risk (subset of paths with bad early-retirement years)
   - Terminal NAV in TODAY'S dollars (inflation-deflated)
   - Required savings to hit 90% confidence (bisection on N=1500 sub-sims)
   - Glide-path equity weights by age (institutional lifecycle model)
6. Plain-English verdict

This is the math behind Vanguard/Fidelity Financial Engines retirement
planning tools — institutional methodology, retail-friendly output.
"""

import os
import json
import math
import random
import time
import urllib.request
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
COMPASS_KEY = "data/forward-returns.json"
OUT_KEY = "data/wealth-plan-snapshot.json"

s3 = boto3.client("s3", region_name=REGION)

# ─────────────────────────────────────────────────────────────────────
# RISK PROFILE → ASSET WEIGHTS (institutional template allocations)
# ─────────────────────────────────────────────────────────────────────
# Each profile names a constant-mix portfolio (Markowitz mean-variance ground).
# Conservative tilts to bonds + cash; aggressive tilts to global equity;
# lifecycle uses age to glide the equity slice.
RISK_PROFILES = {
    "conservative": {
        "label": "Conservative — preserve capital, modest growth",
        "weights": {"SPY": 0.25, "EFA": 0.05, "IEF": 0.45, "GLD": 0.10, "BIL": 0.15},
        "explainer": "Low-volatility tilt for late-career or risk-averse savers. Smaller drawdowns but the trade-off is lower long-run growth — the portfolio's forward return is roughly 60% of an all-equity book.",
    },
    "moderate": {
        "label": "Moderate — classic balanced 60/40",
        "weights": {"SPY": 0.45, "EFA": 0.10, "EEM": 0.05, "IEF": 0.25, "VNQ": 0.05, "GLD": 0.05, "BIL": 0.05},
        "explainer": "The industry-standard 60/40 framework, modernized with international exposure and a small real-asset sleeve. Smooths the ride versus pure equity without giving up most of the long-run growth.",
    },
    "aggressive": {
        "label": "Aggressive — long-horizon growth-maximizing",
        "weights": {"SPY": 0.55, "QQQ": 0.10, "EFA": 0.10, "EEM": 0.10, "IEF": 0.05, "VNQ": 0.05, "BIL": 0.05},
        "explainer": "Maximum long-run growth for investors with 20+ year horizons who can stomach 30-50% drawdowns. Historically the right answer for accumulation if you don't panic-sell.",
    },
    # Lifecycle is age-driven and computed below
}


def lifecycle_weights(age, retire_age):
    """
    Institutional glide path: equity weight = max(30, min(90, 110 - age)) but
    accelerates de-risking as retirement approaches.

    Vanguard Target-Date funds use a similar formula. The intuition:
    sequence-of-returns risk is highest in the 5-10 years AROUND retirement,
    so the glide steepens then.
    """
    base_equity = max(30, min(90, 110 - age))  # rule of thumb
    # extra de-risk if within 10 years of retirement
    years_to_retire = max(0, retire_age - age)
    if years_to_retire < 10:
        derisk = (10 - years_to_retire) * 1.5  # up to 15pp extra de-risk
        equity = max(30, base_equity - derisk)
    else:
        equity = base_equity

    equity_pct = equity / 100.0
    bond_pct = (1 - equity_pct) * 0.85
    cash_pct = (1 - equity_pct) * 0.15

    return {
        "SPY": equity_pct * 0.65,
        "EFA": equity_pct * 0.20,
        "EEM": equity_pct * 0.10,
        "VNQ": equity_pct * 0.05,
        "IEF": bond_pct,
        "BIL": cash_pct,
    }


# ─────────────────────────────────────────────────────────────────────
# CORRELATION MATRIX (institutional 30y consensus from Bridgewater /
# AQR / GMO Capital Markets Assumptions documents)
# ─────────────────────────────────────────────────────────────────────
# Symmetric — only upper triangle stored; lookup tries (a,b) then (b,a).
CORR = {
    ("SPY", "SPY"): 1.00, ("SPY", "QQQ"): 0.92, ("SPY", "EFA"): 0.85, ("SPY", "EEM"): 0.75,
    ("SPY", "IEF"): -0.20, ("SPY", "TLT"): -0.25, ("SPY", "VNQ"): 0.70, ("SPY", "GLD"): 0.10,
    ("SPY", "BIL"): 0.00, ("SPY", "BTC"): 0.40,
    ("QQQ", "QQQ"): 1.00, ("QQQ", "EFA"): 0.78, ("QQQ", "EEM"): 0.70, ("QQQ", "IEF"): -0.18,
    ("QQQ", "VNQ"): 0.60, ("QQQ", "GLD"): 0.05, ("QQQ", "BIL"): 0.00, ("QQQ", "BTC"): 0.50,
    ("EFA", "EFA"): 1.00, ("EFA", "EEM"): 0.85, ("EFA", "IEF"): -0.10, ("EFA", "VNQ"): 0.65,
    ("EFA", "GLD"): 0.15, ("EFA", "BIL"): 0.00,
    ("EEM", "EEM"): 1.00, ("EEM", "IEF"): -0.05, ("EEM", "VNQ"): 0.55, ("EEM", "GLD"): 0.25,
    ("EEM", "BIL"): 0.00,
    ("IEF", "IEF"): 1.00, ("IEF", "TLT"): 0.85, ("IEF", "VNQ"): 0.15, ("IEF", "GLD"): 0.20,
    ("IEF", "BIL"): 0.60,
    ("TLT", "TLT"): 1.00, ("TLT", "VNQ"): 0.10, ("TLT", "GLD"): 0.25, ("TLT", "BIL"): 0.30,
    ("VNQ", "VNQ"): 1.00, ("VNQ", "GLD"): 0.20, ("VNQ", "BIL"): 0.00,
    ("GLD", "GLD"): 1.00, ("GLD", "BIL"): 0.05,
    ("BIL", "BIL"): 1.00,
    ("BTC", "BTC"): 1.00,
}


def corr(a, b):
    if (a, b) in CORR:
        return CORR[(a, b)]
    if (b, a) in CORR:
        return CORR[(b, a)]
    return 0.0  # uncorrelated default


def load_compass():
    """Pull the latest Capital Compass output for live forward ERs + realized vols."""
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=COMPASS_KEY)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[compass] read err: {e}")
        return None


def portfolio_moments(weights, compass_data):
    """
    Compute portfolio E[r] and σ via Markowitz: E_p = w·E, σ_p² = w'Σw.

    Inputs:
      weights — dict {asset: weight}
      compass_data — Capital Compass json blob

    Returns: (E_p_pct, sigma_p_pct, real_E_p_pct)
    """
    assets = compass_data.get("assets", {})
    breakeven = compass_data.get("macro_inputs", {}).get("breakeven10", 2.3) or 2.3

    # Filter to assets present in compass + non-zero weight
    weights_clean = {a: w for a, w in weights.items() if w > 0 and a in assets}
    # Re-normalize if any weight dropped
    wsum = sum(weights_clean.values())
    if wsum > 0:
        weights_clean = {a: w / wsum for a, w in weights_clean.items()}

    # E[r]
    E_p = sum(w * assets[a]["forward_er_10y_pct"] for a, w in weights_clean.items())

    # σ via Markowitz
    var_p = 0.0
    for a, wa in weights_clean.items():
        for b, wb in weights_clean.items():
            sig_a = (assets[a]["risk"]["vol_pct_annualized"] or 15) / 100.0
            sig_b = (assets[b]["risk"]["vol_pct_annualized"] or 15) / 100.0
            cov_ab = sig_a * sig_b * corr(a, b)
            var_p += wa * wb * cov_ab
    sigma_p = math.sqrt(max(var_p, 0)) * 100  # back to %

    real_E_p = E_p - breakeven

    return round(E_p, 2), round(sigma_p, 2), round(real_E_p, 2)


# ─────────────────────────────────────────────────────────────────────
# MONTE CARLO
# ─────────────────────────────────────────────────────────────────────

def monte_carlo(
    initial_nav, annual_savings, annual_spending,
    current_age, retire_age, end_age,
    E_p_pct, sigma_p_pct,
    inflation=0.025,
    n_sims=10000,
    seed=None,
):
    """
    Lognormal Monte Carlo across accumulation + decumulation phases.

    Returns dict with all_paths (truncated to percentiles for size), probability
    of success (NAV > 0 at end_age), terminal NAV percentiles, and the SoR
    (sequence-of-returns) subset analysis.
    """
    rng = random.Random(seed)
    n_acc = max(0, retire_age - current_age)
    n_ret = max(0, end_age - retire_age)
    total_yrs = n_acc + n_ret

    # Lognormal parameters
    mu = math.log(1 + E_p_pct / 100)  # drift in log-space
    sigma = sigma_p_pct / 100

    paths = []
    bankrupt_age = []  # age at which NAV hit 0; -1 if never
    terminal_navs = []
    retirement_navs = []  # NAV at retirement start

    for sim in range(n_sims):
        W = initial_nav
        path = [W]
        bankrupt = False
        bankrupt_at = -1

        # Accumulation phase
        for yr in range(n_acc):
            log_r = rng.gauss(mu - sigma**2 / 2, sigma)
            r = math.exp(log_r) - 1
            # Real savings grow with inflation (assume saver maintains real purchasing power)
            savings_yr = annual_savings * (1 + inflation) ** yr
            W = W * (1 + r) + savings_yr
            path.append(W)

        retirement_navs.append(W)

        # Decumulation phase
        for yr in range(n_ret):
            log_r = rng.gauss(mu - sigma**2 / 2, sigma)
            r = math.exp(log_r) - 1
            # Spending grows with inflation throughout decumulation
            total_yr = n_acc + yr
            spend_yr = annual_spending * (1 + inflation) ** total_yr
            W = W * (1 + r) - spend_yr
            if W <= 0 and not bankrupt:
                bankrupt = True
                bankrupt_at = current_age + total_yr + 1
                W = 0
            path.append(W)

        paths.append(path)
        terminal_navs.append(W)
        bankrupt_age.append(bankrupt_at)

    # Percentile trajectories
    def pct_traj(p):
        # For each yr_index, find p-th percentile across sims
        n_pts = total_yrs + 1
        out = []
        for yr in range(n_pts):
            vals = sorted(path[yr] for path in paths)
            idx = min(int(p * len(vals)), len(vals) - 1)
            out.append(vals[idx])
        return out

    p10 = pct_traj(0.10)
    p25 = pct_traj(0.25)
    p50 = pct_traj(0.50)
    p75 = pct_traj(0.75)
    p90 = pct_traj(0.90)

    n_success = sum(1 for ba in bankrupt_age if ba == -1)
    prob_success = n_success / n_sims

    term_sorted = sorted(terminal_navs)
    term_p10 = term_sorted[int(0.10 * n_sims)]
    term_p50 = term_sorted[int(0.50 * n_sims)]
    term_p90 = term_sorted[int(0.90 * n_sims)]

    # Sequence-of-returns risk: the worst 10% of paths AT retirement start.
    # These are the people who happened to retire into a crash.
    # Compute NAV trajectory through age 75 for this group.
    ret_idx_sorted = sorted(range(n_sims), key=lambda i: retirement_navs[i])
    sor_idx = ret_idx_sorted[: max(1, n_sims // 10)]  # worst 10% at retirement
    if n_ret > 0 and sor_idx:
        sor_paths = [paths[i] for i in sor_idx]
        sor_p50 = []
        for yr in range(total_yrs + 1):
            vals = sorted(p[yr] for p in sor_paths)
            sor_p50.append(vals[len(vals) // 2])
    else:
        sor_p50 = []

    # Bankruptcy age distribution (for those who do go bankrupt)
    bankrupt_ages_only = [ba for ba in bankrupt_age if ba != -1]
    median_bankrupt_age = (
        sorted(bankrupt_ages_only)[len(bankrupt_ages_only) // 2]
        if bankrupt_ages_only else None
    )

    return {
        "n_sims": n_sims,
        "prob_success": round(prob_success, 4),
        "trajectory_p10": [round(v) for v in p10],
        "trajectory_p25": [round(v) for v in p25],
        "trajectory_p50": [round(v) for v in p50],
        "trajectory_p75": [round(v) for v in p75],
        "trajectory_p90": [round(v) for v in p90],
        "sor_p50": [round(v) for v in sor_p50],
        "terminal_nav_p10": round(term_p10),
        "terminal_nav_p50": round(term_p50),
        "terminal_nav_p90": round(term_p90),
        "n_years_accumulation": n_acc,
        "n_years_decumulation": n_ret,
        "median_bankrupt_age": median_bankrupt_age,
        "n_bankrupt": len(bankrupt_ages_only),
    }


def required_savings_bisection(
    initial_nav, current_savings, annual_spending,
    current_age, retire_age, end_age,
    E_p_pct, sigma_p_pct, inflation,
    target_prob=0.90, sub_sims=1500, max_iter=12,
):
    """
    Bisection: find the annual_savings such that prob_success ≥ target_prob.
    Uses a smaller MC (n=1500) per iteration for speed.
    """
    lo, hi = 0.0, max(current_savings * 5, 300_000.0)
    # First check: is the upper bound enough?
    hi_mc = monte_carlo(
        initial_nav, hi, annual_spending,
        current_age, retire_age, end_age,
        E_p_pct, sigma_p_pct, inflation, n_sims=sub_sims, seed=42,
    )
    if hi_mc["prob_success"] < target_prob:
        return {"feasible": False, "max_savings_tested": hi, "max_prob": hi_mc["prob_success"]}

    for _ in range(max_iter):
        mid = (lo + hi) / 2
        mc = monte_carlo(
            initial_nav, mid, annual_spending,
            current_age, retire_age, end_age,
            E_p_pct, sigma_p_pct, inflation, n_sims=sub_sims, seed=42,
        )
        if mc["prob_success"] < target_prob:
            lo = mid
        else:
            hi = mid

    return {"feasible": True, "required_annual_savings": round(hi)}


# ─────────────────────────────────────────────────────────────────────
# DRIVER
# ─────────────────────────────────────────────────────────────────────

def fmt_money(v):
    if v is None:
        return None
    return f"${round(v):,}"


def build_verdict(inputs, mc, opt):
    """Plain-English verdict."""
    prob = mc["prob_success"]
    age = inputs["age"]
    retire = inputs["retire_age"]
    saving = inputs["annual_savings"]

    if prob >= 0.95:
        status = "AHEAD"
        color = "#10b981"
        msg = (
            f"You're significantly ahead of schedule. With current savings of {fmt_money(saving)}/yr, "
            f"there's a {round(prob*100)}% chance you'll fund retirement spending of "
            f"{fmt_money(inputs['annual_spending'])}/yr through age {inputs['end_age']}. "
            f"Median terminal NAV: {fmt_money(mc['terminal_nav_p50'])}."
        )
    elif prob >= 0.80:
        status = "ON_TRACK"
        color = "#22d3ee"
        msg = (
            f"You're on track. {round(prob*100)}% probability of success at current pace. "
            f"Median NAV at age {retire}: {fmt_money(mc['trajectory_p50'][retire - age])}. "
            f"Terminal NAV at {inputs['end_age']}: median {fmt_money(mc['terminal_nav_p50'])}, "
            f"bear-case (p10) {fmt_money(mc['terminal_nav_p10'])}."
        )
    elif prob >= 0.60:
        status = "AT_RISK"
        color = "#fbbf24"
        if opt.get("feasible"):
            delta = opt["required_annual_savings"] - saving
            monthly_extra = round(delta / 12)
            msg = (
                f"You're at risk. Current probability of success: {round(prob*100)}%. "
                f"Raising savings to {fmt_money(opt['required_annual_savings'])}/yr "
                f"(adds {fmt_money(monthly_extra)}/mo) gets you to 90% confidence."
            )
        else:
            msg = (
                f"You're at risk. Probability {round(prob*100)}%. The target may require "
                f"either delaying retirement, reducing target spending, or increasing risk tolerance."
            )
    else:
        status = "OFF_TRACK"
        color = "#ef4444"
        if opt.get("feasible"):
            delta = opt["required_annual_savings"] - saving
            monthly_extra = round(delta / 12)
            msg = (
                f"You're off track. Only a {round(prob*100)}% chance of success at current pace. "
                f"Required: {fmt_money(opt['required_annual_savings'])}/yr "
                f"(+{fmt_money(monthly_extra)}/mo). Alternatively delay retirement by 3-5 years "
                f"or reduce target spending by 15-25%."
            )
        else:
            msg = (
                f"Significantly off track ({round(prob*100)}% success). The plan as stated isn't "
                f"feasible — recommend reducing target spending, delaying retirement, or "
                f"increasing risk tolerance (which raises both expected return and volatility)."
            )
    return {"status": status, "color": color, "message": msg}


def parse_inputs(event):
    """Parse from queryStringParameters or body."""
    qs = (event or {}).get("queryStringParameters") or {}
    body = (event or {}).get("body")
    if body:
        try:
            body = json.loads(body)
            qs = {**body, **qs}
        except Exception:
            pass

    def f(k, d):
        v = qs.get(k)
        if v in (None, ""):
            return d
        try:
            return float(v)
        except Exception:
            return d

    def s(k, d):
        v = qs.get(k)
        return v if v else d

    return {
        "current_nav": f("current_nav", 100_000),
        "age": int(f("age", 35)),
        "retire_age": int(f("retire_age", 65)),
        "annual_savings": f("annual_savings", 24_000),
        "annual_spending": f("annual_spending", 80_000),
        "end_age": int(f("end_age", 95)),
        "inflation": f("inflation", 0.025),
        "risk_profile": s("risk_profile", "moderate"),
        "n_sims": int(f("n_sims", 10_000)),
    }


def cors_response(status, body):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
            "Cache-Control": "no-cache",
        },
        "body": json.dumps(body, default=str),
    }


def lambda_handler(event=None, context=None):
    started = time.time()
    method = (event or {}).get("requestContext", {}).get("http", {}).get("method", "GET")
    if method == "OPTIONS":
        return cors_response(200, {"ok": True})

    inputs = parse_inputs(event)
    print(f"[wealth-plan] v{VERSION} inputs={inputs}")

    # 1. Load Capital Compass forward ERs
    compass = load_compass()
    if not compass:
        return cors_response(500, {"error": "Capital Compass data unavailable. Try again shortly."})

    # 2. Resolve weights from profile (lifecycle = age-aware)
    if inputs["risk_profile"] == "lifecycle":
        weights = lifecycle_weights(inputs["age"], inputs["retire_age"])
        profile_label = "Lifecycle — equity glides down as you approach retirement"
        profile_explainer = (
            "Vanguard target-date-fund-style glide path. Equity weight ~ 110 minus age, "
            "with steeper de-risking inside 10 years of retirement to manage "
            "sequence-of-returns risk."
        )
    elif inputs["risk_profile"] in RISK_PROFILES:
        p = RISK_PROFILES[inputs["risk_profile"]]
        weights = p["weights"]
        profile_label = p["label"]
        profile_explainer = p["explainer"]
    else:
        p = RISK_PROFILES["moderate"]
        weights = p["weights"]
        profile_label = p["label"]
        profile_explainer = p["explainer"]

    # 3. Portfolio moments
    E_p, sigma_p, real_E_p = portfolio_moments(weights, compass)

    # 4. Monte Carlo
    mc = monte_carlo(
        inputs["current_nav"], inputs["annual_savings"], inputs["annual_spending"],
        inputs["age"], inputs["retire_age"], inputs["end_age"],
        E_p, sigma_p, inputs["inflation"], n_sims=inputs["n_sims"],
        seed=42,
    )

    # 5. Required savings optimization (only if current is below 90% target)
    opt = {}
    if mc["prob_success"] < 0.90:
        opt = required_savings_bisection(
            inputs["current_nav"], inputs["annual_savings"], inputs["annual_spending"],
            inputs["age"], inputs["retire_age"], inputs["end_age"],
            E_p, sigma_p, inputs["inflation"],
            target_prob=0.90,
        )

    # 6. Inflation-deflated terminal NAV (today's $$)
    inflation_factor = (1 + inputs["inflation"]) ** (inputs["end_age"] - inputs["age"])
    terminal_real = {
        "p10_today_dollars": round(mc["terminal_nav_p10"] / inflation_factor),
        "p50_today_dollars": round(mc["terminal_nav_p50"] / inflation_factor),
        "p90_today_dollars": round(mc["terminal_nav_p90"] / inflation_factor),
        "spending_today_dollars": round(inputs["annual_spending"]),
        "spending_at_retirement_nominal": round(
            inputs["annual_spending"] * (1 + inputs["inflation"]) ** (inputs["retire_age"] - inputs["age"])
        ),
    }

    # 7. Sensitivity scenarios — what does pulling each lever do?
    base_inputs = dict(inputs)
    sensitivities = {}
    for label, mod in [
        ("retire_5_yrs_later", {"retire_age": min(inputs["retire_age"] + 5, 75)}),
        ("save_20pct_more", {"annual_savings": inputs["annual_savings"] * 1.20}),
        ("spend_20pct_less", {"annual_spending": inputs["annual_spending"] * 0.80}),
    ]:
        scen_inputs = {**base_inputs, **mod}
        scen_mc = monte_carlo(
            scen_inputs["current_nav"], scen_inputs["annual_savings"], scen_inputs["annual_spending"],
            scen_inputs["age"], scen_inputs["retire_age"], scen_inputs["end_age"],
            E_p, sigma_p, scen_inputs["inflation"], n_sims=1500, seed=42,
        )
        sensitivities[label] = {
            "prob_success": scen_mc["prob_success"],
            "terminal_nav_p50": scen_mc["terminal_nav_p50"],
            "delta_pp_success": round((scen_mc["prob_success"] - mc["prob_success"]) * 100, 1),
            "modification": mod,
        }

    # 8. Benchmark portfolio comparison: show what conservative/moderate/aggressive
    # would each look like for this exact saver
    benchmarks = {}
    for prof_key in ["conservative", "moderate", "aggressive"]:
        if prof_key == inputs["risk_profile"]:
            continue
        bp = RISK_PROFILES[prof_key]
        bE, bS, _ = portfolio_moments(bp["weights"], compass)
        bench_mc = monte_carlo(
            inputs["current_nav"], inputs["annual_savings"], inputs["annual_spending"],
            inputs["age"], inputs["retire_age"], inputs["end_age"],
            bE, bS, inputs["inflation"], n_sims=2000, seed=42,
        )
        benchmarks[prof_key] = {
            "label": bp["label"],
            "expected_return": bE, "vol": bS,
            "prob_success": bench_mc["prob_success"],
            "terminal_nav_p50": bench_mc["terminal_nav_p50"],
        }

    # 9. Verdict
    verdict = build_verdict(inputs, mc, opt)

    # 10. Glide path table (for lifecycle visualization)
    glide_table = {}
    for sample_age in range(max(25, inputs["age"]), min(75, inputs["retire_age"] + 5) + 1, 5):
        gw = lifecycle_weights(sample_age, inputs["retire_age"])
        equity_pct = round((gw["SPY"] + gw["EFA"] + gw["EEM"] + gw.get("VNQ", 0)) * 100)
        glide_table[sample_age] = {"equity_pct": equity_pct, "weights": {k: round(v, 3) for k, v in gw.items()}}

    elapsed = round(time.time() - started, 2)

    result = {
        "version": VERSION,
        "engine": "justhodl-wealth-plan",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": elapsed,
        "inputs": inputs,
        "allocation": {
            "profile_label": profile_label,
            "profile_explainer": profile_explainer,
            "weights": {k: round(v, 4) for k, v in weights.items() if v > 0},
            "expected_return_pct": E_p,
            "volatility_pct": sigma_p,
            "real_expected_return_pct": real_E_p,
        },
        "monte_carlo": mc,
        "savings_optimization": opt,
        "in_todays_dollars": terminal_real,
        "sensitivities": sensitivities,
        "benchmarks": benchmarks,
        "glide_path_reference": glide_table,
        "verdict": verdict,
        "compass_generated_at": compass.get("generated_at"),
        "methodology": (
            "10,000-sim lognormal Monte Carlo. Portfolio moments via Markowitz w'Σw "
            "with 30y institutional consensus correlation matrix. Forward expected "
            "returns from live Capital Compass (Bogle Sources-of-Return for equities, "
            "YTM for bonds, Erb-Harvey for gold). Required savings via bisection on "
            "1500-sim subordinate runs. Lifecycle glide path: equity ≈ 110-age with "
            "extra 1.5pp/yr de-risk inside 10 years of retirement (Vanguard TDF style)."
        ),
    }

    # Cache the latest snapshot for reference (overwrites each call)
    try:
        s3.put_object(
            Bucket=BUCKET, Key=OUT_KEY,
            Body=json.dumps(result, default=str, indent=2).encode(),
            ContentType="application/json",
            CacheControl="no-cache",
        )
    except Exception as e:
        print(f"[s3 snapshot] {e}")

    return cors_response(200, result)
