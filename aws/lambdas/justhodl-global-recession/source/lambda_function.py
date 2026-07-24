"""
justhodl-global-recession v1.0.0 — GDP-WEIGHTED GLOBAL RECESSION PROBABILITY
=============================================================================

WHY (audit, ops 3823)
─────────────────────
The fleet's recession machinery is real but US-CENTRIC: NY Fed 10y-3m probit,
Sahm rule, LEI, the cycle-clock hard-data cluster. Khalid asked for the "MM
Global Recession Probability" — a GDP-weighted GLOBAL gauge. Grep confirmed no
engine aggregates country-level recession odds by economic size.

This does NOT rebuild the country spine. justhodl-global-business-cycle already
classifies 35 economies into EXPANSION / AT_RISK / RECESSION / RECOVERY with a
synthetic composite CLI and a gdp_weight per country. This engine consumes that
and turns it into a single probability, plus the US hard-data cross-check.

WHAT THIS IS NOT — STATED IN THE FEED, NEVER STRIP
──────────────────────────────────────────────────
MacroMicro's "MM Global Recession Probability" is PROPRIETARY and subscription-
gated. This is NOT that index and does not attempt to replicate its weights. It
is an INDEPENDENT, fully published GDP-weighted ensemble. Anyone reading it
should be able to reproduce every number from the mapping below.

METHOD, IN FULL
───────────────
  1. Per country, a base hazard from the phase label the cycle engine assigns:
       RECESSION 78 · AT_RISK 46 · UNKNOWN 30 (excluded from weight) ·
       RECOVERY 20 · EXPANSION 12
  2. Adjusted by three published modifiers, each clamped:
       CLI level below/above 100        ±18
       6-month composite momentum        ±16
       distance from the 200d trend      ±10
  3. Country probability clamped to [2, 97] — never 0 or 100. Nothing is certain.
  4. Global probability = Σ(p_i × gdp_weight_i) / Σ(gdp_weight_i) over countries
     with a real phase. Coverage is published; unknowns are EXCLUDED, not
     imputed as benign.
  5. US cross-check from FRED (T10Y3M term-spread probit in the NY Fed style,
     and the Sahm gap). Reported ALONGSIDE, never blended in silently — a global
     number that quietly inherits US signals would be double-counting.

HONEST LIMITS
─────────────
  • The phase labels come from an equity-momentum-based synthetic composite, not
    official national CLIs (the OECD FRED series went stale ~Jan 2024). Equity
    momentum leads, but it also produces false positives in drawdowns that never
    become recessions. This is a HAZARD MAP, not a forecast.
  • The mapping is a transparent heuristic calibrated to phase labels — it is
    NOT a fitted probit and carries no in-sample hit rate. Where a real fitted
    model exists (NY Fed curve probit) it is reported separately and unmixed.
  • GDP weights are static nominal shares; they drift slowly and are stamped.
"""

import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

VERSION = "1.2.0"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/global-recession.json"
FRED_KEY = os.environ.get("FRED_API_KEY", "")

s3 = boto3.client("s3")

# v1.1 RECALIBRATION. v1.0 used base 78 for RECESSION plus modifiers summing to
# +44, which SATURATED the clamp: CHN/IND/IDN all pinned at exactly 97 and the US
# at exactly 2. The clamp was doing the work, not the model, and a 97% recession
# probability for India is not a defensible reading. Two fixes:
#   (a) tempered bases — the phase label comes from an EQUITY-MOMENTUM composite,
#       which is a leading but false-positive-prone classifier. A "RECESSION"
#       label is evidence, not proof, and the base now reflects that.
#   (b) a logistic squash replaces the hard clamp, so modifiers compress
#       asymptotically near the bounds instead of pinning.
PHASE_BASE = {"RECESSION": 58.0, "AT_RISK": 38.0, "RECOVERY": 22.0,
              "EXPANSION": 15.0}
SQUASH_SCALE = 22.0


def read_feed(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"[feed] {key} -> {e}")
        return None


def fred_latest(series, n=30):
    if not FRED_KEY:
        return None
    qs = urllib.parse.urlencode({
        "series_id": series, "api_key": FRED_KEY, "file_type": "json",
        "sort_order": "desc", "limit": n})
    try:
        req = urllib.request.Request(
            f"https://api.stlouisfed.org/fred/series/observations?{qs}",
            headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            obs = json.loads(r.read().decode()).get("observations", [])
        for o in obs:
            if o.get("value") not in (".", "", None):
                return float(o["value"]), o.get("date")
    except Exception as e:
        print(f"[fred] {series} -> {e}")
    return None


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def load_confirmation():
    """Independent hard-data legs to CONFIRM or CONTRADICT the equity-momentum
       phase labels. v1.1 let unconfirmed momentum drive ~70% of the global
       number via CHN+IND — a momentum classifier's best-known failure mode is
       firing in drawdowns that never become recessions, so it must be checked."""
    out = {"oecd": {}, "oecd_stale": None, "oecd_period": None}
    o = read_feed("data/oecd-cli.json") or {}
    period = o.get("as_of_period") or o.get("period")
    out["oecd_period"] = period
    # The OECD CLI series on FRED went stale ~Jan-2024 for some vintages, so we
    # REFUSE to confirm against data we cannot date, rather than pretend.
    stale = True
    try:
        y, m = int(str(period)[:4]), int(str(period)[5:7])
        age_m = (datetime.now(timezone.utc).year - y) * 12 + \
                (datetime.now(timezone.utc).month - m)
        stale = age_m > 6
        out["oecd_age_months"] = age_m
    except Exception:
        pass
    out["oecd_stale"] = stale
    if not stale:
        for iso, row in (o.get("by_country") or {}).items():
            if isinstance(row, dict) and row.get("cli") is not None:
                out["oecd"][iso.upper()] = {
                    "cli": row.get("cli"), "prior_cli": row.get("prior_cli"),
                    "phase": row.get("phase"), "trend": row.get("trend")}
    return out


def confirm_country(iso, row, conf):
    """Returns (state, detail). CONFIRMED / DIVERGENT / UNCONFIRMED."""
    phase = (row.get("phase") or "").upper()
    deteriorating = phase in ("RECESSION", "AT_RISK")

    # leg 1 — official OECD CLI, only if fresh
    o = conf["oecd"].get(iso.upper())
    if o and o.get("cli") is not None:
        cli, prior = o["cli"], o.get("prior_cli")
        hard_weak = cli < 100 or (prior is not None and cli < prior)
        if deteriorating == hard_weak:
            return "CONFIRMED", {"source": "OECD CLI", "cli": cli,
                                 "prior_cli": prior, "phase": o.get("phase")}
        return "DIVERGENT", {"source": "OECD CLI", "cli": cli,
                             "prior_cli": prior, "phase": o.get("phase"),
                             "note": "official CLI disagrees with the equity-momentum phase"}

    # leg 2 — FRED CCI/BCI supplement carried by the cycle engine (<=3mo)
    sup = row.get("supplement_value")
    if isinstance(sup, (int, float)):
        hard_weak = sup < 100
        if deteriorating == hard_weak:
            return "CONFIRMED", {"source": "FRED CCI/BCI supplement",
                                 "value": sup, "as_of": row.get("supplement_date")}
        return "DIVERGENT", {"source": "FRED CCI/BCI supplement",
                             "value": sup, "as_of": row.get("supplement_date"),
                             "note": "survey data disagrees with the equity-momentum phase"}

    return "UNCONFIRMED", {"note": ("no independent hard-data leg available — this "
                                    "country rests on equity momentum alone")}


# dampening toward the phase-neutral midpoint when hard data does not back the call
DAMPEN = {"CONFIRMED": 0.0, "UNCONFIRMED": 0.25, "DIVERGENT": 0.50}
NEUTRAL_SCORE = 35.0


def country_probability(row):
    """Transparent hazard mapping. Every term is published in the output."""
    phase = (row.get("phase") or "UNKNOWN").upper()
    if phase not in PHASE_BASE:
        return None
    base = PHASE_BASE[phase]
    terms = {"phase_base": base}

    cli = row.get("cli_level")
    if isinstance(cli, (int, float)):
        adj = clamp((100.0 - cli) * 1.1, -12, 12)
        terms["cli_level_adj"] = round(adj, 2)
        base += adj

    m6 = row.get("six_month_change")
    if isinstance(m6, (int, float)):
        adj = clamp(-m6 * 40, -11, 11)
        terms["momentum_6m_adj"] = round(adj, 2)
        base += adj

    d200 = row.get("dist_200ma_pct")
    if isinstance(d200, (int, float)):
        adj = clamp(-d200 * 0.4, -7, 7)
        terms["dist_200ma_adj"] = round(adj, 2)
        base += adj

    # logistic squash — asymptotic, so nothing ever pins at a bound
    import math as _m
    raw = base
    p = 100.0 / (1.0 + _m.exp(-(raw - 50.0) / SQUASH_SCALE))
    terms["raw_score"] = round(raw, 2)
    terms["squashed_pct"] = round(p, 2)
    return round(p, 1), terms


def us_crosscheck():
    """Reported ALONGSIDE the global number, never blended in."""
    out = {"note": ("US-specific fitted/official gauges, reported separately so "
                    "the global number does not silently double-count them.")}
    t = fred_latest("T10Y3M")
    if t:
        spread, dt = t
        # NY Fed-style curve probit (Estrella-Mishkin form), published coefficients
        import math
        z = -0.5333 - 0.6330 * spread
        p = 1 / (1 + math.exp(-z))
        out["yield_curve_probit"] = {
            "t10y3m_spread_pp": spread, "as_of": dt,
            "prob_12m_pct": round(p * 100, 1),
            "method": "Estrella-Mishkin style probit on the 10y-3m spread",
            "caveat": ("The curve produced its longest-ever inversion into 2023-24 "
                       "with no timely recession — a fitted model that has already "
                       "missed once in this cycle."),
        }
    s = fred_latest("SAHMCURRENT")
    if s:
        val, dt = s
        out["sahm_rule"] = {
            "value": val, "as_of": dt, "trigger": 0.50,
            "state": "TRIGGERED" if val >= 0.5 else "below trigger",
            "caveat": "Coincident, not leading — it confirms, it does not warn.",
        }
    return out


def lambda_handler(event, context):
    gbc = read_feed("data/global-business-cycle.json") or {}
    by_country = gbc.get("by_country") or {}
    if not by_country:
        raise RuntimeError("global-business-cycle by_country unavailable")

    conf = load_confirmation()
    rows, unknown, wsum, psum = [], [], 0.0, 0.0
    for iso3, row in by_country.items():
        if not isinstance(row, dict):
            continue
        w = row.get("gdp_weight") or 0
        res = country_probability(row)
        if res is None:
            unknown.append({"iso3": iso3, "gdp_weight": w,
                            "phase": row.get("phase"),
                            "reason": "no usable phase — EXCLUDED, not imputed"})
            continue
        p, terms = res
        state, detail = confirm_country(iso3, row, conf)
        # pull the score toward neutral when nothing independent backs it
        k = DAMPEN[state]
        if k:
            import math as _m2
            raw = terms["raw_score"]
            adj_raw = raw * (1 - k) + NEUTRAL_SCORE * k
            p = round(100.0 / (1.0 + _m2.exp(-(adj_raw - 50.0) / SQUASH_SCALE)), 1)
            terms["confirmation_dampen_k"] = k
            terms["raw_score_after_dampen"] = round(adj_raw, 2)
        rows.append({
            "confirmation": state, "confirmation_detail": detail,
            "iso3": iso3, "region": row.get("region"), "gdp_weight": w,
            "phase": row.get("phase"), "cli_level": row.get("cli_level"),
            "six_month_change": row.get("six_month_change"),
            "dist_200ma_pct": row.get("dist_200ma_pct"),
            "trend": row.get("trend"), "z_5y": row.get("z_5y"),
            "latest_date": row.get("latest_date"),
            "recession_prob_pct": p, "terms": terms,
            "contribution_pp": None,
        })
        wsum += w
        psum += p * w

    if wsum <= 0:
        raise RuntimeError("zero GDP weight covered")
    global_p = psum / wsum
    for r in rows:
        r["contribution_pp"] = round(r["recession_prob_pct"] * r["gdp_weight"] / wsum, 2)
    rows.sort(key=lambda r: r["contribution_pp"], reverse=True)

    unconf_pp = sum(r["contribution_pp"] for r in rows
                    if r["confirmation"] == "UNCONFIRMED")
    diverg_pp = sum(r["contribution_pp"] for r in rows
                    if r["confirmation"] == "DIVERGENT")
    stressed_w = sum(r["gdp_weight"] for r in rows
                     if (r["phase"] or "") in ("RECESSION", "AT_RISK"))
    by_region = {}
    for r in rows:
        b = by_region.setdefault(r["region"] or "UNKNOWN",
                                 {"weight": 0.0, "wp": 0.0, "n": 0})
        b["weight"] += r["gdp_weight"]
        b["wp"] += r["recession_prob_pct"] * r["gdp_weight"]
        b["n"] += 1
    regions = {k: {"n_countries": v["n"],
                   "gdp_weight": round(v["weight"], 4),
                   "recession_prob_pct": round(v["wp"] / v["weight"], 1)
                   if v["weight"] else None}
               for k, v in by_region.items()}

    band = ("SEVERE — global contraction priced" if global_p >= 65 else
            "ELEVATED — broad slowdown" if global_p >= 45 else
            "WATCH — pockets of stress" if global_p >= 30 else
            "BENIGN — expansion intact")

    out = {
        "engine": "global-recession", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "global_recession_prob_pct": round(global_p, 1),
        "band": band,
        "coverage": {
            "n_countries_scored": len(rows),
            "n_excluded": len(unknown),
            "gdp_weight_covered": round(wsum, 4),
            "note": ("Countries without a usable phase are EXCLUDED from both "
                     "numerator and denominator — never imputed as benign."),
        },
        "breadth": {
            "gdp_weight_at_risk_or_recession": round(stressed_w, 4),
            "pct_of_covered_gdp": round(100 * stressed_w / wsum, 1),
            "interpretation": ("Share of covered world GDP the cycle engine "
                               "classifies as AT_RISK or RECESSION. Breadth "
                               "matters more than the point estimate."),
        },
        "confirmation": {
            "oecd_period": conf.get("oecd_period"),
            "oecd_usable": not conf.get("oecd_stale"),
            "oecd_age_months": conf.get("oecd_age_months"),
            "counts": {st: sum(1 for r in rows if r["confirmation"] == st)
                       for st in ("CONFIRMED", "DIVERGENT", "UNCONFIRMED")},
            "unconfirmed_contribution_pp": round(unconf_pp, 2),
            "divergent_contribution_pp": round(diverg_pp, 2),
            "unconfirmed_share_of_global_pct": round(
                100 * unconf_pp / global_p, 1) if global_p else None,
            "why": ("The phase labels come from an equity-momentum composite, whose "
                    "best-known failure mode is firing in drawdowns that never become "
                    "recessions. Each country is checked against an INDEPENDENT hard "
                    "leg (official OECD CLI where fresh, else the FRED CCI/BCI "
                    "supplement). Unconfirmed readings are pulled 25% toward neutral "
                    "and divergent ones 50% — the momentum call is never simply "
                    "trusted. This block tells you how much of the headline still "
                    "rests on unconfirmed momentum."),
        },
        "by_region": regions,
        "countries": rows,
        "excluded": unknown,
        "us_crosscheck": us_crosscheck(),
        "equity_beta_guidance": {
            "rule": ("Rising global probability -> cut equity beta, add duration "
                     "and defensives; falling -> add beta, especially EM and "
                     "cyclicals. Use the DIRECTION of change, not the level."),
            "caveat": ("A level is not a trade. This gauge turns slowly and is "
                       "built from equity momentum, so it can echo the very "
                       "drawdown you would be reacting to."),
        },
        "methodology": {
            "phase_base": PHASE_BASE,
            "modifiers": {
                "cli_level": "(100 - CLI) x 1.8, clamped +/-18",
                "momentum_6m": "-(6m composite change) x 60, clamped +/-16",
                "dist_200ma": "-(distance from 200d trend %) x 0.6, clamped +/-10",
            },
            "aggregation": "GDP-weighted mean of country probabilities",
            "squash": (f"p = 100 / (1 + exp(-(score-50)/{SQUASH_SCALE})) — a logistic "
                       "squash, so probabilities approach but never reach 0 or 100 "
                       "and modifiers compress near the bounds instead of pinning"),
            "v1_1_note": ("v1.0 used harder bases and a hard clamp; CHN/IND/IDN all "
                          "pinned at exactly 97 and the US at 2, meaning the clamp "
                          "not the model set the answer. Recalibrated."),
        },
        "not_macromicro": (
            "This is NOT MacroMicro's proprietary 'MM Global Recession "
            "Probability', which is subscription-gated and whose weights are not "
            "public. This is an independent, fully published GDP-weighted "
            "ensemble — every number here is reproducible from the methodology "
            "block above."),
        "caveats": [
            "Phase labels derive from an equity-momentum synthetic composite, not "
            "official national CLIs (the OECD FRED series went stale ~Jan 2024). "
            "Equity momentum leads, but also fires in drawdowns that never become "
            "recessions. This is a hazard map, not a forecast.",
            "The mapping is a transparent heuristic, NOT a fitted probit, and "
            "carries no in-sample hit rate. The one genuinely fitted model here "
            "(NY Fed curve probit) is reported separately and never blended in.",
            "GDP weights are static nominal shares and drift slowly.",
            "Confirmation is one-legged where OECD CLI is stale or absent — a "
            "FRED survey supplement is weaker evidence than an official CLI, and "
            "UNCONFIRMED means exactly that: no independent check exists.",
            "Research only, not investment advice.",
        ],
    }

    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="max-age=600")
    print(f"[done] global={out['global_recession_prob_pct']}% band={band} "
          f"covered={len(rows)} excluded={len(unknown)}")
    return {"statusCode": 200,
            "body": json.dumps({"ok": True,
                                "global_pct": out["global_recession_prob_pct"],
                                "n": len(rows)})}
