"""Provider-reported sovereign observations with complete original retention.

Legacy nominal-yield/CDS heuristics remain for compatibility, explicitly without
validated source definitions, quote clocks, ranking or portfolio authority.
Publishes data/global-sovereign.json and preserves the complete daily history.
"""
import json
import math
import sovereign_history
import sovereign_sources
import re
import time
import urllib.request
from datetime import datetime, timezone, timedelta

import boto3

VERSION = "1.4.2"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/global-sovereign.json"
HIST_KEY = "data/global-sovereign-history.json"

WGB_ENDPOINT = "https://www.worldgovernmentbonds.com/wp-json/country/v1/main"
WGB_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")

s3 = boto3.client("s3", region_name="us-east-1")

# display name -> (WGB slug, region). Verified 45/45 live.
COUNTRIES = {
    "United States": ("united-states", "North America"),
    "Canada": ("canada", "North America"),
    "Mexico": ("mexico", "North America"),
    "Germany": ("germany", "Europe"),
    "France": ("france", "Europe"),
    "Italy": ("italy", "Europe"),
    "Spain": ("spain", "Europe"),
    "United Kingdom": ("united-kingdom", "Europe"),
    "Netherlands": ("netherlands", "Europe"),
    "Belgium": ("belgium", "Europe"),
    "Austria": ("austria", "Europe"),
    "Portugal": ("portugal", "Europe"),
    "Greece": ("greece", "Europe"),
    "Ireland": ("ireland", "Europe"),
    "Finland": ("finland", "Europe"),
    "Sweden": ("sweden", "Europe"),
    "Norway": ("norway", "Europe"),
    "Denmark": ("denmark", "Europe"),
    "Switzerland": ("switzerland", "Europe"),
    "Poland": ("poland", "Europe"),
    "Czech Republic": ("czech-republic", "Europe"),
    "Hungary": ("hungary", "Europe"),
    "Russia": ("russia", "Europe"),
    "Turkey": ("turkey", "Europe"),
    "Japan": ("japan", "Asia-Pacific"),
    "China": ("china", "Asia-Pacific"),
    "India": ("india", "Asia-Pacific"),
    "Indonesia": ("indonesia", "Asia-Pacific"),
    "Malaysia": ("malaysia", "Asia-Pacific"),
    "Thailand": ("thailand", "Asia-Pacific"),
    "Philippines": ("philippines", "Asia-Pacific"),
    "Vietnam": ("vietnam", "Asia-Pacific"),
    "South Korea": ("south-korea", "Asia-Pacific"),
    "Singapore": ("singapore", "Asia-Pacific"),
    "Hong Kong": ("hong-kong", "Asia-Pacific"),
    "Taiwan": ("taiwan", "Asia-Pacific"),
    "Australia": ("australia", "Asia-Pacific"),
    "New Zealand": ("new-zealand", "Asia-Pacific"),
    "Brazil": ("brazil", "Latin America"),
    "Chile": ("chile", "Latin America"),
    "Colombia": ("colombia", "Latin America"),
    "Peru": ("peru", "Latin America"),
    "South Africa": ("south-africa", "Middle East & Africa"),
    "Israel": ("israel", "Middle East & Africa"),
    "Saudi Arabia": ("saudi-arabia", "Middle East & Africa"),
}


def wgb_country(slug, capture):
    """Return reported fields only after retaining complete provider responses."""
    return capture.country(slug)


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def stress_score(cds, spread, y):
    """0-100 sovereign stress. CDS-weighted (direct default pricing is the best gauge),
    then spread-vs-Bund, then absolute yield. Falls back gracefully when CDS absent."""
    cds_s = clamp((cds / 250.0) * 90.0, 0, 100) if cds is not None else None
    spr_s = clamp(30.0 + (spread / 150.0) * 50.0, 0, 100) if spread is not None else None
    yld_s = clamp(5.0 + (y / 12.0) * 80.0, 0, 100) if y is not None else None
    parts = [(cds_s, 0.55), (spr_s, 0.25), (yld_s, 0.20)]
    live = [(s, w) for s, w in parts if s is not None]
    if not live:
        return None
    return round(sum(s * w for s, w in live) / sum(w for _, w in live), 1)


def regime_from(score):
    if score is None:
        return "N/A"
    if score >= 70:
        return "DISTRESS"
    if score >= 50:
        return "STRESS"
    if score >= 35:
        return "ELEVATED"
    if score >= 20:
        return "NORMAL"
    return "CALM"


def lambda_handler(event=None, context=None):
    t0 = time.time()
    prior = sovereign_history.begin(s3, S3_BUCKET, datetime.now(timezone.utc).isoformat())
    acquisition = sovereign_sources.Capture(s3, S3_BUCKET, COUNTRIES, WGB_UA)
    next_history = None
    rows = []
    errors = []
    for name, (slug, region) in COUNTRIES.items():
        d = wgb_country(slug, acquisition)
        if not d or d.get("bond10y_pct") is None:
            errors.append(name)
            continue
        score = stress_score(d.get("cds_bp"), d.get("spread_vs_bund_bp"), d.get("bond10y_pct"))
        rows.append({
            "country": name, "region": region,
            "yield_10y_pct": d.get("bond10y_pct"),
            "cds_bp": d.get("cds_bp"),
            "cds_default_prob_pct": d.get("cds_default_prob_pct"),
            "spread_vs_bund_bp": d.get("spread_vs_bund_bp"),
            "rating": d.get("rating"),
            "cb_rate_pct": d.get("cb_rate_pct"),
            "stress_0_100": score,
            "regime": regime_from(score),
            "as_of": d.get("as_of"),
        })
        time.sleep(0.3)  # be polite to the source

    rows.sort(key=lambda r: (r["stress_0_100"] is None, -(r["stress_0_100"] or 0)))

    # regional aggregates (mean stress, mean CDS)
    regions = {}
    for r in rows:
        reg = r["region"]
        regions.setdefault(reg, {"stress": [], "cds": []})
        if r["stress_0_100"] is not None:
            regions[reg]["stress"].append(r["stress_0_100"])
        if r["cds_bp"] is not None:
            regions[reg]["cds"].append(r["cds_bp"])
    region_agg = []
    for reg, v in regions.items():
        region_agg.append({
            "region": reg,
            "avg_stress": round(sum(v["stress"]) / len(v["stress"]), 1) if v["stress"] else None,
            "avg_cds_bp": round(sum(v["cds"]) / len(v["cds"]), 1) if v["cds"] else None,
            "n": len(v["stress"]),
        })
    region_agg.sort(key=lambda x: -(x["avg_stress"] or 0))

    with_cds = [r for r in rows if r["cds_bp"] is not None]
    scored = [r for r in rows if r["stress_0_100"] is not None]

    # ── EURODOLLAR-HUB FUNDING STRESS — the offshore-USD system's core funding centers.
    # NOT all sovereign risk: only the jurisdictions where eurodollar (offshore USD) funding
    # concentrates — major USD borrowers (US/Japan/EU core) + offshore centers (London/HK/SG)
    # + euro-system incl periphery. Designed to SNIFF DANGER FIRST: stress in this system
    # shows up as one or two hubs breaking away from the calm pack (France/Italy 2011,
    # Switzerland 2023), so the composite is MAX-AWARE — it blends the CDS-weighted average
    # with the single most-stressed hub, so a lone canary lights it up rather than being
    # diluted by a calm Germany.
    EURODOLLAR_HUBS = {
        "United States", "United Kingdom", "Germany", "France", "Italy", "Spain",
        "Switzerland", "Netherlands", "Belgium", "Ireland", "Finland",
        "Greece", "Portugal", "Sweden", "Japan", "Hong Kong", "Singapore",
        "South Korea", "Taiwan", "Canada", "Australia",
        # USD-dependent economies at the periphery of the eurodollar system — they crack
        # EARLY when dollar funding tightens (commodity + heavy USD-funding exposure), so
        # they act as leading canaries for global financing stress.
        "Chile", "Peru",
    }
    hubs = [r for r in rows if r["country"] in EURODOLLAR_HUBS]
    hub_cds = [(r["country"], r["cds_bp"]) for r in hubs if r["cds_bp"] is not None]

    def cds_to_stress(bp):
        # 5bp→0, 20bp→~25, 40bp→~57, 60bp→~82, 80bp+→~95. Calm hubs sit <25bp.
        return round(clamp((bp - 5.0) / 75.0 * 100.0, 0, 100), 1)

    eurodollar_hub_stress = None
    hub_detail = []
    worst_hub = None
    if hub_cds:
        avg_bp = sum(c for _, c in hub_cds) / len(hub_cds)
        avg_stress = cds_to_stress(avg_bp)
        # worst hub (the canary)
        wname, wbp = max(hub_cds, key=lambda x: x[1])
        worst_stress = cds_to_stress(wbp)
        worst_hub = {"country": wname, "cds_bp": round(wbp, 1), "stress": worst_stress}
        # dispersion: how far the worst is above the pack (danger builds as pack fractures)
        # DANGER-FIRST composite: 60% pack average + 40% worst hub → a lone spike still moves it
        eurodollar_hub_stress = round(0.60 * avg_stress + 0.40 * worst_stress, 1)
        hub_detail = sorted(
            [{"country": c, "cds_bp": round(b, 1), "stress": cds_to_stress(b)} for c, b in hub_cds],
            key=lambda x: -x["stress"])

    source_evidence = acquisition.finish()
    payload = {
        "source_evidence": source_evidence,
        "version": VERSION, "ok": bool(rows),
        "calls_eligible": False, "sizing_eligible": False, "execution_eligible": False, "forecast_qualified": False,
        "decision": {"verb": "WAIT", "meaning": "abstain", "reason": "Provider quote definitions and observation clocks, sovereign rankings and funding interpretations remain unqualified."},
        "quality": {"status": "unverified", "scope": "Legacy provider-reported quotes and heuristic review scores; not original-source replay or validated forecasts."},
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 1),
        "n_countries": len(rows),
        "n_errors": len(errors),
        "errors": errors,
        "global_avg_stress": round(sum(r["stress_0_100"] for r in scored) / len(scored), 1) if scored else None,
        "global_avg_cds_bp": round(sum(r["cds_bp"] for r in with_cds) / len(with_cds), 1) if with_cds else None,
        "eurodollar_hub_stress_0_100": eurodollar_hub_stress,
        "eurodollar_hub_avg_cds_bp": round(sum(c for _, c in hub_cds) / len(hub_cds), 1) if hub_cds else None,
        "eurodollar_hub_worst": worst_hub,
        "eurodollar_hub_n": len(hub_cds),
        "eurodollar_hub_detail": hub_detail,
        "highest_stress": scored[0] if scored else None,
        "lowest_stress": scored[-1] if scored else None,
        "highest_cds": max(with_cds, key=lambda r: r["cds_bp"]) if with_cds else None,
        "countries": rows,
        "regions": region_agg,
        "source": "World Government Bonds (worldgovernmentbonds.com) — provider-reported 10Y yield, CDS, spread field (benchmark unverified), rating and central-bank rate.",
    }

    # ── HISTORICAL SNAPSHOTTING — accumulate a daily time-series of the barometer so it
    # gains trend + percentile context (WGB only gives current values; we build our own
    # history). Append today's reading (deduped to latest-per-day), preserve every stored date, then
    # compute where today sits vs its own history.
    if eurodollar_hub_stress is not None:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        hist = list(prior['history']['doc']) if prior['history'] else []
        snap = {
            "date": today,
            "stress": eurodollar_hub_stress,
            "avg_cds_bp": payload["eurodollar_hub_avg_cds_bp"],
            "worst_country": (worst_hub or {}).get("country"),
            "worst_cds_bp": (worst_hub or {}).get("cds_bp"),
        }
        hist = [h for h in hist if h.get("date") != today]  # dedupe → latest per day
        hist.append(snap)
        hist.sort(key=lambda h: h["date"])
        next_history = hist

        # percentile of today's reading within its own history + short-window trend
        series = [h["stress"] for h in hist if h.get("stress") is not None]
        if len(series) >= 3:
            below = sum(1 for v in series if v <= eurodollar_hub_stress)
            payload["eurodollar_hub_percentile"] = round(below / len(series) * 100.0, 1)
        payload["eurodollar_hub_history_n"] = len(hist)
        payload["eurodollar_hub_history"] = hist  # retain every stored daily point
        # Exact calendar-date comparisons; a daily snapshot is not two observations/day.
        def ago(days):
            target = (datetime.fromisoformat(today).date() - timedelta(days=days)).isoformat()
            row = next((h for h in hist if h["date"] == target), {})
            value = row.get("stress")
            return value if type(value) in (int, float) and math.isfinite(value) else None
        prev7, prev30 = ago(7), ago(30)
        payload["history_comparison_basis"] = "Exact UTC snapshot calendar dates; missing prior dates remain unavailable. These are unqualified heuristic index points."
        payload["eurodollar_hub_chg_7d"] = None
        payload["eurodollar_hub_chg_30d"] = None
        if prev7 is not None:
            payload["eurodollar_hub_chg_7d"] = round(eurodollar_hub_stress - prev7, 1)
        if prev30 is not None:
            payload["eurodollar_hub_chg_30d"] = round(eurodollar_hub_stress - prev30, 1)

    publication = sovereign_history.publish(s3, S3_BUCKET, prior, payload, next_history)
    return {"statusCode": 200, "body": json.dumps({
        "ok": payload["ok"], "n": len(rows), "errors": len(errors), "publication_attempt": publication,
        "global_avg_cds": payload["global_avg_cds_bp"],
        "highest_stress": (payload["highest_stress"] or {}).get("country"),
        "elapsed_s": payload["elapsed_s"],
    })}


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
