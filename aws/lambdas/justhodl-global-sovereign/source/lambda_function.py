"""
JUSTHODL GLOBAL SOVEREIGN DESK — worldwide sovereign bond & CDS intelligence.

Harvests live sovereign data for ~45 major economies from World Government Bonds' own REST
endpoint (/wp-json/country/v1/main), reverse-engineered from the site JS: 10-year yield,
sovereign CDS (5Y), CDS-implied default probability, spread-vs-Bund, credit rating, and
central-bank policy rate.

Builds a per-country sovereign-stress score (CDS-weighted — direct default pricing is the
best single gauge), ranks the world by credit risk, flags the most/least stressed, and
emits regional aggregates. Publishes data/global-sovereign.json.

OUTPUT: data/global-sovereign.json
"""
import json
import re
import time
import urllib.request
from datetime import datetime, timezone

import boto3

VERSION = "1.1.0"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/global-sovereign.json"

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


def _http(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": WGB_UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "ignore")


def wgb_country(slug):
    """Fetch live sovereign data from WGB's REST endpoint. Returns dict or None."""
    try:
        page = _http(f"https://www.worldgovernmentbonds.com/country/{slug}/")
    except Exception:
        return None
    m = re.search(r"var\s+jsGlobalVars\s*=\s*(\{.*?\});", page, re.S)
    if not m:
        return None
    raw, gv, depth = m.group(1), None, 0
    for i, ch in enumerate(raw):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                try:
                    gv = json.loads(raw[:i + 1])
                except Exception:
                    return None
                break
    if not gv:
        return None
    body = json.dumps({"GLOBALVAR": gv}).encode()
    req = urllib.request.Request(WGB_ENDPOINT, data=body, headers={
        "User-Agent": WGB_UA, "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Referer": f"https://www.worldgovernmentbonds.com/country/{slug}/",
        "Origin": "https://www.worldgovernmentbonds.com",
        "X-Requested-With": "XMLHttpRequest"})
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            d = json.loads(r.read().decode("utf-8", "ignore"))
    except Exception:
        return None
    if not d.get("success"):
        return None

    def num(k):
        v = d.get(k)
        try:
            return float(v) if v not in (None, "", "----") else None
        except (ValueError, TypeError):
            return None
    return {
        "bond10y_pct": num("bond10y"),
        "cds_bp": num("lastCds"),
        "cds_default_prob_pct": num("lastCdsDefaultProb"),
        "spread_vs_bund_bp": num("mainSpreadValue"),
        "rating": d.get("lastRatingValue"),
        "cb_rate_pct": num("cbRateNumber"),
        "as_of": d.get("lastDataValDesc"),
    }


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
    rows = []
    errors = []
    for name, (slug, region) in COUNTRIES.items():
        d = wgb_country(slug)
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

    # Core developed-market sovereign CDS — the systemic-risk signal. Normally quiet
    # (~10-25bp); a joint spike is a flight-from-quality tell that front-runs equity
    # drawdowns. This is what feeds the JSI (distinct from BTP-Bund yield-spread).
    CORE_DM = {"United States", "Germany", "France", "Italy", "Spain",
               "United Kingdom", "Japan", "Netherlands", "Belgium", "Canada"}
    core_cds_vals = [r["cds_bp"] for r in rows
                     if r["country"] in CORE_DM and r["cds_bp"] is not None]
    core_dm_cds_bp = round(sum(core_cds_vals) / len(core_cds_vals), 1) if core_cds_vals else None
    # map to 0-100 stress: 10bp→~15, 25bp→~40, 40bp→~70, 60bp+→~90 (systemic).
    core_dm_cds_stress = None
    if core_dm_cds_bp is not None:
        core_dm_cds_stress = round(clamp((core_dm_cds_bp - 5.0) / 55.0 * 90.0, 0, 100), 1)

    payload = {
        "version": VERSION, "ok": bool(rows),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 1),
        "n_countries": len(rows),
        "n_errors": len(errors),
        "errors": errors,
        "global_avg_stress": round(sum(r["stress_0_100"] for r in scored) / len(scored), 1) if scored else None,
        "global_avg_cds_bp": round(sum(r["cds_bp"] for r in with_cds) / len(with_cds), 1) if with_cds else None,
        "core_dm_cds_bp": core_dm_cds_bp,
        "core_dm_cds_stress_0_100": core_dm_cds_stress,
        "core_dm_cds_n": len(core_cds_vals),
        "highest_stress": scored[0] if scored else None,
        "lowest_stress": scored[-1] if scored else None,
        "highest_cds": max(with_cds, key=lambda r: r["cds_bp"]) if with_cds else None,
        "countries": rows,
        "regions": region_agg,
        "source": "World Government Bonds (worldgovernmentbonds.com) — live 10Y yield, sovereign CDS, spread-vs-Bund, rating, central-bank rate.",
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload, default=str).encode(),
                  ContentType="application/json", CacheControl="max-age=1800, public")
    return {"statusCode": 200, "body": json.dumps({
        "ok": payload["ok"], "n": len(rows), "errors": len(errors),
        "global_avg_cds": payload["global_avg_cds_bp"],
        "highest_stress": (payload["highest_stress"] or {}).get("country"),
        "elapsed_s": payload["elapsed_s"],
    })}


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
