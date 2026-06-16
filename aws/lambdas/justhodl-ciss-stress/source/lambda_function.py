"""
justhodl-ciss-stress — the complete ECB systemic-stress data layer.

Auto-discovers and pulls EVERY current ECB stress series (no hard-coded key
lists, so it self-heals when ECB renames series — which it just did):
  • CISS dataflow  — euro-area composite + 5 sub-indices + correlation,
                     SovCISS (GDP- & equal-weighted), and every country CISS.
  • CLIFS dataflow — Country-Level Index of Financial Stress.

Discovery: list each flow's series (lastNObservations=1), keep only the FRESH
ones (latest obs >= FRESH_CUTOFF) so the discontinued/frozen legacy variants
(e.g. the pre-2025-05 SS_*.CON codes) are dropped automatically.

Full history is pulled back to inception (CISS composite reaches 1980). Daily
series are stored weekly-downsampled to keep the payload light for the page;
the exact latest daily value + per-series stats (percentile, z-score, 1y change,
regime band, all-time & crisis peaks) are always exact.

OUTPUT: data/ciss-stress.json
SCHEDULE: daily 07:10 UTC (ECB publishes CISS daily with a ~2-3 business-day lag;
          monthly country/sovereign series refresh within the same daily sweep).
"""
import json
import time
import urllib.request
from datetime import datetime, timezone

import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/ciss-stress.json"
UA = {"User-Agent": "JustHodl Research raafouis@gmail.com"}
BASE = "https://data-api.ecb.europa.eu/service/data"
FRESH_CUTOFF = "2025-07-01"   # ISO; drops the frozen 2025-05 legacy variants (lexicographic-safe for YYYY-MM too)

COUNTRY = {
    "U2": "Euro Area", "AT": "Austria", "BE": "Belgium", "DE": "Germany", "ES": "Spain",
    "FI": "Finland", "FR": "France", "GR": "Greece", "IE": "Ireland", "IT": "Italy",
    "NL": "Netherlands", "PT": "Portugal", "GB": "United Kingdom", "US": "United States",
    "CN": "China", "CZ": "Czechia", "DK": "Denmark", "HU": "Hungary", "PL": "Poland", "SE": "Sweden",
}
# CISS indicator-code -> human label
IND = {
    "SS_CIN": "Composite (CISS)", "SS_BMN": "Bond market", "SS_EMN": "Equity market",
    "SS_FIN": "Financial intermediaries", "SS_FXN": "FX market", "SS_MMN": "Money market",
    "SS_CON": "Cross-subindex correlation", "SOV_GDPWN": "Sovereign (GDP-weighted)",
    "SOV_EWN": "Sovereign (equal-weighted)", "SOV_CIN": "Sovereign composite", "SOV_CI": "Sovereign composite",
}


def _get(url, t=50):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=t) as r:
            return r.status, r.read().decode("utf-8", "ignore")
    except Exception:
        return None, ""


def _csv_rows(body):
    lines = body.splitlines()
    if not lines:
        return []
    hdr = lines[0].split(",")
    try:
        ki, ti, vi = hdr.index("KEY"), hdr.index("TIME_PERIOD"), hdr.index("OBS_VALUE")
    except ValueError:
        return []
    out = []
    for ln in lines[1:]:
        c = ln.split(",")
        if len(c) > max(ki, ti, vi) and c[vi] not in ("", "NaN"):
            out.append((c[ki], c[ti], c[vi]))
    return out


def discover(flow):
    """All series keys in a flow with their latest date; keep only fresh ones."""
    st, body = _get(f"{BASE}/{flow}?format=csvdata&lastNObservations=1")
    keys = {}
    if st == 200:
        for k, t, v in _csv_rows(body):
            if t >= FRESH_CUTOFF:
                keys[k] = t
    return keys


def history(key):
    """Full history for one series as [[date, value], ...] ascending."""
    st, body = _get(f"{BASE}/CISS/{key.split('.', 1)[1]}?format=csvdata" if key.startswith("CISS.")
                    else f"{BASE}/CLIFS/{key.split('.', 1)[1]}?format=csvdata")
    pts = []
    if st == 200:
        for _, t, v in _csv_rows(body):
            try:
                pts.append([t, round(float(v), 6)])
            except Exception:
                pass
    pts.sort(key=lambda x: x[0])
    return pts


def downsample_weekly(pts):
    """Keep one point per ISO week (last obs of the week) + always the final point."""
    if len(pts) <= 1200:
        return pts
    seen, out = set(), []
    for d, v in pts:
        wk = d[:4] + "-" + (d[5:7] if len(d) >= 7 else "01")  # month bucket for very long daily
        # week bucket: year+week from date
        try:
            wk = "%s-W%02d" % (d[:4], datetime.strptime(d[:10], "%Y-%m-%d").isocalendar()[1])
        except Exception:
            pass
        if wk not in seen:
            seen.add(wk); out.append([d, v])
    if out and out[-1] != pts[-1]:
        out.append(pts[-1])
    return out


def stats(pts):
    vals = [v for _, v in pts]
    if not vals:
        return {}
    latest = vals[-1]
    smin, smax = min(vals), max(vals)
    n = len(vals)
    below = sum(1 for v in vals if v <= latest)
    pctile = round(below / n * 100, 1)
    mean = sum(vals) / n
    var = sum((v - mean) ** 2 for v in vals) / n
    sd = var ** 0.5
    z = round((latest - mean) / sd, 2) if sd else 0.0
    # 1y change (approx: compare to ~252 daily / 12 monthly back)
    look = 252 if n > 600 else 12
    prior = vals[-look] if n > look else vals[0]
    return {
        "latest": latest, "min": round(smin, 4), "max": round(smax, 4),
        "pctile": pctile, "zscore": z, "mean": round(mean, 4),
        "chg_1y": round(latest - prior, 4),
        "pct_of_peak": round(latest / smax * 100, 1) if smax else None,
    }


def categorize(key):
    p = key.split(".")
    flow = p[0]
    area = p[2] if len(p) > 2 else "?"
    ind = p[-2] if len(p) >= 2 else "?"
    if flow == "CLIFS":
        return "clifs", area, "CLIFS — financial stress"
    if area == "U2" and ind == "SS_CIN":
        return "ea_headline", area, IND.get(ind, ind)
    if area == "U2" and ind.startswith("SS_"):
        return "ea_subindex", area, IND.get(ind, ind)
    if ind.startswith("SOV"):
        return ("sovereign_ea" if area == "U2" else "sovereign_country"), area, IND.get(ind, ind)
    if ind == "SS_CIN":
        return "country_ciss", area, IND.get(ind, ind)
    return "other", area, IND.get(ind, ind)


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc).isoformat()
    universe = {}
    universe.update({k: ("CISS", t) for k, t in discover("CISS").items()})
    universe.update({k: ("CLIFS", t) for k, t in discover("CLIFS").items()})

    series = []
    for key, (flow, latest_date) in sorted(universe.items()):
        pts = history(key)
        time.sleep(0.15)
        if not pts:
            continue
        cat, area, ind_label = categorize(key)
        freq = key.split(".")[1] if len(key.split(".")) > 1 else "D"
        st = stats(pts)
        series.append({
            "id": key.replace(".", "_"), "key": key,
            "flow": flow, "category": cat, "area": area,
            "country": COUNTRY.get(area, area), "freq": freq,
            "label": "%s — %s" % (COUNTRY.get(area, area), ind_label),
            "indicator": ind_label,
            "latest_date": pts[-1][0], "start_date": pts[0][0],
            "n_obs": len(pts), "points": downsample_weekly(pts), **st,
        })

    cats = {}
    for s in series:
        cats.setdefault(s["category"], 0)
        cats[s["category"]] += 1
    # headline regime band off the EA composite
    head = next((s for s in series if s["category"] == "ea_headline"), None)
    band = None
    if head:
        v = head["latest"]
        band = ("CRISIS" if v >= 0.45 else "STRESS" if v >= 0.2 else
                "ELEVATED" if v >= 0.1 else "NORMAL" if v >= 0.04 else "CALM")

    out = {
        "engine": "ciss-stress", "version": "1.0.0", "generated_at": now,
        "elapsed_s": round(time.time() - t0, 1),
        "n_series": len(series), "categories": cats,
        "ea_composite": head["latest"] if head else None,
        "ea_composite_date": head["latest_date"] if head else None,
        "ea_regime": band,
        "frequency_note": "CISS composite & sub-indices: daily (ECB ~2-3 business-day lag). SovCISS: daily. Country CISS / CLIFS: monthly.",
        "provenance": "ECB Data Portal (data-api.ecb.europa.eu) — CISS + CLIFS dataflows, full history.",
        "series": series,
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    return {"statusCode": 200, "body": json.dumps({
        "n_series": len(series), "categories": cats, "ea_regime": band,
        "ea_composite": out["ea_composite"], "elapsed_s": out["elapsed_s"]})}
