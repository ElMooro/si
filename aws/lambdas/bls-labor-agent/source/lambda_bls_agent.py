"""
bls-labor-agent — Bureau of Labor Statistics desk (CPI, PPI, Employment Situation/
NFP, JOLTS, ECI). Persists to S3 (data/bls-labor.json).

Uses BLS v2 with a registered key (env BLS_API_KEY) when the key is VALID — that
unlocks inline MoM/YoY calculations + higher limits. If the key is missing/invalid,
falls back to the free keyless v1 API and computes changes locally. Real data only.
"""
import os
import json
import urllib.request
from datetime import datetime, timezone

import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/bls-labor.json"
API_KEY = os.environ.get("BLS_API_KEY", "")
V2 = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
V1 = "https://api.bls.gov/publicAPI/v1/timeseries/data/"

LABOR = {
    "unemployment_rate": "LNS14000000",
    "u6_underemployment": "LNS13327709",
    "nonfarm_payrolls_level_k": "CES0000000001",
    "avg_hourly_earnings": "CES0500000003",
    "labor_force_participation": "LNS11300000",
    "emp_population_ratio": "LNS12300000",
    "job_openings_k": "JTS000000000000000JOL",
    "quits_rate": "JTS000000000000000QUR",
    "hires_rate": "JTS000000000000000HIR",
}
INFLATION = {
    "cpi_all_items": "CUUR0000SA0",
    "cpi_core": "CUUR0000SA0L1E",
    "cpi_shelter": "CUUR0000SAH1",
    "cpi_core_services_sa": "CUSR0000SASLE",
    "ppi_final_demand": "WPUFD4",
    "ppi_core": "WPUFD49104",
    "eci_compensation": "CIU1010000000000A",
}
PRODUCTIVITY = {
    "nonfarm_productivity": "PRS85006092",   # output per hour, nonfarm business (index)
    "unit_labor_costs": "PRS85006112",       # unit labor costs, nonfarm business (index)
    "real_hourly_comp": "PRS85006152",       # real hourly compensation (index)
}


def _call(url, payload):
    req = urllib.request.Request(url, data=json.dumps(payload).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=45) as r:
        return json.loads(r.read())


def fetch_bls(series_map):
    now = datetime.now(timezone.utc)
    base = {"seriesid": list(series_map.values()),
            "startyear": str(now.year - 2), "endyear": str(now.year)}
    resp, api = None, None
    if API_KEY:
        try:
            p = dict(base); p["registrationkey"] = API_KEY; p["calculations"] = True
            r = _call(V2, p)
            if r.get("status") == "REQUEST_SUCCEEDED" and (r.get("Results") or {}).get("series"):
                resp, api = r, "v2"
        except Exception:
            pass
    if resp is None:
        r = _call(V1, base)
        resp, api = r, "v1"
    by_id = {s.get("seriesID"): s.get("data", []) for s in (resp.get("Results", {}) or {}).get("series", [])}
    inv = {v: k for k, v in series_map.items()}
    out = {}
    for sid, rows in by_id.items():
        name = inv.get(sid, sid)
        if not rows:
            out[name] = {"series_id": sid, "error": "no data"}
            continue
        pm = {}
        for d in rows:
            try:
                pm[(d["year"], d["period"])] = float(d["value"])
            except Exception:
                pass
        latest = rows[0]
        try:
            val = float(latest["value"])
        except Exception:
            val = None
        ly, lp = latest["year"], latest["period"]
        mom = yoy = None
        if val is not None and len(rows) > 1:
            try:
                pv = float(rows[1]["value"]); mom = round((val / pv - 1) * 100, 2) if pv else None
            except Exception:
                pass
        pk = (str(int(ly) - 1), lp)
        if val is not None and pm.get(pk):
            yoy = round((val / pm[pk] - 1) * 100, 2)
        calc = (latest.get("calculations") or {}).get("pct_changes") or {}
        try:
            if calc.get("1") is not None: mom = float(calc["1"])
            if calc.get("12") is not None: yoy = float(calc["12"])
        except Exception:
            pass
        out[name] = {"series_id": sid, "value": val, "period": f"{ly}-{lp[-2:]}",
                     "period_name": latest.get("periodName"), "mom_pct": mom, "yoy_pct": yoy}
    return out, api


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    labor = inflation = {}
    api = None
    errs = []
    try:
        labor, api = fetch_bls(LABOR)
    except Exception as e:
        errs.append("labor:" + str(e)[:100]); labor = {}
    try:
        inflation, api2 = fetch_bls(INFLATION); api = api or api2
    except Exception as e:
        errs.append("inflation:" + str(e)[:100]); inflation = {}
    productivity = {}
    try:
        productivity, api3 = fetch_bls(PRODUCTIVITY); api = api or api3
    except Exception as e:
        errs.append("productivity:" + str(e)[:100]); productivity = {}

    def g(d, k, f="value"):
        return (d.get(k) or {}).get(f)

    ur = g(labor, "unemployment_rate")
    core_yoy = g(inflation, "cpi_core", "yoy_pct")
    summary = {
        "unemployment_rate": ur,
        "cpi_yoy_pct": g(inflation, "cpi_all_items", "yoy_pct"),
        "core_cpi_yoy_pct": core_yoy,
        "shelter_yoy_pct": g(inflation, "cpi_shelter", "yoy_pct"),
        "core_services_yoy_pct": g(inflation, "cpi_core_services_sa", "yoy_pct"),
        "wage_growth_yoy_pct": g(labor, "avg_hourly_earnings", "yoy_pct"),
        "job_openings_k": g(labor, "job_openings_k"),
        "quits_rate": g(labor, "quits_rate"),
        "unit_labor_costs_yoy_pct": g(productivity, "unit_labor_costs", "yoy_pct"),
        "productivity_yoy_pct": g(productivity, "nonfarm_productivity", "yoy_pct"),
        "labor_read": ("TIGHT" if (ur is not None and ur < 4.2) else "LOOSENING" if ur is not None else None),
        "inflation_read": ("ABOVE TARGET" if (core_yoy is not None and core_yoy > 2.5)
                           else "NEAR TARGET" if core_yoy is not None else None),
    }
    n_live = sum(1 for d in (labor, inflation, productivity) for v in d.values()
                 if isinstance(v, dict) and v.get("value") is not None)
    out = {
        "generated_at": now.isoformat(),
        "source": "U.S. Bureau of Labor Statistics API (api.bls.gov) — direct, free",
        "api_version": api,
        "key_valid": (api == "v2"),
        "summary": summary, "labor_market": labor, "inflation": inflation,
        "productivity": productivity,
        "_series_live": n_live, "_error": "; ".join(errs) or None,
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="max-age=3600")
    return {"ok": True, "series_live": n_live, "api": api}
