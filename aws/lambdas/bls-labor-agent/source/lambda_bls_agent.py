"""
bls-labor-agent — Bureau of Labor Statistics desk (the source behind CPI, PPI,
the Employment Situation / NFP, JOLTS, and ECI). Persists to S3 so the platform
consumes it directly from BLS, not just FRED's re-publication.

Key is read from env BLS_API_KEY (provisioned to SSM + Lambda env; never hardcoded).
BLS API v2 with a registered key allows 50 series/request and returns MoM/YoY
calculations inline. Real data only — a series that can't fetch records its error.
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
BASE = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

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


def fetch_bls(series_map):
    now = datetime.now(timezone.utc)
    payload = {
        "seriesid": list(series_map.values()),
        "startyear": str(now.year - 2),
        "endyear": str(now.year),
        "registrationkey": API_KEY,
        "calculations": True,
    }
    req = urllib.request.Request(BASE, data=json.dumps(payload).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=40) as r:
        resp = json.loads(r.read())
    by_id = {}
    for s in (resp.get("Results", {}) or {}).get("series", []):
        by_id[s.get("seriesID")] = s.get("data", [])
    out = {}
    inv = {v: k for k, v in series_map.items()}
    for sid, rows in by_id.items():
        name = inv.get(sid, sid)
        if not rows:
            out[name] = {"series_id": sid, "error": "no data"}
            continue
        latest = rows[0]
        calc = latest.get("calculations", {}) or {}
        pc = calc.get("pct_changes", {}) or {}
        try:
            val = float(latest.get("value"))
        except Exception:
            val = latest.get("value")
        out[name] = {
            "series_id": sid,
            "value": val,
            "period": f"{latest.get('year')}-{latest.get('period','')[-2:]}",
            "period_name": latest.get("periodName"),
            "mom_pct": _f(pc.get("1")),
            "3mo_pct": _f(pc.get("3")),
            "yoy_pct": _f(pc.get("12")),
        }
    return out


def _f(x):
    try:
        return float(x)
    except Exception:
        return None


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    if not API_KEY:
        body = {"generated_at": now.isoformat(), "error": "BLS_API_KEY not set"}
        S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(body).encode(),
                      ContentType="application/json")
        return {"ok": False, "error": "no key"}

    labor, inflation = {}, {}
    err = None
    try:
        labor = fetch_bls(LABOR)
    except Exception as e:
        err = "labor: " + str(e)[:120]
    try:
        inflation = fetch_bls(INFLATION)
    except Exception as e:
        err = (err + " | " if err else "") + "inflation: " + str(e)[:120]

    def g(d, k, f="value"):
        return (d.get(k) or {}).get(f)

    ur = g(labor, "unemployment_rate")
    cpi_yoy = g(inflation, "cpi_all_items", "yoy_pct")
    core_yoy = g(inflation, "cpi_core", "yoy_pct")
    summary = {
        "unemployment_rate": ur,
        "cpi_yoy_pct": cpi_yoy,
        "core_cpi_yoy_pct": core_yoy,
        "shelter_yoy_pct": g(inflation, "cpi_shelter", "yoy_pct"),
        "core_services_yoy_pct": g(inflation, "cpi_core_services_sa", "yoy_pct"),
        "wage_growth_yoy_pct": g(labor, "avg_hourly_earnings", "yoy_pct"),
        "job_openings_k": g(labor, "job_openings_k"),
        "quits_rate": g(labor, "quits_rate"),
        "labor_read": ("TIGHT" if (ur is not None and ur < 4.2) else "LOOSENING" if ur is not None else None),
        "inflation_read": ("ABOVE TARGET" if (core_yoy is not None and core_yoy > 2.5)
                           else "NEAR TARGET" if core_yoy is not None else None),
    }
    n_live = sum(1 for d in (labor, inflation) for v in d.values() if "value" in v)
    out = {
        "generated_at": now.isoformat(),
        "source": "U.S. Bureau of Labor Statistics API v2 (api.bls.gov) — direct, free",
        "summary": summary,
        "labor_market": labor,
        "inflation": inflation,
        "_series_live": n_live,
        "_error": err,
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="max-age=3600")
    return {"ok": True, "series_live": n_live}
