"""
census-economic-agent — U.S. Census Bureau economic desk. The source behind
Advance Retail Sales (the GDP-nowcast input), Housing Starts/Permits, and
Durable-Goods / core capital-goods orders. Persists to S3 (data/census-economic.json).

Key from env CENSUS_API_KEY (SSM + Lambda env; not hardcoded). Uses the EITS
timeseries API. Real data only — each series records its own error if it fails.
"""
import os
import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/census-economic.json"
API_KEY = os.environ.get("CENSUS_API_KEY", "")
BASE = "https://api.census.gov/data/timeseries/eits"


def eits_latest(program, category, data_type, sa="yes"):
    """Return {value, prior, mom_pct, period} for one EITS series (latest month)."""
    now = datetime.now(timezone.utc)
    params = {
        "get": "cell_value,time",
        "CATEGORY_CODE": category,
        "DATA_TYPE_CODE": data_type,
        "SEASONALLY_ADJ": sa,
        "time": "from %d-01" % (now.year - 2),
        "for": "us:*",
        "key": API_KEY,
    }
    url = BASE + "/" + program + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/census"})
    with urllib.request.urlopen(req, timeout=30) as r:
        rows = json.loads(r.read())
    if not rows or len(rows) < 2:
        return {"error": "no rows"}
    hdr = rows[0]
    ci, ti = hdr.index("cell_value"), hdr.index("time")
    pts = []
    for row in rows[1:]:
        try:
            pts.append((row[ti], float(row[ci])))
        except Exception:
            pass
    if not pts:
        return {"error": "no numeric values"}
    pts.sort()
    val = pts[-1][1]
    prior = pts[-2][1] if len(pts) > 1 else None
    yago = None
    per = pts[-1][0]
    if "-" in per:
        y, m = per.split("-"); pk = f"{int(y)-1}-{m}"
        for p, v in pts:
            if p == pk:
                yago = v
    return {
        "value": val, "period": per,
        "mom_pct": round((val / prior - 1) * 100, 2) if prior else None,
        "yoy_pct": round((val / yago - 1) * 100, 2) if yago else None,
    }


def safe(program, category, data_type, sa="yes"):
    try:
        return eits_latest(program, category, data_type, sa)
    except Exception as e:
        return {"error": str(e)[:100]}


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    if not API_KEY:
        S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                      Body=json.dumps({"generated_at": now.isoformat(), "error": "CENSUS_API_KEY not set"}).encode(),
                      ContentType="application/json")
        return {"ok": False, "error": "no key"}

    retail = {
        "retail_and_food_services": safe("marts", "44X72", "SM"),
        "retail_ex_auto": safe("marts", "44Y72", "SM"),
        "food_services": safe("marts", "722", "SM"),
    }
    housing = {
        "housing_starts_total": safe("resconst", "TOTAL", "STARTS"),
        "building_permits_total": safe("resconst", "TOTAL", "PERMITS"),
    }
    durable = {
        "durable_new_orders_total": safe("advm3", "TCG", "NO"),
        "core_capital_goods_orders": safe("advm3", "NDE", "NO"),
    }

    def gv(d, k, f="value"):
        return (d.get(k) or {}).get(f)

    summary = {
        "retail_sales_mom_pct": gv(retail, "retail_and_food_services", "mom_pct"),
        "retail_sales_yoy_pct": gv(retail, "retail_and_food_services", "yoy_pct"),
        "retail_ex_auto_mom_pct": gv(retail, "retail_ex_auto", "mom_pct"),
        "housing_starts": gv(housing, "housing_starts_total"),
        "building_permits": gv(housing, "building_permits_total"),
        "core_capex_orders_mom_pct": gv(durable, "core_capital_goods_orders", "mom_pct"),
    }
    n_live = sum(1 for grp in (retail, housing, durable) for v in grp.values() if "value" in v)
    out = {
        "generated_at": now.isoformat(),
        "source": "U.S. Census Bureau EITS API (api.census.gov) — direct, free",
        "summary": summary,
        "retail": retail, "housing": housing, "durable_goods": durable,
        "_series_live": n_live,
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="max-age=3600")
    return {"ok": True, "series_live": n_live}
