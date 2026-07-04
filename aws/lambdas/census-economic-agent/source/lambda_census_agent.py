"""
census-economic-agent — U.S. Census Bureau retail desk. Advance Monthly Retail
Trade (MARTS) — total retail, ex-auto, and food services — the timeliest hard
read on the consumer and a direct input to GDP nowcasts. Persists to S3
(data/census-economic.json).

Key from env CENSUS_API_KEY (SSM + Lambda env; not hardcoded). EITS timeseries API
with `time` as a predicate. Real data only; each series records its own error.
(Housing/durable-goods use a different EITS shape [time_slot_id] and are already
covered via FRED, so this desk focuses on retail where Census is the primary read.)
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
BASE = "https://api.census.gov/data/timeseries/eits/marts"

CATEGORIES = {
    "retail_and_food_services": "44X72",
    "retail_trade": "44000",
    "retail_ex_auto": "44Y72",
    "motor_vehicles_parts": "441",
    "food_services": "722",
}


def eits(category, data_type="SM", sa="yes"):
    now = datetime.now(timezone.utc)
    params = {"get": "cell_value", "CATEGORY_CODE": category, "DATA_TYPE_CODE": data_type,
              "SEASONALLY_ADJ": sa, "time": "from %d-01" % (now.year - 2), "for": "us"}
    if API_KEY:
        params["key"] = API_KEY
    url = BASE + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/census"})
    with urllib.request.urlopen(req, timeout=30) as r:
        rows = json.loads(r.read())
    if not rows or len(rows) < 2:
        return {"error": "no rows"}
    hdr = rows[0]
    ci = hdr.index("cell_value")
    ti = hdr.index("time") if "time" in hdr else None
    pts = []
    for row in rows[1:]:
        try:
            pts.append((row[ti] if ti is not None else "", float(row[ci])))
        except Exception:
            pass
    if not pts:
        return {"error": "no numeric values"}
    if ti is not None:
        pts.sort()
    val = pts[-1][1]
    per = pts[-1][0]
    prior = pts[-2][1] if len(pts) > 1 else None
    yago = None
    if ti is not None and "-" in per:
        y, m = per.split("-"); pk = f"{int(y)-1}-{m}"
        for p, v in pts:
            if p == pk:
                yago = v
    return {"value_musd": val, "period": per,
            "mom_pct": round((val / prior - 1) * 100, 2) if prior else None,
            "yoy_pct": round((val / yago - 1) * 100, 2) if yago else None}


def safe(category):
    try:
        return eits(category)
    except Exception as e:
        return {"error": str(e)[:100]}


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    if not API_KEY:
        # EITS is usable keyless at low volume; continue but note it
        pass
    retail = {name: safe(code) for name, code in CATEGORIES.items()}

    def g(k, f="value_musd"):
        return (retail.get(k) or {}).get(f)

    total = retail.get("retail_and_food_services") or retail.get("retail_trade") or {}
    summary = {
        "retail_sales_musd": total.get("value_musd"),
        "retail_sales_mom_pct": total.get("mom_pct"),
        "retail_sales_yoy_pct": total.get("yoy_pct"),
        "retail_ex_auto_mom_pct": g("retail_ex_auto", "mom_pct"),
        "food_services_yoy_pct": g("food_services", "yoy_pct"),
        "period": total.get("period"),
        "read": None,
    }
    if summary["retail_sales_mom_pct"] is not None:
        m = summary["retail_sales_mom_pct"]
        summary["read"] = ("STRONG" if m >= 0.6 else "SOLID" if m >= 0.2
                           else "SOFT" if m >= -0.2 else "CONTRACTING")
    n_live = sum(1 for v in retail.values() if isinstance(v, dict) and v.get("value_musd") is not None)
    out = {
        "generated_at": now.isoformat(),
        "source": "U.S. Census Bureau MARTS / EITS API (api.census.gov) — direct, free",
        "summary": summary, "retail": retail, "_series_live": n_live,
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="max-age=3600")
    return {"ok": True, "series_live": n_live}
