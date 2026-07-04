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
FRED_KEY = os.environ.get("FRED_API_KEY", "")
BASE = "https://api.census.gov/data/timeseries/eits/marts"

CATEGORIES = {
    "retail_and_food_services": "44X72",
    "retail_trade": "44000",
    "retail_ex_auto": "44Y72",
    "motor_vehicles_parts": "441",
    "gasoline_stations": "447",
    "building_materials": "444",
    "food_services": "722",
}
# Retail control group = total retail & food services minus the volatile 4:
# motor vehicles (441), gasoline (447), building materials (444), food services (722).
# This is the exact BEA/GDP "control" definition — computed from published levels.
CONTROL_MINUS = ["441", "447", "444", "722"]
# Core capital-goods orders + durable goods — Census M3 survey, canonical on FRED.
FRED_ORDERS = {
    "core_capex_orders": ("NEWORDER", "Core capital-goods orders — nondefense ex-aircraft ($M)"),
    "durable_goods_orders": ("DGORDER", "Durable goods new orders ($M)"),
    "durable_ex_transport": ("ADXTNO", "Durable goods ex-transport new orders ($M)"),
}


def fetch_marts():
    """One EITS call: get all MARTS series as output columns, filtered client-side.
    (EITS rejects CATEGORY_CODE/DATA_TYPE_CODE as server-side predicates.)"""
    now = datetime.now(timezone.utc)
    params = {"get": "cell_value,category_code,data_type_code,seasonally_adj",
              "time": "from %d" % (now.year - 2), "for": "us"}
    if API_KEY:
        params["key"] = API_KEY
    url = BASE + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/census"})
    with urllib.request.urlopen(req, timeout=40) as r:
        rows = json.loads(r.read())
    hdr = rows[0]
    idx = {h: i for i, h in enumerate(hdr)}
    ci, cat, dt, saj = idx["cell_value"], idx["category_code"], idx["data_type_code"], idx["seasonally_adj"]
    ti = idx.get("time")
    series = {}
    for row in rows[1:]:
        try:
            v = float(row[ci])
        except Exception:
            continue
        key = (row[cat], row[dt], row[saj])
        t = row[ti] if ti is not None else ""
        series.setdefault(key, []).append((t, v))
    return series


def pick(series, category, data_type="SM", sa="yes"):
    pts = series.get((category, data_type, sa)) or []
    if not pts:
        return {"error": "not found (%s/%s/%s)" % (category, data_type, sa)}
    pts.sort()
    val, per = pts[-1][1], pts[-1][0]
    prior = pts[-2][1] if len(pts) > 1 else None
    yago = None
    if "-" in per:
        y, m = per.split("-"); pk = f"{int(y)-1}-{m}"
        for p, v in pts:
            if p == pk:
                yago = v
    return {"value_musd": val, "period": per,
            "mom_pct": round((val / prior - 1) * 100, 2) if prior else None,
            "yoy_pct": round((val / yago - 1) * 100, 2) if yago else None}


def control_group(series):
    """Retail control group = total(44X72) minus autos/gas/building/food-services,
    computed at the level for every period, then MoM/YoY. The GDP-nowcast input."""
    def lv(cat):
        return {p: v for p, v in (series.get((cat, "SM", "yes")) or [])}
    total = lv("44X72")
    minus = [lv(c) for c in CONTROL_MINUS]
    if not total:
        return {"error": "no total"}
    ctrl = {}
    for p, v in total.items():
        if all(p in m for m in minus):
            ctrl[p] = v - sum(m[p] for m in minus)
    if not ctrl:
        return {"error": "incomplete components"}
    pers = sorted(ctrl)
    val, per = ctrl[pers[-1]], pers[-1]
    prior = ctrl[pers[-2]] if len(pers) > 1 else None
    yago = None
    if "-" in per:
        y, m = per.split("-"); yago = ctrl.get(f"{int(y)-1}-{m}")
    return {"value_musd": round(val, 1), "period": per,
            "mom_pct": round((val / prior - 1) * 100, 2) if prior else None,
            "yoy_pct": round((val / yago - 1) * 100, 2) if yago else None}


def fred_metric(sid):
    try:
        u = ("https://api.stlouisfed.org/fred/series/observations?series_id=%s"
             "&api_key=%s&file_type=json&sort_order=desc&limit=30" % (sid, FRED_KEY))
        obs = [o for o in json.loads(urllib.request.urlopen(u, timeout=20).read()).get("observations", [])
               if o.get("value") not in (".", "", None)]
        if not obs:
            return {"error": "empty"}
        cur = float(obs[0]["value"])
        prev = float(obs[1]["value"]) if len(obs) > 1 else None
        yago = float(obs[12]["value"]) if len(obs) > 12 else None
        return {"value": round(cur, 1), "date": obs[0]["date"],
                "mom_pct": round((cur - prev) / prev * 100, 2) if prev else None,
                "yoy_pct": round((cur - yago) / yago * 100, 2) if yago else None}
    except Exception as e:
        return {"error": str(e)[:80]}


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    if not API_KEY:
        # EITS is usable keyless at low volume; continue but note it
        pass
    try:
        series = fetch_marts()
        fetch_err = None
    except Exception as e:
        series, fetch_err = {}, str(e)[:140]
    retail = {name: pick(series, code) for name, code in CATEGORIES.items()}
    control = control_group(series)
    orders = {name: fred_metric(sid) for name, (sid, lbl) in FRED_ORDERS.items()} if FRED_KEY else {}

    def g(k, f="value_musd"):
        return (retail.get(k) or {}).get(f)

    total = retail.get("retail_and_food_services") or retail.get("retail_trade") or {}
    summary = {
        "retail_sales_musd": total.get("value_musd"),
        "retail_sales_mom_pct": total.get("mom_pct"),
        "retail_sales_yoy_pct": total.get("yoy_pct"),
        "retail_ex_auto_mom_pct": g("retail_ex_auto", "mom_pct"),
        "control_group_mom_pct": control.get("mom_pct"),
        "control_group_yoy_pct": control.get("yoy_pct"),
        "core_capex_mom_pct": (orders.get("core_capex_orders") or {}).get("mom_pct"),
        "core_capex_yoy_pct": (orders.get("core_capex_orders") or {}).get("yoy_pct"),
        "durable_goods_mom_pct": (orders.get("durable_goods_orders") or {}).get("mom_pct"),
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
        "summary": summary, "retail": retail, "control_group": control,
        "manufacturing_orders": orders, "_series_live": n_live, "_fetch_error": fetch_err,
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="max-age=3600")
    return {"ok": True, "series_live": n_live}
