"""
eia-energy-agent — US/global energy dashboard feed.

The hard-coded EIA API key was revoked (403). FRED mirrors the core EIA petroleum
& natural-gas data for free, so the engine now pulls its CORE dashboard from FRED
(WTI, Brent, Henry Hub, crude inventories, production, SPR, fuel prices, product
stocks) with a working key. The EIA STEO block (OPEC output, world supply/demand
forecasts) is kept as a BONUS that only populates if a VALID EIA_API_KEY is set —
otherwise it honestly reports that it needs a key. Writes data/eia-energy.json.
Real data only. Also serves the same payload over its HTTP endpoint.
"""
import json, os, urllib.request, traceback
from datetime import datetime

import boto3

FRED_KEY = os.environ.get("FRED_API_KEY", "")
EIA_KEY = os.environ.get("EIA_API_KEY", "")
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/eia-energy.json"

# FRED series (EIA-sourced, still maintained on FRED) -> (label, unit).
# NOTE: FRED discontinued the EIA WEEKLY petroleum-status series (crude stocks,
# production, SPR, product stocks, imports/exports) — those now live only behind
# the EIA API and populate via the eia_petroleum() block when a valid key is set.
FRED_ENERGY = {
    "WTIPUUS":  ("DCOILWTICO",  "WTI Crude ($/bbl)", "$/bbl"),
    "BREPROD":  ("DCOILBRENTEU", "Brent Crude ($/bbl)", "$/bbl"),
    "PRCE_NOM_HENRY": ("DHHNGSP", "Henry Hub Nat Gas ($/MMBtu)", "$/MMBtu"),
    "MGWHUUS":  ("GASREGW",   "US Regular Gasoline ($/gal)", "$/gal"),
    "D2WHUUS":  ("GASDESW",   "US Diesel ($/gal)", "$/gal"),
}

# EIA API series (bonus — populate only with a valid EIA_API_KEY). Free to register
# at eia.gov/opendata; a key is emailed instantly. These are the weekly inventory /
# production numbers plus STEO forecasts (OPEC output, world supply/demand).
EIA_PETROLEUM = {"PET.WCESTUS1.W": "US Crude Inventories ex-SPR (kbbl)",
                 "PET.WCRFPUS2.W": "US Crude Production (kbbl/d)",
                 "PET.WCSSTUS1.W": "US SPR Crude Stocks (kbbl)",
                 "PET.WGTSTUS1.W": "US Gasoline Stocks (kbbl)",
                 "PET.WDISTUS1.W": "US Distillate Stocks (kbbl)"}
STEO = {"PAPR_OPEC": "OPEC Crude Production (Mb/d)", "PATC_WORLD": "World Petroleum Consumption (Mb/d)",
        "PASC_WORLD": "World Petroleum Supply (Mb/d)", "PAPR_NON_OPEC": "Non-OPEC Production (Mb/d)"}


def _get(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode()


def fred_metric(sid):
    """Latest value + WoW/MoM + YoY + 52pt history from a FRED series (latest first)."""
    try:
        url = ("https://api.stlouisfed.org/fred/series/observations"
               "?series_id=%s&api_key=%s&file_type=json&sort_order=desc&limit=400" % (sid, FRED_KEY))
        rows = [o for o in json.loads(_get(url)).get("observations", []) if o.get("value") not in (".", "", None)]
        if not rows:
            return {"error": "empty"}
        vals = []
        for o in rows:
            try:
                vals.append((o["date"], float(o["value"])))
            except Exception:
                pass
        if not vals:
            return {"error": "no numeric"}
        cur = vals[0][1]
        prev = vals[1][1] if len(vals) > 1 else None
        yago = vals[52][1] if len(vals) > 52 else (vals[12][1] if len(vals) > 12 else None)
        return {"period": vals[0][0], "value": round(cur, 3),
                "chg": round(cur - prev, 3) if prev is not None else None,
                "chg_pct": round((cur - prev) / abs(prev) * 100, 2) if prev else None,
                "yoy": round((cur - yago) / abs(yago) * 100, 2) if yago else None,
                "history": [{"p": d, "v": round(v, 3)} for d, v in vals[:52][::-1]]}
    except Exception as e:
        return {"error": "%s: %s" % (type(e).__name__, str(e)[:80])}


def eia_v2(series_id):
    """Fetch a legacy EIA series id via the v2 /seriesid route (needs valid key)."""
    if not EIA_KEY:
        return {"error": "needs a valid EIA_API_KEY (free at eia.gov/opendata)"}
    try:
        url = "https://api.eia.gov/v2/seriesid/%s?api_key=%s" % (series_id, EIA_KEY)
        rows = json.loads(_get(url)).get("response", {}).get("data", [])
        rows = sorted([r for r in rows if r.get("value") not in (None, "")], key=lambda r: r.get("period", ""), reverse=True)
        if not rows:
            return {"error": "empty"}
        cur = float(rows[0]["value"])
        y = float(rows[52]["value"]) if len(rows) > 52 else (float(rows[12]["value"]) if len(rows) > 12 else None)
        return {"period": rows[0].get("period"), "value": round(cur, 3),
                "yoy": round((cur - y) / abs(y) * 100, 2) if y else None,
                "history": [{"p": r.get("period"), "v": float(r["value"])} for r in rows[:52][::-1]]}
    except Exception as e:
        return {"error": "%s: %s" % (type(e).__name__, str(e)[:80])}


def build():
    core = {}
    for key, (sid, label, unit) in FRED_ENERGY.items():
        d = fred_metric(sid)
        core[key] = {"name": label, "unit": unit, "fred": sid,
                     **({"data": d} if "error" not in d else {"error": d["error"]})}
    eia = {}
    for sid, label in EIA_PETROLEUM.items():
        d = eia_v2(sid)
        eia[sid] = {"name": label, **({"data": d} if "error" not in d else {"error": d["error"]})}
    steo = {}
    for sid, label in STEO.items():
        d = eia_v2("STEO." + sid + ".M")
        steo[sid] = {"name": label, **({"data": d} if "error" not in d else {"error": d["error"]})}

    def grp(keys):
        return {k: core[k] for k in keys if k in core}

    ok = len([v for v in core.values() if v.get("data")])
    return {
        "agent": "eia-energy-agent",
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "source": "FRED (EIA-sourced daily/weekly prices) — live; EIA API blocks (inventories/production/STEO) need a valid EIA key",
        "oil_markets": grp(["WTIPUUS", "BREPROD"]),
        "natural_gas": grp(["PRCE_NOM_HENRY"]),
        "fuel_prices": grp(["MGWHUUS", "D2WHUUS"]),
        "inventories_production": eia,   # populates with a valid EIA key
        "steo_forecast": steo,           # populates with a valid EIA key
        "all_series": core,
        "metrics_ok": ok, "metrics_err": len(core) - ok,
        "eia_key_present": bool(EIA_KEY),
        "note": None if EIA_KEY else "Live: WTI/Brent/Henry Hub/gasoline/diesel via FRED. Add a free EIA API key (SSM/env EIA_API_KEY) to unlock crude inventories, production, SPR, and OPEC/world supply-demand.",
    }


def lambda_handler(event, context=None):
    h = {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*",
         "Access-Control-Allow-Headers": "Content-Type", "Access-Control-Allow-Methods": "GET, POST, OPTIONS"}
    if isinstance(event, dict) and event.get("requestContext", {}).get("http", {}).get("method") == "OPTIONS":
        return {"statusCode": 200, "headers": h, "body": "{}"}
    path = event.get("rawPath", "") if isinstance(event, dict) else ""
    if "/health" in path:
        return {"statusCode": 200, "headers": h, "body": json.dumps({"status": "healthy", "agent": "eia-energy-agent"})}
    if "/debug" in path:
        return {"statusCode": 200, "headers": h, "body": json.dumps({"debug": True, "wti": fred_metric("DCOILWTICO")}, default=str)}
    try:
        payload = build()
        try:
            S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(payload, default=str).encode(),
                          ContentType="application/json", CacheControl="max-age=3600")
            payload["_s3"] = OUT_KEY
        except Exception as se:
            payload["_s3_error"] = str(se)[:150]
        return {"statusCode": 200, "headers": h, "body": json.dumps(payload, default=str)}
    except Exception as e:
        return {"statusCode": 500, "headers": h, "body": json.dumps({"error": str(e), "trace": traceback.format_exc()})}
