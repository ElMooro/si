"""justhodl-freight-pulse v1.0 — US freight canary composite.

Khalid's directive: freight as a leading canary for trade/manufacturing —
anticipate slowdown or acceleration before it prints in GDP. Sources (all
FRED, monthly): DOT Freight TSI, Cass Freight shipments & expenditures,
ATA truck tonnage, AAR rail carloads & intermodal. Per series: level, yoy,
6m annualized slope, z vs 5y; composite FREIGHT_PULSE −100..+100 with
ACCELERATING / STABLE / DECELERATING verdict + inflection flag when slope
sign diverges from yoy (turning points). Feeds data/freight-pulse.json.
Pairs with portwatch exporters pulse (origin gateways) for the full chain:
foreign port -> US freight -> economy. stdlib-only; never fabricates.
"""
import json
import os
import urllib.request
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
BUCKET = "justhodl-dashboard-live"
KEY = "data/freight-pulse.json"
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")
S3 = boto3.client("s3", region_name="us-east-1")

SERIES = {
    "tsi_freight": ("TSIFRGHT", "DOT Freight Transportation Services Index"),
    "cass_shipments": ("FRGSHPUSM649NCIS", "Cass Freight Index: Shipments"),
    "cass_expend": ("FRGEXPUSM649NCIS", "Cass Freight Index: Expenditures"),
    "truck_tonnage": ("TRUCKD11", "ATA Truck Tonnage Index"),
    "rail_carloads": ("RAILFRTCARLOADSD11", "Rail Freight Carloads"),
    "rail_intermodal": ("RAILFRTINTERMODALD11", "Rail Freight Intermodal"),
}


def _fred(sid):
    u = (f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}"
         f"&api_key={FRED_KEY}&file_type=json&observation_start=2015-01-01")
    try:
        r = urllib.request.urlopen(u, timeout=25)
        obs = json.loads(r.read()).get("observations") or []
        pts = [(o["date"], float(o["value"])) for o in obs
               if o.get("value") not in (None, ".", "")]
        return pts, None
    except Exception as e:
        return [], str(e)[:100]


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    out = {"ok": False, "version": VERSION, "generated_at": now.isoformat(),
           "series": {}, "errors": []}
    scores = []
    for key, (sid, name) in SERIES.items():
        pts, err = _fred(sid)
        if err or len(pts) < 30:
            out["errors"].append(f"{key}: {err or 'short'}")
            out["series"][key] = {"name": name, "ok": False, "err": err}
            continue
        vals = [v for _, v in pts]
        latest_d, latest = pts[-1]
        yoy = (100 * (latest / vals[-13] - 1)) if len(vals) >= 13 and vals[-13] else None
        m6 = (100 * ((latest / vals[-7]) ** 2 - 1)) if len(vals) >= 7 and vals[-7] else None
        base = vals[-60:]
        mean = sum(base) / len(base)
        sd = (sum((x - mean) ** 2 for x in base) / len(base)) ** 0.5 or 1e-9
        z = round((latest - mean) / sd, 2)
        d = {"name": name, "ok": True, "date": latest_d,
             "level": round(latest, 1),
             "yoy_pct": round(yoy, 1) if yoy is not None else None,
             "m6_ann_pct": round(m6, 1) if m6 is not None else None,
             "z_5y": z}
        d["inflection"] = (yoy is not None and m6 is not None
                           and ((yoy < 0 < m6) or (yoy > 0 > m6)))
        out["series"][key] = d
        if yoy is not None and m6 is not None:
            scores.append(max(-100, min(100, yoy * 4 + m6 * 3 + z * 10)))
    if scores:
        comp = round(sum(scores) / len(scores), 1)
        out["composite"] = comp
        out["verdict"] = ("ACCELERATING" if comp >= 15 else
                          "DECELERATING" if comp <= -15 else "STABLE")
        out["inflections"] = [k for k, v in out["series"].items()
                              if v.get("inflection")]
        out["n_live"] = len(scores)
        out["ok"] = len(scores) >= 4
    out["method"] = ("per series: yoy, 6m annualized slope, z vs 5y; "
                     "composite = mean(yoy*4 + m6*3 + z*10) clamped ±100; "
                     "inflection = slope sign diverges from yoy (turning point)")
    S3.put_object(Bucket=BUCKET, Key=KEY,
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=3600")
    print(f"[freight] live={out.get('n_live')} comp={out.get('composite')} "
          f"verdict={out.get('verdict')} infl={out.get('inflections')} "
          f"errs={out['errors']}")
    return {"ok": out["ok"], "composite": out.get("composite"),
            "verdict": out.get("verdict")}


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2)[:1200])
