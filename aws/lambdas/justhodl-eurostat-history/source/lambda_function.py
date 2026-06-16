"""
justhodl-eurostat-history — euro-area business/consumer CONFIDENCE (full DG-ECFIN
set) + INDUSTRIAL & MANUFACTURING PRODUCTION YoY (all MIG breakdowns), pulled from
Eurostat with deep history (1980s/90s) and dropped into data/ecb-hist/ so they (a)
heal into the ECB hub manifest, (b) are chartable via the same full-history modal.

Sources (Eurostat dissemination API, JSON-stat):
  • ei_bssi_m_r2 — Economic Sentiment + industrial/services/consumer/retail/
    construction confidence indicators (EA20, seasonally adjusted)
  • sts_inpr_m   — industrial production YoY (PCH_SM) for total industry,
    manufacturing, and each Main Industrial Grouping (SCA)

OUTPUTS: data/ecb-hist/<id>.json (per series, ecb-history format)
         data/ecb-confidence.json (compact latest snapshot for page tiles)
SCHEDULE: daily 07:20 UTC.  Real data only.
"""
import json
import time
import urllib.request
import urllib.parse
import statistics
from datetime import datetime, timezone

import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
EUROSTAT = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"

# id -> (dataset, label, filter params). time is always the free dimension.
CONFIDENCE = {
    "conf_esi":          ("BS-ESI-I",     "Euro Area — Economic Sentiment Indicator (ESI)"),
    "conf_industrial":   ("BS-ICI-BAL",   "Euro Area — Industrial confidence"),
    "conf_services":     ("BS-SCI-BAL",   "Euro Area — Services confidence"),
    "conf_consumer":     ("BS-CSMCI-BAL", "Euro Area — Consumer confidence"),
    "conf_retail":       ("BS-RCI-BAL",   "Euro Area — Retail trade confidence"),
    "conf_construction": ("BS-CCI-BAL",   "Euro Area — Construction confidence"),
}
IP_YOY = {
    "ip_yoy_total":       ("B-D",      "Euro Area — Industrial production YoY (total industry, %)"),
    "ip_yoy_manufacturing": ("C",      "Euro Area — Manufacturing production YoY (%)"),
    "ip_yoy_intermediate": ("MIG_ING", "Euro Area — Production YoY · intermediate goods (%)"),
    "ip_yoy_capital":     ("MIG_CAG",  "Euro Area — Production YoY · capital goods (%)"),
    "ip_yoy_durable":     ("MIG_DCOG", "Euro Area — Production YoY · durable consumer goods (%)"),
    "ip_yoy_nondurable":  ("MIG_NDCOG","Euro Area — Production YoY · non-durable consumer goods (%)"),
    "ip_yoy_energy":      ("MIG_NRG",  "Euro Area — Production YoY · energy (%)"),
}


def _get(url, t=40):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl raafouis@gmail.com"})
        with urllib.request.urlopen(req, timeout=t) as r:
            return r.read().decode("utf-8", "ignore")
    except Exception as e:
        print("fetch fail %s -> %s" % (url[:80], e))
        return None


def eurostat(dataset, params):
    """Return [[period, value], ...] from a JSON-stat response (time = free dim)."""
    qs = "&".join("%s=%s" % (k, urllib.parse.quote(str(v))) for k, v in params.items())
    body = _get("%s/%s?format=JSON&%s" % (EUROSTAT, dataset, qs))
    if not body:
        return []
    try:
        j = json.loads(body)
        ids = j["id"]; sizes = j["size"]
        stride = {}; acc = 1
        for d, sz in zip(reversed(ids), reversed(sizes)):
            stride[d] = acc; acc *= sz
        tindex = j["dimension"]["time"]["category"]["index"]
        val = j["value"]
        pts = []
        for period, pos in sorted(tindex.items(), key=lambda kv: kv[1]):
            v = val.get(str(pos * stride["time"]))
            if v is not None:
                pts.append([period, round(float(v), 4)])
        return pts
    except Exception as e:
        print("parse fail %s: %s" % (dataset, e))
        return []


def _stats(pts):
    vals = [v for _, v in pts]
    latest = vals[-1]
    pctl = round(100.0 * sum(1 for v in vals if v <= latest) / len(vals), 1)
    mu = statistics.mean(vals); sd = statistics.pstdev(vals)
    z = round((latest - mu) / sd, 2) if sd else 0.0
    return latest, round(min(vals), 4), round(max(vals), 4), pctl, z


def write_series(sid, label, pts, flow_key):
    if len(pts) < 5:
        print("  skip %s (only %d pts)" % (sid, len(pts)))
        return None
    latest, lo, hi, pctl, z = _stats(pts)
    lp = pts[-1][0]
    freq = "annual" if len(lp) == 4 else "quarterly" if "Q" in lp else "monthly" if len(lp) == 7 else "daily"
    out = {"id": sid, "label": label, "freq": freq, "flow_key": flow_key,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "n_points": len(pts), "first_date": pts[0][0], "latest_date": pts[-1][0],
           "latest": latest, "min": lo, "max": hi, "percentile": pctl, "z_score": z,
           "points": pts}
    S3.put_object(Bucket=BUCKET, Key="data/ecb-hist/%s.json" % sid,
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=43200")
    return {"id": sid, "label": label, "latest": latest, "latest_date": pts[-1][0],
            "percentile": pctl, "z_score": z, "first_date": pts[0][0], "n_points": len(pts)}


def lambda_handler(event, context):
    snap = {"generated_at": datetime.now(timezone.utc).isoformat(),
            "confidence": [], "production_yoy": [], "written": 0}
    written = 0

    for sid, (indic, label) in CONFIDENCE.items():
        pts = eurostat("ei_bssi_m_r2",
                       {"geo": "EA20", "s_adj": "SA", "indic": indic})
        meta = write_series(sid, label, pts, "eurostat/ei_bssi_m_r2/%s" % indic)
        time.sleep(0.4)
        if meta:
            written += 1; snap["confidence"].append(meta)

    for sid, (nace, label) in IP_YOY.items():
        pts = eurostat("sts_inpr_m",
                       {"geo": "EA20", "s_adj": "SCA", "unit": "PCH_SM", "nace_r2": nace})
        if len(pts) < 5:  # manufacturing 'C' sometimes only as 'B_C'; retry once
            alt = {"C": "B_C", "MIG_NRG": "MIG_NRG_X_E"}.get(nace)
            if alt:
                pts = eurostat("sts_inpr_m",
                               {"geo": "EA20", "s_adj": "SCA", "unit": "PCH_SM", "nace_r2": alt})
        meta = write_series(sid, label, pts, "eurostat/sts_inpr_m/%s" % nace)
        time.sleep(0.4)
        if meta:
            written += 1; snap["production_yoy"].append(meta)

    snap["written"] = written
    S3.put_object(Bucket=BUCKET, Key="data/ecb-confidence.json",
                  Body=json.dumps(snap, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    return {"statusCode": 200, "body": json.dumps({"written": written,
            "confidence": len(snap["confidence"]), "ip_yoy": len(snap["production_yoy"])})}
