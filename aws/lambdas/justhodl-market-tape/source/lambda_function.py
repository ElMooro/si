"""
justhodl-market-tape — homepage ticker strip feed. REAL DATA ONLY.

Emits data/market-tape.json:
  {"generated": iso, "items":[{"label","value","display","chg_pct"?}, ...]}

Sources:
  - FMP /stable/quote?symbol= for ^GSPC(SPX) ^IXIC(NDX) BTCUSD(BTC) GCUSD(GOLD)
  - FRED for DGS10(US10Y) VIXCLS(VIX) DTWEXBGS(USD broad; labeled DXY* proxy)
Design rule: the page renders whatever this feed provides — any symbol that
fails to resolve is OMITTED (never faked). Runs every 5 min via EB Scheduler.
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("JH_BUCKET", "justhodl-dashboard-live")
KEY = "data/market-tape.json"
_s3 = boto3.client("s3")


def _get(url, tries=2):
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
            with urllib.request.urlopen(req, timeout=10) as r:
                return json.loads(r.read().decode())
        except Exception:
            time.sleep(0.6 * (i + 1))
    return None


def fmp_quote(sym):
    k = os.environ.get("FMP_KEY", "")
    if not k:
        return None
    d = _get(f"https://financialmodelingprep.com/stable/quote?symbol={sym}&apikey={k}")
    if isinstance(d, list) and d:
        d = d[0]
    if not isinstance(d, dict):
        return None
    px = d.get("price")
    ch = d.get("changesPercentage", d.get("changePercentage"))
    return (px, ch) if isinstance(px, (int, float)) else None


def fred_latest(sid):
    k = os.environ.get("FRED_KEY", "") or os.environ.get("FRED_API_KEY", "")
    if not k:
        return None
    d = _get(f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}"
             f"&api_key={k}&file_type=json&sort_order=desc&limit=2")
    obs = (d or {}).get("observations", [])
    vals = [float(o["value"]) for o in obs if o.get("value") not in (".", "", None)]
    if not vals:
        return None
    chg = ((vals[0] - vals[1]) / vals[1] * 100) if len(vals) > 1 and vals[1] else None
    return vals[0], chg


def fmt(v, big):
    return f"{v:,.0f}" if big else (f"{v:,.1f}" if v >= 100 else f"{v:,.2f}")


def lambda_handler(event, context):
    items = []

    for label, sym, big in [("SPX", "^GSPC", False), ("NDX", "^IXIC", True),
                            ("BTC", "BTCUSD", True), ("GOLD", "GCUSD", True)]:
        q = fmp_quote(sym)
        if q:
            px, ch = q
            it = {"label": label, "value": px, "display": fmt(px, big)}
            if isinstance(ch, (int, float)):
                it["chg_pct"] = round(ch, 2)
            items.append(it)

    t = fred_latest("DGS10")
    if t:
        items.append({"label": "US10Y", "value": t[0], "display": f"{t[0]:.2f}%"})
    v = fred_latest("VIXCLS")
    if v:
        items.append({"label": "VIX", "value": v[0], "display": f"{v[0]:.1f}"})
    d = fred_latest("DTWEXBGS")
    if d:
        it = {"label": "DXY", "value": d[0], "display": f"{d[0]:.1f}"}
        if d[1] is not None:
            it["chg_pct"] = round(d[1], 2)
        items.append(it)

    out = {"generated": datetime.now(timezone.utc).isoformat(),
           "items": items, "n": len(items), "_data_quality": "real:FMP+FRED"}
    _s3.put_object(Bucket=BUCKET, Key=KEY, Body=json.dumps(out).encode(),
                   ContentType="application/json", CacheControl="max-age=120")
    return {"statusCode": 200, "body": json.dumps({"n": len(items),
            "labels": [i["label"] for i in items]})}
