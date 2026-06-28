"""justhodl-coinbase-premium · v1.0 — US-institutional spot-demand proxy.

Coinbase is the venue of choice for US institutions; Kraken/global is the broader reference.
A positive Coinbase premium (Coinbase price > reference) = US/institutional bid; a negative
premium = US-side distribution. A simple, well-known, free signal — and a genuine gap in the fleet.

  premium% = (Coinbase price / Kraken price − 1) × 100

BTC + ETH. Self-accumulates history for percentile/trend (no free historical premium exists).
"""
import json
import time
import urllib.request
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/coinbase-premium.json"
HIST_KEY = "data/coinbase-premium-history.json"


def _get(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def premium(cb_product, kr_pair):
    cb = _get("https://api.exchange.coinbase.com/products/%s/ticker" % cb_product)
    kr = _get("https://api.kraken.com/0/public/Ticker?pair=%s" % kr_pair)
    cbp = float(cb.get("price"))
    krp = float(list(kr["result"].values())[0]["c"][0])
    prem = round((cbp / krp - 1) * 100, 3)
    read = ("US institutional BID" if prem >= 0.05 else "US distribution" if prem <= -0.05 else "balanced")
    return {"coinbase": round(cbp, 2), "kraken": round(krp, 2), "premium_pct": prem, "read": read}


def lambda_handler(event, context):
    t0 = time.time()
    out = {"generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "version": "1.0"}
    diag = []
    pairs = {"btc": ("BTC-USD", "XBTUSD"), "eth": ("ETH-USD", "ETHUSD")}
    for a, (cbp, krp) in pairs.items():
        try:
            out[a] = premium(cbp, krp)
        except Exception as e:
            out[a] = {"_err": str(e)[:120]}
            diag.append("%s:%s" % (a, str(e)[:60]))

    btc = out.get("btc") or {}
    out["btc_premium_pct"] = btc.get("premium_pct")
    out["interpretation"] = (("BTC Coinbase premium %s%% — %s" % (btc.get("premium_pct"), btc.get("read")))
                             if btc.get("premium_pct") is not None else None)

    try:
        try:
            hist = json.loads(s3.get_object(Bucket=BUCKET, Key=HIST_KEY)["Body"].read())
        except Exception:
            hist = {"series": []}
        ser = hist.get("series", [])
        stamp = out["generated_at"]
        ser.append({"t": stamp, "btc": btc.get("premium_pct"), "eth": (out.get("eth") or {}).get("premium_pct")})
        ser = ser[-2000:]
        hist["series"] = ser
        s3.put_object(Bucket=BUCKET, Key=HIST_KEY, Body=json.dumps(hist, default=str).encode(),
                      ContentType="application/json")
        # rolling stats over recent history
        bvals = [x["btc"] for x in ser if x.get("btc") is not None]
        if bvals:
            out["btc_premium_avg_recent"] = round(sum(bvals[-168:]) / len(bvals[-168:]), 3)
        out["history_n"] = len(ser)
    except Exception as e:
        diag.append("hist:" + str(e)[:60])

    out["duration_s"] = round(time.time() - t0, 1)
    if diag:
        out["_diag"] = diag
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=900")
    return {"statusCode": 200, "body": json.dumps({"btc_premium_pct": out.get("btc_premium_pct"),
                                                    "read": btc.get("read")})}
