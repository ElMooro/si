"""justhodl-crypto-stablecoin-peg · v1.0 — stablecoin peg-stability / depeg monitor.

Stablecoin supply (stablecoin-flow) tells you offshore-dollar creation; this tells you whether
those dollars are actually holding their peg. A depeg is a fast, high-severity crypto tail-risk
(USDC briefly broke to $0.88 in the Mar-2023 SVB scare; UST collapsed in 2022). Cheap insurance
to watch continuously.

  depeg% = (price − 1) × 100   per coin; the gauge is the worst |deviation| across majors.

SOURCE: CoinGecko (free). USDT / USDC / DAI / FDUSD.
"""
import json
import time
import urllib.request
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/crypto-stablecoin-peg.json"
COINS = {"tether": "USDT", "usd-coin": "USDC", "dai": "DAI", "first-digital-usd": "FDUSD"}


def _get(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def lambda_handler(event, context):
    t0 = time.time()
    out = {"generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "version": "1.0"}
    ids = ",".join(COINS.keys())
    coins = {}
    worst = 0.0
    worst_name = None
    try:
        sp = _get("https://api.coingecko.com/api/v3/simple/price?ids=%s&vs_currencies=usd" % ids)
        for cid, sym in COINS.items():
            p = (sp.get(cid) or {}).get("usd")
            if p is None:
                continue
            dev = round((p - 1) * 100, 3)
            st = "green" if abs(dev) <= 0.1 else "yellow" if abs(dev) <= 0.5 else "red"
            coins[sym] = {"price": p, "depeg_pct": dev, "status": st}
            if abs(dev) > abs(worst):
                worst = dev
                worst_name = sym
    except Exception as e:
        out["_err"] = str(e)[:120]

    gauge = "green" if abs(worst) <= 0.1 else "yellow" if abs(worst) <= 0.5 else "red"
    status = ("STABLE" if gauge == "green" else "MINOR DRIFT" if gauge == "yellow" else "DEPEG ALERT")
    out["coins"] = coins
    out["worst_depeg_pct"] = worst
    out["worst_coin"] = worst_name
    out["gauge"] = gauge
    out["status"] = status
    out["interpretation"] = ("All majors holding peg." if gauge == "green"
                             else "%s deviating most (%s%%) — watch." % (worst_name, worst) if gauge == "yellow"
                             else "DEPEG: %s at %s%% — crypto tail-risk elevated." % (worst_name, worst))
    out["duration_s"] = round(time.time() - t0, 1)
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=900")
    return {"statusCode": 200, "body": json.dumps({"status": status, "worst": worst, "coin": worst_name})}
