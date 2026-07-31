"""justhodl-cq-feed v1.0 ops4205 — CryptoQuant paid rail. Self-discovers
metric paths per TV on-chain bare, caches routes."""
import json
import re
import time
import urllib.request
from datetime import datetime, timezone

import boto3

MARKER = "cq-feed v1.0 ops4205"
S3 = boto3.client("s3")
SSM = boto3.client("ssm")
BUCKET = "justhodl-dashboard-live"
FAMS = ("market-indicator", "network-indicator", "network-data",
        "market-data", "flow-indicator", "miner-flows",
        "exchange-flows", "fund-data", "inter-entity-flows")
PREF = ("GLASSNODE", "INTOTHEBLOCK", "COINMETRICS", "CRYPTOQUANT")


def cqget(key, path, extra=""):
    url = ("https://api.cryptoquant.com/v1/" + path
           + "?window=day&limit=1" + extra)
    try:
        req = urllib.request.Request(
            url, headers={"Authorization": "Bearer " + key,
                          "User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=12) as r:
            j = json.loads(r.read().decode())
        rows = ((j.get("result") or {}).get("data")) or []
        if rows:
            row = rows[0]
            dt = str(row.get("date") or row.get("datetime"))[:10]
            for k, v in row.items():
                if k in ("date", "datetime"):
                    continue
                if isinstance(v, (int, float)):
                    return float(v), dt, k
    except Exception:
        pass
    return None, None, None


def lambda_handler(event, context):
    t0 = time.time()
    key = SSM.get_parameter(Name="/justhodl/cryptoquant_api",
                            WithDecryption=True)["Parameter"]["Value"]
    wl = json.loads(S3.get_object(
        Bucket=BUCKET, Key="data/tv-watchlists.json")["Body"].read())
    bares = set()
    for l in (wl.get("lists") or []):
        for sy in l.get("symbols") or []:
            sy = str(sy)
            if sy.split(":")[0] in PREF:
                bares.add(sy.split(":", 1)[1])
    try:
        doc = json.loads(S3.get_object(
            Bucket=BUCKET, Key="data/cq-feed.json")["Body"].read())
        prices = doc.get("prices") or {}
        routes = doc.get("routes") or {}
        deadr = doc.get("dead") or {}
    except Exception:
        prices, routes, deadr = {}, {}, {}
    tried = 0
    for bare in sorted(bares):
        if time.time() - t0 > 220 or tried >= 60:
            break
        if bare in deadr:
            continue
        m = re.match(r"^(BTC|ETH)[_\.]?(.+)$", bare)
        coin = (m.group(1).lower() if m else "btc")
        metric = re.sub(r"[^A-Za-z0-9]+", "-",
                        (m.group(2) if m else bare)).strip("-").lower()
        route = routes.get(bare)
        cands = ([route] if route else
                 [f"{coin}/{fam}/{metric}" for fam in FAMS])
        got = False
        for path in cands:
            extra = ("&exchange=all_exchange"
                     if "exchange" in path or "flow" in path else "")
            v, dt, fld = cqget(key, path, extra)
            if v is not None:
                for k2 in (bare,) + tuple(
                        p + ":" + bare for p in PREF):
                    prices[k2] = {"value": v, "asof": dt,
                                  "src": path + "." + str(fld)[:20]}
                routes[bare] = path
                got = True
                break
            time.sleep(0.12)
        tried += 1
        if not got and not route:
            deadr[bare] = 1
    out = {"generated_at": datetime.now(timezone.utc).isoformat(),
           "marker": MARKER, "prices": prices, "routes": routes,
           "dead": deadr, "n": len(routes),
           "targets": len(bares),
           "elapsed_s": round(time.time() - t0, 1)}
    S3.put_object(Bucket=BUCKET, Key="data/cq-feed.json",
                  Body=json.dumps(out).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=600")
    print("[cq-feed] routed=%d dead=%d targets=%d %.0fs"
          % (len(routes), len(deadr), len(bares), out["elapsed_s"]))
    return {"routed": len(routes), "targets": len(bares)}
