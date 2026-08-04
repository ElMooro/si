"""justhodl-cq-feed v2.0 ops4221 — the full CQ catalog daily.
19 proven paths, value+prev, BTC_* bare aliases for vault/bus."""
import json
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

import boto3

MARKER = "cq-feed v2.1 ops4365"
S3 = boto3.client("s3")
SSM = boto3.client("ssm")
BUCKET = "justhodl-dashboard-live"
ALIAS = {"btc/market-indicator/mvrv": ("mvrv", "BTC_MVRV"),
         "btc/market-indicator/sopr": ("sopr", "BTC_SOPR"),
         "btc/network-indicator/nupl": ("nupl", "BTC_NUPL"),
         "btc/market-indicator/realized-price":
         ("realized_price", "BTC_REALIZED_PRICE"),
         "btc/exchange-flows/reserve": ("reserve", "BTC_EXCH_RESERVE"),
         "btc/exchange-flows/netflow": ("netflow_total",
                                        "BTC_EXCH_NETFLOW"),
         "btc/flow-indicator/exchange-whale-ratio":
         ("exchange_whale_ratio", "BTC_WHALE_RATIO"),
         "btc/flow-indicator/mpi": ("mpi", "BTC_MPI"),
         "btc/market-indicator/stablecoin-supply-ratio":
         ("stablecoin_supply_ratio", "BTC_SSR"),
         "btc/network-data/hashrate": ("hashrate", "BTC_HASHRATE")}


def lambda_handler(event, context):
    t0 = time.time()
    key = SSM.get_parameter(Name="/justhodl/cryptoquant_api",
                            WithDecryption=True)["Parameter"]["Value"]
    cat = json.loads(S3.get_object(
        Bucket=BUCKET, Key="data/cq-catalog.json")["Body"].read()
    ).get("catalog") or {}
    metrics = {}
    prices = {}
    for path, meta in cat.items():
        if time.time() - t0 > 240:
            break
        url = ("https://api.cryptoquant.com/v1/" + path
               + "?window=day&limit=2" + str(meta.get("extra") or ""))
        rows = []
        for attempt in (1, 2):   # ops4365: quota-aware — retry once on 429
            try:
                req = urllib.request.Request(
                    url, headers={"Authorization": "Bearer " + key,
                                  "User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=12) as r:
                    rows = ((json.loads(r.read().decode())
                             .get("result") or {}).get("data")) or []
                break
            except urllib.error.HTTPError as e:
                if e.code == 429 and attempt == 1:
                    time.sleep(21)
                    continue
                break
            except Exception:
                break
        if not rows and False:
            pass
        if not rows:
            continue
        cur = rows[0]
        prv = rows[1] if len(rows) > 1 else {}
        flds = {k: v for k, v in cur.items()
                if k not in ("date", "datetime")
                and isinstance(v, (int, float))}
        slug = path.replace("/", "_")
        metrics[slug] = {"path": path, "asof": str(cur.get("date"))[:10],
                         "fields": flds,
                         "prev": {k: v for k, v in prv.items()
                                  if k in flds}}
        if path in ALIAS:
            fk, bare = ALIAS[path]
            v = flds.get(fk)
            if isinstance(v, (int, float)):
                rec = {"value": float(v),
                       "asof": str(cur.get("date"))[:10],
                       "src": path + "." + fk}
                prices[bare] = rec
                for p2 in ("GLASSNODE", "INTOTHEBLOCK",
                           "COINMETRICS", "CRYPTOQUANT"):
                    prices[p2 + ":" + bare] = rec
        time.sleep(2.6)   # ops4365: ~23 req/min fits CQ per-minute quota
    doc = {"generated_at": datetime.now(timezone.utc).isoformat(),
           "marker": MARKER, "n_metrics": len(metrics),
           "metrics": metrics, "prices": prices,
           "elapsed_s": round(time.time() - t0, 1)}
    S3.put_object(Bucket=BUCKET, Key="data/cq-feed.json",
                  Body=json.dumps(doc).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=600")
    print("[cq-feed v2] metrics=%d aliases=%d %.0fs"
          % (len(metrics), len(prices), doc["elapsed_s"]))
    return {"metrics": len(metrics)}
