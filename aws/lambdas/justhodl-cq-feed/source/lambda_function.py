"""justhodl-cq-feed v2.2 — full Professional catalog snapshots (latest+prev).

Loads bundled cq-paths.json (unique path+params, v1 + v2 community). All
numeric sibling fields banked. Soft-fail empties/429s. Does not invent
history. Token/symbol/pair and age-distribution matrices are not in the path
list.
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

import boto3

MARKER = "cq-feed v2.2 cqfull"
S3 = boto3.client("s3")
SSM = boto3.client("ssm")
BUCKET = "justhodl-dashboard-live"
V1 = "https://api.cryptoquant.com/v1"
PATH_SLEEP = 0.55
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


def _bundled_paths():
    p = os.path.join(os.path.dirname(__file__), "cq-paths.json")
    try:
        with open(p, "r", encoding="utf-8") as f:
            doc = json.load(f)
        return list(doc.get("paths") or [])
    except Exception as e:
        print("[cq-feed] bundled miss", str(e)[:80])
        return []


def _s3_catalog_paths():
    try:
        cat = json.loads(S3.get_object(
            Bucket=BUCKET, Key="data/cq-catalog.json")["Body"].read()
        ).get("catalog") or {}
        out = []
        for path, meta in cat.items():
            out.append({
                "path": path,
                "extra": str((meta or {}).get("extra") or ""),
                "base": str((meta or {}).get("base") or ""),
            })
        return out
    except Exception:
        return []


def _load_paths():
    bundled = _bundled_paths()
    if bundled:
        return bundled
    return _s3_catalog_paths()


def lambda_handler(event, context):
    t0 = time.time()
    key = SSM.get_parameter(Name="/justhodl/cryptoquant_api",
                            WithDecryption=True)["Parameter"]["Value"]
    paths = _load_paths()
    metrics = {}
    prices = {}
    catalog = {}
    errors = []
    deadline = t0 + 840
    for item in paths:
        if time.time() > deadline:
            errors.append({"path": "_run", "err": "timeout budget"})
            break
        path = str(item.get("path") or "").lstrip("/")
        extra = str(item.get("extra") or "")
        base = str(item.get("base") or "").rstrip("/") or V1
        url = (base + "/" + path + "?window=day&limit=2" + extra)
        rows = []
        last_err = None
        for attempt in (1, 2):
            try:
                req = urllib.request.Request(
                    url, headers={"Authorization": "Bearer " + key,
                                  "User-Agent": "JustHodl/2.1"})
                with urllib.request.urlopen(req, timeout=12) as r:
                    rows = ((json.loads(r.read().decode())
                             .get("result") or {}).get("data")) or []
                last_err = None
                break
            except urllib.error.HTTPError as e:
                last_err = "HTTP %s" % e.code
                if e.code == 429 and attempt == 1:
                    time.sleep(8)
                    continue
                break
            except Exception as e:
                last_err = str(e)[:80]
                break
        if not rows:
            if last_err:
                errors.append({"path": path, "err": last_err})
            time.sleep(PATH_SLEEP)
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
        catalog[path] = {"fields": list(flds.keys()),
                         "sample": flds,
                         "asof": str(cur.get("date"))[:10],
                         "extra": extra,
                         "base": "" if base == V1 else base}
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
        time.sleep(PATH_SLEEP)
    doc = {"generated_at": datetime.now(timezone.utc).isoformat(),
           "marker": MARKER, "n_metrics": len(metrics),
           "n_paths": len(paths),
           "metrics": metrics, "prices": prices,
           "errors": errors or None,
           "elapsed_s": round(time.time() - t0, 1)}
    S3.put_object(Bucket=BUCKET, Key="data/cq-feed.json",
                  Body=json.dumps(doc).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=600")
    S3.put_object(Bucket=BUCKET, Key="data/cq-catalog.json",
                  Body=json.dumps({
                      "marker": MARKER,
                      "n": len(catalog),
                      "catalog": catalog,
                      "expanded_by": "justhodl-cq-feed v2.2",
                      "expanded_at": doc["generated_at"],
                  }).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=600")
    print("[cq-feed v2.2] metrics=%d aliases=%d paths=%d %.0fs"
          % (len(metrics), len(prices), len(paths), doc["elapsed_s"]))
    return {"metrics": len(metrics), "paths": len(paths),
            "errors": len(errors)}
