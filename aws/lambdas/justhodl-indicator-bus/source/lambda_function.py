"""justhodl-indicator-bus v1.0 ops4215 — THE canonical indicator bus.
Union of vault LIVE + all feed dicts, one artifact, every engine's input."""
import json
import time
from datetime import datetime, timezone

import boto3

MARKER = "indicator-bus v1.0 ops4215"
S3 = boto3.client("s3")
BUCKET = "justhodl-dashboard-live"


def load(key):
    try:
        return json.loads(S3.get_object(
            Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return {}


def lambda_handler(event, context):
    t0 = time.time()
    bus = {}
    origin = {}

    v = load("data/tradingview.json")
    for r in v.get("symbols") or []:
        if r.get("status") == "LIVE":
            sy = str(r.get("symbol"))
            bus[sy] = {"v": r.get("value"), "asof": r.get("asof"),
                       "src": str(r.get("source"))[:40]}
    origin["vault"] = len(bus)

    te = load("data/te-feed.json").get("prices") or {}
    n0 = len(bus)
    for k, r in te.items():
        if k not in bus and isinstance(r, dict) and "value" in r:
            bus[k] = {"v": r["value"], "asof": r.get("asof"),
                      "src": "te:" + str(r.get("cat", ""))[:30]}
    origin["te_extra"] = len(bus) - n0

    fams = load("data/families.json").get("families") or {}
    n0 = len(bus)
    for fam, d2 in fams.items():
        if not isinstance(d2, dict):
            continue
        for cc, pair in d2.items():
            k = str(cc) + str(fam)
            if k not in bus and isinstance(pair, list) and pair:
                bus[k] = {"v": pair[0],
                          "asof": str(pair[1])[-10:]
                          if len(pair) > 1 else None,
                          "src": "family:" + fam}
    origin["family_extra"] = len(bus) - n0

    for key, tag in (("data/symbol-feed.json", "yahoo"),
                     ("data/cot-feed.json", "cftc"),
                     ("data/cq-feed.json", "cryptoquant"),
                     ("data/fleet-inventory.json", "fleet")):
        p = load(key).get("prices") or {}
        n0 = len(bus)
        for k, r in p.items():
            if k in bus or not isinstance(r, dict):
                continue
            val = r.get("value")
            if val is None:
                continue
            bus[k] = {"v": val, "asof": r.get("asof"),
                      "src": tag + ":" + str(r.get("ysym")
                                             or r.get("src")
                                             or r.get("src_key")
                                             or "")[:32]}
        origin[tag + "_extra"] = len(bus) - n0

    doc = {"generated_at": datetime.now(timezone.utc).isoformat(),
           "marker": MARKER, "n": len(bus), "origin": origin,
           "indicators": bus,
           "elapsed_s": round(time.time() - t0, 1)}
    S3.put_object(Bucket=BUCKET, Key="data/indicator-bus.json",
                  Body=json.dumps(doc).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=300")
    print("[bus] n=%d origin=%s %.0fs"
          % (len(bus), json.dumps(origin), doc["elapsed_s"]))
    return {"n": len(bus)}
