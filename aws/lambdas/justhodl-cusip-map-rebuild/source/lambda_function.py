"""justhodl-cusip-map-rebuild — the AAPL-cohort fix (ops 4473).

Rebuild the cusip map from FULL 13F holdings: cursor over every 13F-HR in
the E3 EDGAR index (4,281 this quarter), fetch each filing's full
submission, regex the <infoTable> pairs (nameOfIssuer, cusip), merge into
data/13f-cusip-map-v2.json keyed by CUSIP with issuer name + mention
count. 15 filings/run, hourly → full quarter in ~12 days, but megacaps
(AAPL 037833100) appear in nearly EVERY filing so they land on run 1.
Explicit failures; F4 snapshots of filing bytes are skipped (size) but
accession ids are the provenance."""
import gzip
import json
import os
import re
import time
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
s3 = boto3.client("s3", region_name="us-east-1")
PER_RUN = int(os.environ.get("PER_RUN", "15"))
PAIR = re.compile(r"<nameOfIssuer>(.*?)</nameOfIssuer>.*?"
                  r"<cusip>\s*([0-9A-Za-z]{9})\s*</cusip>", re.S | re.I)


def lambda_handler(event, context):
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    state_key = "data/_state/cusip-rebuild.json"
    try:
        state = json.loads(s3.get_object(
            Bucket=BUCKET, Key=state_key)["Body"].read())
    except Exception:
        state = {"done_paths": [], "filings": None}
    if not state.get("filings"):
        try:
            state["filings"] = json.loads(gzip.decompress(s3.get_object(
                Bucket=BUCKET, Key=state_key + ".filings")
                ["Body"].read()))
        except Exception:
            pass
    if not state.get("filings"):
        idx = json.loads(gzip.decompress(s3.get_object(
            Bucket=BUCKET,
            Key="data/warm/edgar-filings/2026/QTR3.json.gz")
            ["Body"].read()))
        state["filings"] = [f["path"] for f in idx.get("filings", [])
                            if f.get("form") == "13F-HR"]
        state["n_total"] = len(state["filings"])
    try:
        vmap = json.loads(s3.get_object(
            Bucket=BUCKET,
            Key="data/13f-cusip-map-v2.json")["Body"].read())
    except Exception:
        vmap = {}
    done = set(state["done_paths"])
    todo = [p for p in state["filings"] if p not in done][:PER_RUN]
    parsed = pairs = errs = 0
    for path in todo:
        url = "https://www.sec.gov/Archives/" + path
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "JustHodl research admin@justhodl.ai"})
            raw = urllib.request.urlopen(req, timeout=60).read()
            txt = raw.decode("utf-8", "replace")
            for name, cus in PAIR.findall(txt):
                cus = cus.upper()
                e = vmap.get(cus) or {"name": name.strip()[:80],
                                      "count": 0}
                e["count"] += 1
                vmap[cus] = e
                pairs += 1
            parsed += 1
        except Exception as e:
            errs += 1
            state.setdefault("failures", {})[path] = \
                f"{type(e).__name__}: {str(e)[:50]}"
        state["done_paths"].append(path)
        time.sleep(0.4)
    nd = len(set(state["done_paths"]))
    state["as_of"] = now
    state["progress_pct"] = (round(100 * nd / state["n_total"], 1)
                             if state.get("n_total") else 0)
    state["status"] = ("COMPLETE" if nd >= state.get("n_total", 0)
                       else "converging")
    s3.put_object(Bucket=BUCKET, Key=state_key,
                  Body=json.dumps({k: v for k, v in state.items()
                                   if k != "filings"},
                                  default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    s3.put_object(Bucket=BUCKET, Key=state_key + ".filings",
                  Body=gzip.compress(json.dumps(
                      state["filings"]).encode()))
    # persist filings list separately to keep state small — reload next run
    s3.put_object(Bucket=BUCKET, Key="data/13f-cusip-map-v2.json",
                  Body=json.dumps(vmap, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    aapl = vmap.get("037833100")
    res = {"ok": True, "filings_parsed": parsed, "pairs": pairs,
           "errors": errs, "map_size": len(vmap),
           "done": nd, "of": state.get("n_total"),
           "AAPL_037833100": aapl}
    print(json.dumps(res, default=str))
    return {"statusCode": 200, "body": json.dumps(res, default=str)}
