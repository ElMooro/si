"""justhodl-symbology-master — E1 v1 (ops 4441).

Nightly symbology mastering. v1 grounds the identifier spine in the SEC's
authoritative, free company_tickers.json: TICKER <-> CIK <-> NAME for every
SEC registrant (~10k+), the join key for EDGAR (E3), 13F CUSIP work, and
insider chains. Structure ships enrichment-ready: cusip/isin/figi/sedol
fields exist per record and populate as OpenFIGI (key-gated) and the 13F
cusip-map are wired in later E1 passes — absent identifiers are explicit
nulls, never invented. Coverage vs the Bloomberg 320k target is computed
honestly (that target includes global + delisted + funds; SEC registrants
are the US-listed operating spine).
Writes data/symbology/master.json + a raw snapshot (F4)."""
import json
import os
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
s3 = boto3.client("s3", region_name="us-east-1")
try:
    from raw_snapshot import snapshot
except Exception:
    snapshot = None


def lambda_handler(event, context):
    url = "https://www.sec.gov/files/company_tickers.json"
    req = urllib.request.Request(url, headers={
        "User-Agent": "JustHodl research admin@justhodl.ai"})
    with urllib.request.urlopen(req, timeout=30) as r:
        raw = r.read()
    raw_key = snapshot("sec", url, raw) if snapshot else None
    data = json.loads(raw)
    by_ticker, by_cik = {}, {}
    for rec in data.values():
        t = (rec.get("ticker") or "").upper()
        cik = str(rec.get("cik_str") or "").zfill(10)
        if not t:
            continue
        row = {"ticker": t, "cik": cik, "name": rec.get("title"),
               "cusip": None, "isin": None, "figi": None, "sedol": None,
               "source": {"kind": "sec", "url": url,
                          "raw_snapshot_key": raw_key}}
        by_ticker[t] = row
        by_cik.setdefault(cik, []).append(t)
    n = len(by_ticker)
    doc = {"as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "spec": "E1 v1 — SEC spine; OpenFIGI/CUSIP enrichment in later "
                   "passes (absent ids are explicit nulls, never invented)",
           "n_tickers": n, "n_ciks": len(by_cik),
           "coverage": {
               "vs_bloomberg_320k_pct": round(100 * n / 320000, 2),
               "note": "320k includes global+delisted+funds; SEC registrants "
                       "are the US-listed operating spine"},
           "enrichment_status": {"cik": "complete", "cusip": "pending "
                                 "(13f cusip-map join)", "figi": "pending "
                                 "(OpenFIGI key)", "isin": "pending",
                                 "sedol": "pending"},
           "by_ticker": by_ticker}
    s3.put_object(Bucket=BUCKET, Key="data/symbology/master.json",
                  Body=json.dumps(doc, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    res = {"ok": True, "n_tickers": n, "n_ciks": len(by_cik),
           "raw_key": raw_key}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
