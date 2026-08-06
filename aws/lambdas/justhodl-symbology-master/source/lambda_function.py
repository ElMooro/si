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

import time

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
s3 = boto3.client("s3", region_name="us-east-1")
try:
    from raw_snapshot import snapshot
except Exception:
    snapshot = None




# ── ops 4442: FIGI enrichment (OpenFIGI v3, key in SSM — repo is public) ──
_ssm_cache = {}


def _figi_key():
    if "k" not in _ssm_cache:
        try:
            ssm = boto3.client("ssm", region_name="us-east-1")
            _ssm_cache["k"] = ssm.get_parameter(
                Name="/justhodl/openfigi/api-key",
                WithDecryption=True)["Parameter"]["Value"]
        except Exception as e:
            print("figi key unavailable:", str(e)[:60])
            _ssm_cache["k"] = None
    return _ssm_cache["k"]


def enrich_figi(by_ticker, limit=2500):
    """Fill null FIGIs via OpenFIGI v3 mapping. Batch 100 jobs/request,
    polite pacing, bounded per run — converges to full coverage across
    nightly runs. Unmatched tickers get figi_status='no_match' (explicit,
    never invented)."""
    key = _figi_key()
    if not key:
        return {"enriched": 0, "note": "no key in SSM"}
    todo = [t for t, r in by_ticker.items()
            if r.get("figi") is None and r.get("figi_status") != "no_match"]
    todo = todo[:limit]
    done = no_match = errors = 0
    for i in range(0, len(todo), 100):
        batch = todo[i:i + 100]
        jobs = [{"idType": "TICKER", "idValue": t, "exchCode": "US"}
                for t in batch]
        req = urllib.request.Request(
            "https://api.openfigi.com/v3/mapping",
            data=json.dumps(jobs).encode(),
            headers={"Content-Type": "application/json",
                     "X-OPENFIGI-APIKEY": key})
        try:
            with urllib.request.urlopen(req, timeout=25) as r:
                res = json.loads(r.read())
        except Exception as e:
            errors += 1
            print("figi batch err:", str(e)[:80])
            time.sleep(3)
            continue
        for t, item in zip(batch, res):
            d = (item.get("data") or [None])[0] if isinstance(item, dict)                 else None
            if d and d.get("figi"):
                by_ticker[t]["figi"] = d["figi"]
                by_ticker[t]["figi_name"] = d.get("name")
                done += 1
            else:
                by_ticker[t]["figi_status"] = "no_match"
                no_match += 1
        time.sleep(0.35)
    return {"enriched": done, "no_match": no_match, "errors": errors,
            "remaining_null": sum(1 for r in by_ticker.values()
                                  if r.get("figi") is None
                                  and r.get("figi_status") != "no_match")}


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
    # carry forward FIGIs already resolved in prior runs (progressive)
    try:
        prev = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/symbology/master.json")["Body"].read())
        for tkr, old_r in (prev.get("by_ticker") or {}).items():
            if tkr in by_ticker:
                for f in ("figi", "figi_name", "figi_status", "cusip",
                          "isin", "sedol"):
                    if old_r.get(f) is not None:
                        by_ticker[tkr][f] = old_r[f]
    except Exception:
        pass
    figi_stats = enrich_figi(by_ticker)
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
                                 "(13f cusip-map join)", "figi": figi_stats, "isin": "pending",
                                 "sedol": "pending"},
           "by_ticker": by_ticker}
    s3.put_object(Bucket=BUCKET, Key="data/symbology/master.json",
                  Body=json.dumps(doc, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    res = {"ok": True, "n_tickers": n, "n_ciks": len(by_cik),
           "raw_key": raw_key}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
