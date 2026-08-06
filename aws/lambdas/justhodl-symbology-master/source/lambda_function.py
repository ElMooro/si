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




# ── ops 4469: CUSIP→ISIN→LEI chain (13F map + GLEIF cross-walk) ──────────
import io as _io
import zipfile as _zipfile




def _norm_name(s):
    s = (s or "").upper()
    for ch in ".,'&/()-":
        s = s.replace(ch, " ")
    drop = {"INC", "CORP", "CORPORATION", "CO", "COMPANY", "LTD", "PLC",
            "HOLDINGS", "HOLDING", "GROUP", "THE", "CLASS", "A", "B", "C",
            "COM", "NEW", "DEL", "TRUST", "LP", "SA", "NV", "AG"}
    toks = [w for w in s.split() if w and w not in drop]
    return " ".join(toks[:3])


def _isin_check_digit(body11):
    s = "".join(str(int(c, 36)) for c in body11)
    digits = [int(c) for c in s]
    total = 0
    dbl = True
    for d in reversed(digits):
        v = d * 2 if dbl else d
        total += v - 9 if v > 9 else v
        dbl = not dbl
    return str((10 - total % 10) % 10)


def enrich_cusip_chain(by_ticker):
    """Fill cusip (13F filings map), isin (US+cusip+check), lei (GLEIF
    ISIN->LEI file). Shape-flexible on the 13F map; explicit no_match."""
    stats = {"cusip": 0, "isin": 0, "lei": 0, "map_shape": None}
    try:
        m = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/13f-cusip-map.json")["Body"].read())
    except Exception as e:
        stats["error"] = f"13f map: {type(e).__name__}: {str(e)[:60]}"
        return stats
    cus_by_t = {}
    items = (m.items() if isinstance(m, dict) else
             [(None, x) for x in m] if isinstance(m, list) else [])
    for k, v in items:
        if isinstance(v, dict):
            cus = (v.get("cusip") or (k if k and len(str(k)) == 9
                                      else None))
            tkr = (v.get("ticker") or v.get("symbol") or "")
        else:
            cus, tkr = k, str(v)
        tkr = (tkr or "").upper().strip()
        if cus and tkr and len(str(cus)) == 9 and tkr in by_ticker:
            cus_by_t.setdefault(tkr, str(cus).upper())
    # ops 4470: pass 2 — name-normalized join for map rows whose ticker
    # field is absent (the AAPL cohort). Unique-match only; ambiguity
    # stays null rather than approximately right.
    name_to_t = {}
    for tkr, r in by_ticker.items():
        n = _norm_name(r.get("name"))
        if n:
            name_to_t.setdefault(n, []).append(tkr)
    name_joined = 0
    for k, v in items:
        if not isinstance(v, dict):
            continue
        cus = (v.get("cusip") or (k if k and len(str(k)) == 9 else None))
        if not cus or len(str(cus)) != 9:
            continue
        nm = _norm_name(v.get("name") or v.get("issuer")
                        or v.get("company"))
        cands = name_to_t.get(nm) or []
        if len(cands) == 1 and cands[0] not in cus_by_t:
            cus_by_t[cands[0]] = str(cus).upper()
            name_joined += 1
    stats["name_joined"] = name_joined
    stats["map_shape"] = (type(m).__name__ + f"/{len(cus_by_t)} joinable")
    want_isin = {}
    for tkr, cus in cus_by_t.items():
        r = by_ticker[tkr]
        if r.get("cusip") is None:
            r["cusip"] = cus
            stats["cusip"] += 1
        body = "US" + cus
        isin = body + _isin_check_digit(body)
        if r.get("isin") is None:
            r["isin"] = isin
            stats["isin"] += 1
        want_isin[isin] = tkr
    if want_isin:
        try:
            zb = s3.get_object(Bucket=BUCKET,
                               Key="data/warm/gleif/isin-lei-latest.zip"
                               )["Body"].read()
            zf = _zipfile.ZipFile(_io.BytesIO(zb))
            name = zf.namelist()[0]
            with zf.open(name) as fh:
                header = fh.readline().decode("utf-8",
                                              "replace").strip()
                cols = [c.strip().strip('"').upper()
                        for c in header.split(",")]
                try:
                    ii = cols.index("ISIN")
                    li = cols.index("LEI")
                except ValueError:
                    ii, li = 1, 0
                for line in fh:
                    parts = line.decode("utf-8", "replace")                         .strip().split(",")
                    if len(parts) <= max(ii, li):
                        continue
                    isin = parts[ii].strip().strip('"')
                    tkr = want_isin.get(isin)
                    if tkr and by_ticker[tkr].get("lei") is None:
                        by_ticker[tkr]["lei"] =                             parts[li].strip().strip('"')
                        stats["lei"] += 1
        except Exception as e:
            stats["gleif_error"] = (f"{type(e).__name__}: "
                                    f"{str(e)[:60]}")
    return stats


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
               "lei": None,
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
                          "isin", "sedol", "lei"):
                    if old_r.get(f) is not None:
                        by_ticker[tkr][f] = old_r[f]
    except Exception:
        pass
    figi_stats = enrich_figi(by_ticker)
    cusip_stats = enrich_cusip_chain(by_ticker)
    n = len(by_ticker)
    doc = {"as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "spec": "E1 v1 — SEC spine; OpenFIGI/CUSIP enrichment in later "
                   "passes (absent ids are explicit nulls, never invented)",
           "n_tickers": n, "n_ciks": len(by_cik),
           "coverage": {
               "vs_bloomberg_320k_pct": round(100 * n / 320000, 2),
               "note": "320k includes global+delisted+funds; SEC registrants "
                       "are the US-listed operating spine"},
           "enrichment_status": {"cik": "complete", "cusip_chain": cusip_stats, "figi": figi_stats, "isin": "pending",
                                 "sedol": "pending"},
           "by_ticker": by_ticker}
    s3.put_object(Bucket=BUCKET, Key="data/symbology/master.json",
                  Body=json.dumps(doc, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    res = {"ok": True, "n_tickers": n, "n_ciks": len(by_cik),
           "raw_key": raw_key}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
