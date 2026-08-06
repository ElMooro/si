"""justhodl-global-expansion — every missing provider from Khalid's doc
(ops 4486). Eleven providers, one engine, generic fetchers, per-provider
explicit status. Keys read from SSM slots; absent key = named missing,
never fabricated. Large files stream via /tmp (4GB ephemeral) with sha256.
  banxico    FX/TIIE (token /justhodl/banxico-token)
  statcan    full cube catalog (the 100%% worklist)
  eiopa      monthly RFR zip (candidate-chain)
  occ        daily volume (candidate-chain)
  sec-dera   Financial Statement Data Set qtr zip
  sec-midas  market-structure metrics zip
  gdelt      latest 15-min export (lastupdate pointer)
  boe        SONIA full history CSV
  entsoe     EU load sample (token /justhodl/entsoe-token)
  copernicus ERA5 (key /justhodl/cds-key; else stated APR)
  nasa-power US-midwest T2M/precip daily json
"""
import gzip
import hashlib
import json
import os
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
s3 = boto3.client("s3", region_name="us-east-1")
try:
    from raw_snapshot import snapshot
except Exception:
    snapshot = None

_ssm = {}


def _key(name):
    if name not in _ssm:
        try:
            c = boto3.client("ssm", region_name="us-east-1")
            _ssm[name] = c.get_parameter(
                Name=name, WithDecryption=True)["Parameter"]["Value"]
        except Exception:
            _ssm[name] = None
    return _ssm[name]


def _fetch(u, headers=None, timeout=90):
    h = {"User-Agent": "JustHodl research admin@justhodl.ai"}
    h.update(headers or {})
    req = urllib.request.Request(u, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def _stream_zip(u, out_key, headers=None, min_bytes=100_000):
    tmp = "/tmp/dl.bin"
    h = {"User-Agent": "JustHodl research admin@justhodl.ai"}
    h.update(headers or {})
    req = urllib.request.Request(u, headers=h)
    sha = hashlib.sha256()
    size = 0
    with urllib.request.urlopen(req, timeout=240) as r, \
            open(tmp, "wb") as f:
        while True:
            c = r.read(8 * 1024 * 1024)
            if not c:
                break
            f.write(c)
            sha.update(c)
            size += len(c)
    if size < min_bytes:
        raise ValueError(f"too small: {size}b")
    s3.upload_file(tmp, BUCKET, out_key)
    try:
        os.remove(tmp)
    except OSError:
        pass
    return {"ok": True, "bytes": size,
            "mb": round(size / 1e6, 1), "sha256": sha.hexdigest()[:16],
            "key": out_key, "source": u[:100]}


def _json_store(name, cands, out_key, headers=None, wrap_note=None):
    last = "none"
    for u in cands:
        try:
            raw = _fetch(u, headers=headers)
            d = json.loads(raw)
            rk = snapshot(name, u, raw[:400000]) if snapshot else None
            s3.put_object(Bucket=BUCKET, Key=out_key,
                          Body=gzip.compress(json.dumps(
                              {"source_url": u,
                               "raw_snapshot_key": rk,
                               "note": wrap_note,
                               "payload": d}).encode()),
                          ContentType="application/gzip")
            n = (len(d) if isinstance(d, list) else
                 len(d.get("object", d.get("data", d.get(
                     "series", d.get("features", d))))
                     if isinstance(d, dict) else 0))
            return {"ok": True, "items": n, "key": out_key,
                    "source": u[:100]}
        except Exception as e:
            last = f"{u[:60]} -> {type(e).__name__}: {str(e)[:50]}"
    return {"data_unavailable": True, "reason": last}


def _csv_store(name, cands, out_key, headers=None, min_bytes=2000):
    last = "none"
    for u in cands:
        try:
            raw = _fetch(u, headers=headers, timeout=120)
            if len(raw) < min_bytes:
                raise ValueError(f"small {len(raw)}b")
            rk = snapshot(name, u, raw[:400000]) if snapshot else None
            s3.put_object(Bucket=BUCKET, Key=out_key,
                          Body=gzip.compress(raw),
                          ContentType="application/gzip")
            return {"ok": True, "bytes": len(raw),
                    "lines": raw.count(b"\n"), "key": out_key,
                    "raw_snapshot_key": rk, "source": u[:100]}
        except Exception as e:
            last = f"{u[:60]} -> {type(e).__name__}: {str(e)[:50]}"
    return {"data_unavailable": True, "reason": last}


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    S = {"as_of": now.isoformat(timespec="seconds")}
    only = (event or {}).get("only")

    def want(n):
        return (not only) or n == only

    if want("banxico"):
        tok = _key("/justhodl/banxico-token")
        if tok:
            S["banxico"] = _json_store(
                "banxico",
                ["https://www.banxico.org.mx/SieAPIRest/service/v1/"
                 "series/SF43718,SF61745,SP68257/datos"],
                "data/warm/banxico/core-series.json.gz",
                headers={"Bmx-Token": tok},
                wrap_note="FIX rate, TIIE28, CPI")
        else:
            S["banxico"] = {"data_unavailable": True,
                            "reason": "no token at /justhodl/"
                                      "banxico-token (free signup)"}
    if want("statcan"):
        S["statcan"] = _json_store(
            "statcan",
            ["https://www150.statcan.gc.ca/t1/wds/rest/"
             "getAllCubesListLite"],
            "data/warm/statcan/cube-catalog.json.gz",
            wrap_note="full cube catalog = 100% worklist")
    if want("eiopa"):
        ym = (now.replace(day=1) - timedelta(days=1))
        tag = ym.strftime("%Y%m%d")
        folders = [now.strftime("%Y-%m"), ym.strftime("%Y-%m")]
        names = [f"eiopa_rfr_{tag}.zip", f"eiopa_rfr_{tag}_0.zip",
                 f"EIOPA_RFR_{tag}.zip",
                 f"eiopa_rfr_term_structures_{tag}.zip"]
        S["eiopa"] = _csv_store(
            "eiopa",
            [f"https://www.eiopa.europa.eu/system/files/{f}/{n}"
             for f in folders for n in names],
            "data/warm/eiopa/rfr-latest.zip.gz", min_bytes=100_000)
    if want("occ"):
        S["occ"] = _csv_store(
            "occ",
            ["https://marketdata.theocc.com/mdapi/download-daily"
             "-volume",
             "https://marketdata.theocc.com/mdapi/daily-volume",
             "https://www.theocc.com/api/market-data/volume/daily",
             "https://marketdata.theocc.com/mdapi/volume-totals"],
            "data/warm/occ/daily-volume.csv.gz",
            headers={"Accept": "text/csv,application/json,*/*",
                     "Referer": "https://www.theocc.com/"
                                "market-data/volume",
                     "Origin": "https://www.theocc.com"})
    if want("sec_dera"):
        qy = []
        y, q = now.year, (now.month - 1) // 3 + 1
        for _ in range(5):
            q -= 1
            if q == 0:
                q, y = 4, y - 1
            qy.append((y, q))
        last = "none"
        for y2, q2 in qy:
            try:
                S["sec_dera"] = _stream_zip(
                    f"https://www.sec.gov/files/dera/data/"
                    f"financial-statement-data-sets/{y2}q{q2}.zip",
                    f"data/warm/sec-dera/fsds-{y2}q{q2}.zip",
                    min_bytes=5_000_000)
                break
            except Exception as e:
                last = f"{y2}q{q2}: {type(e).__name__}: {str(e)[:40]}"
        else:
            S["sec_dera"] = {"data_unavailable": True, "reason": last}
    qy2 = qy if "qy" in dir() else []
    if want("sec_midas"):
        S["sec_midas"] = _csv_store(
            "sec_midas",
            [f"https://www.sec.gov/files/opa/data/market-structure/"
             f"metrics-individual-security-and-exchange/"
             f"individual_security_exchange_{yy}_q{qq}.zip"
             for yy, qq in (qy if want("sec_dera") else [(now.year,1),(now.year-1,4),(now.year-1,3)])] +
            [f"https://www.sec.gov/files/data/market-structure/"
             f"metrics-by-individual-security/"
             f"individual_security_{yy}_q{qq}.zip"
             for yy, qq in (qy if want("sec_dera") else [(now.year,1),(now.year-1,4),(now.year-1,3)])],
            "data/warm/sec-midas/latest.zip.gz", min_bytes=500_000)
    if want("gdelt"):
        try:
            ptr = _fetch("http://data.gdeltproject.org/gdeltv2/"
                         "lastupdate.txt").decode()
            url = next((ln.split()[-1] for ln in ptr.splitlines()
                        if ln.endswith(".export.CSV.zip")), None)
            if not url:
                raise ValueError("no export url in pointer")
            S["gdelt"] = _stream_zip(
                url, "data/warm/gdelt/latest-export.zip",
                min_bytes=5_000)
        except Exception as e:
            S["gdelt"] = {"data_unavailable": True,
                          "reason": f"{type(e).__name__}: "
                                    f"{str(e)[:60]}"}
    if want("boe"):
        S["boe"] = _csv_store(
            "boe",
            ["https://www.bankofengland.co.uk/boeapps/database/"
             "fromshowcolumns.asp?csv.x=yes&SeriesCodes=IUDSOIA"
             "&CSVF=TN&UsingCodes=Y&Datefrom=01/Jan/2015"
             "&Dateto=now&VPD=Y&VFD=N"],
            "data/warm/boe/sonia-history.csv.gz")
    if want("entsoe"):
        tok = _key("/justhodl/entsoe-token")
        if tok:
            per = now.strftime("%Y%m%d")
            S["entsoe"] = _csv_store(
                "entsoe",
                [f"https://web-api.tp.entsoe.eu/api?securityToken="
                 f"{tok}&documentType=A65&processType=A16"
                 f"&outBiddingZone_Domain=10Y1001A1001A83F"
                 f"&periodStart={per}0000&periodEnd={per}2300"],
                "data/warm/entsoe/de-load-sample.xml.gz",
                min_bytes=500)
        else:
            S["entsoe"] = {"data_unavailable": True,
                           "reason": "no token at /justhodl/"
                                     "entsoe-token (free signup)"}
    if want("copernicus"):
        S["copernicus"] = ({"data_unavailable": True,
                            "reason": "needs CDS key at /justhodl/"
                                      "cds-key + heavy retrieval — "
                                      "filed as APR path, not faked"}
                           if not _key("/justhodl/cds-key") else
                           {"data_unavailable": True,
                            "reason": "key present; retrieval job = "
                                      "E10 task (multi-GB)"})
    if want("nasa_power"):
        S["nasa_power"] = _json_store(
            "nasa_power",
            ["https://power.larc.nasa.gov/api/temporal/daily/point"
             "?parameters=T2M,PRECTOTCORR&community=AG"
             "&longitude=-95&latitude=40&start=20240101"
             f"&end={now.strftime('%Y%m%d')}&format=JSON"],
            "data/warm/nasa-power/midwest-daily.json.gz",
            wrap_note="US midwest ag-weather proxy")
    s3.put_object(Bucket=BUCKET,
                  Key="data/warm/global-expansion-summary.json",
                  Body=json.dumps(S, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")
    res = {"ok": True,
           "live": [k for k, v in S.items()
                    if isinstance(v, dict) and v.get("ok")],
           "missing": {k: v.get("reason") for k, v in S.items()
                       if isinstance(v, dict)
                       and v.get("data_unavailable")}}
    print(json.dumps(res, default=str)[:700])
    return {"statusCode": 200, "body": json.dumps(res, default=str)}
