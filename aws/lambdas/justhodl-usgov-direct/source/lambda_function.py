"""justhodl-usgov-direct — Perplexity's flag actioned (ops 4466).

ADDITIVE originating-agency ingestion (existing engines untouched; FRED
untouched per Khalid's APR-0003 rejection):
  BEA  — key sat unused in SSM since ops 2821: GetDataSetList catalog
         (100%-pattern) + NIPA T10101 GDP proof-pull.
  BLS  — beyond the CES-only agent: CPI, PPI, JOLTS, productivity,
         unemployment (~20 series) via v2 POST with the SSM key.
  Fed DDP — H.15 full-package zip via candidate-chain; explicit fail if
         the shape differs.
All to data/warm/usgov/; F4 snapshots; keys never in code."""
import gzip
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

_ssm = {}


def _key(name):
    if name not in _ssm:
        try:
            c = boto3.client("ssm", region_name="us-east-1")
            _ssm[name] = c.get_parameter(
                Name=name, WithDecryption=True)["Parameter"]["Value"]
        except Exception as e:
            print(f"{name}: {str(e)[:60]}")
            _ssm[name] = None
    return _ssm[name]


BLS_SERIES = ["CUUR0000SA0", "CUUR0000SA0L1E", "CUSR0000SA0",
              "WPUFD4", "WPUFD49104", "WPSFD4",
              "PRS85006092", "PRS85006112",
              "LNS14000000", "LNS12300000", "LNS11300000",
              "JTS000000000000000JOL", "JTS000000000000000QUL",
              "JTS000000000000000HIL", "CES0500000003"]


def _now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _bea(summary):
    k = _key("/justhodl/bea-api-key")
    if not k:
        summary["bea"] = {"data_unavailable": True,
                          "reason": "no key at /justhodl/bea-api-key"}
        return
    base = "https://apps.bea.gov/api/data/"
    try:
        u = (f"{base}?UserID={k}&method=GETDATASETLIST"
             "&ResultFormat=JSON")
        raw = urllib.request.urlopen(u, timeout=45).read()
        rk = snapshot("bea", u.replace(k, "***"), raw) if snapshot else None
        ds = (json.loads(raw).get("BEAAPI", {}).get("Results", {})
              .get("Dataset") or [])
        names = [d.get("DatasetName") for d in ds]
        u2 = (f"{base}?UserID={k}&method=GetData&datasetname=NIPA"
              "&TableName=T10101&Frequency=Q&Year=ALL&ResultFormat=JSON")
        raw2 = urllib.request.urlopen(u2, timeout=60).read()
        rk2 = (snapshot("bea", u2.replace(k, "***"), raw2)
               if snapshot else None)
        rows = (json.loads(raw2).get("BEAAPI", {}).get("Results", {})
                .get("Data") or [])
        s3.put_object(Bucket=BUCKET,
                      Key="data/warm/usgov/bea/nipa-t10101.json.gz",
                      Body=gzip.compress(json.dumps(
                          {"table": "T10101 (Real GDP % change)",
                           "raw_snapshot_key": rk2,
                           "n_rows": len(rows),
                           "rows": rows}).encode()),
                      ContentType="application/gzip")
        s3.put_object(Bucket=BUCKET,
                      Key="data/warm/usgov/bea/catalog.json",
                      Body=json.dumps({"datasets": names,
                                       "raw_snapshot_key": rk}).encode(),
                      ContentType="application/json")
        # ops 4467: 100%-worklist materialization — per-dataset
        # parameter map (the walkable universe, stored once)
        pmap = {}
        for dn in names:
            try:
                u3 = (f"{base}?UserID={k}&method=GetParameterList"
                      f"&datasetname={dn}&ResultFormat=JSON")
                r3 = json.loads(urllib.request.urlopen(
                    u3, timeout=30).read())
                params = (r3.get("BEAAPI", {}).get("Results", {})
                          .get("Parameter") or [])
                pmap[dn] = [p.get("ParameterName") for p in params
                            if isinstance(p, dict)]
            except Exception as e:
                pmap[dn] = [f"err: {type(e).__name__}"]
        s3.put_object(Bucket=BUCKET,
                      Key="data/warm/usgov/bea/parameter-map.json",
                      Body=json.dumps({"as_of": _now_iso(),
                                       "map": pmap}).encode(),
                      ContentType="application/json")
        summary["bea"] = {"ok": True, "datasets": len(names),
                          "gdp_rows": len(rows),
                          "param_map": {d: len(v) for d, v in
                                        pmap.items()}}
    except Exception as e:
        summary["bea"] = {"data_unavailable": True,
                          "reason": f"{type(e).__name__}: {str(e)[:70]}"}


def _bls(summary):
    k = _key("/justhodl/bls-api-key")
    body = {"seriesid": BLS_SERIES, "startyear": "2000",
            "endyear": str(datetime.now().year)}
    if k:
        body["registrationkey"] = k
    try:
        req = urllib.request.Request(
            "https://api.bls.gov/publicAPI/v2/timeseries/data/",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"})
        raw = urllib.request.urlopen(req, timeout=60).read()
        rk = (snapshot("bls", "api.bls.gov/v2 batch", raw)
              if snapshot else None)
        d = json.loads(raw)
        series = d.get("Results", {}).get("series") or []
        n_obs = 0
        for sr in series:
            sid = sr.get("seriesID")
            data = sr.get("data") or []
            n_obs += len(data)
            s3.put_object(
                Bucket=BUCKET,
                Key=f"data/warm/usgov/bls/{sid}.json.gz",
                Body=gzip.compress(json.dumps(
                    {"series": sid, "raw_snapshot_key": rk,
                     "n_obs": len(data), "data": data}).encode()),
                ContentType="application/gzip")
        summary["bls"] = {"ok": d.get("status") == "REQUEST_SUCCEEDED",
                          "status": d.get("status"),
                          "series": len(series), "obs": n_obs,
                          "keyed": bool(k)}
    except Exception as e:
        summary["bls"] = {"data_unavailable": True,
                          "reason": f"{type(e).__name__}: {str(e)[:70]}"}


DDP_RELEASES = ["H15", "H41", "H8", "G19", "H10", "CP"]


def _fed_ddp(summary):
    """ops 4467: full core-release sweep — H.15 rates, H.4.1 balance
    sheet, H.8 bank credit, G.19 consumer credit, H.10 FX, CP paper."""
    out = {}
    for rel in DDP_RELEASES:
        u = ("https://www.federalreserve.gov/datadownload/"
             f"Output.aspx?rel={rel}&filetype=zip")
        try:
            req = urllib.request.Request(u, headers={
                "User-Agent": "JustHodl research admin@justhodl.ai"})
            raw = urllib.request.urlopen(req, timeout=90).read()
            if len(raw) < 2000:
                raise ValueError(f"too small ({len(raw)}b)")
            rk = snapshot("fed-ddp", u, raw) if snapshot else None
            s3.put_object(Bucket=BUCKET,
                          Key=f"data/warm/usgov/fed-ddp/{rel}.zip",
                          Body=raw)
            out[rel] = {"ok": True, "bytes": len(raw),
                        "raw_snapshot_key": rk}
        except Exception as e:
            out[rel] = {"data_unavailable": True,
                        "reason": f"{type(e).__name__}: {str(e)[:60]}"}
    summary["fed_ddp"] = out


def lambda_handler(event, context):
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    summary = {"as_of": now}
    _bea(summary)
    _bls(summary)
    _fed_ddp(summary)
    s3.put_object(Bucket=BUCKET,
                  Key="data/warm/usgov/latest-summary.json",
                  Body=json.dumps(summary, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    print(json.dumps(summary, default=str)[:600])
    return {"statusCode": 200, "body": json.dumps(summary, default=str)}
