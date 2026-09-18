"""justhodl-usgov-direct â€” Perplexity's flag actioned (ops 4466).

ADDITIVE originating-agency ingestion (existing engines untouched; FRED
untouched per Khalid's APR-0003 rejection):
  BEA  â€” key sat unused in SSM since ops 2821: GetDataSetList catalog
         (100%-pattern) + NIPA T10101 GDP proof-pull.
  BLS  â€” beyond the CES-only agent: CPI, PPI, JOLTS, productivity,
         unemployment (~20 series) via v2 POST with the SSM key.
  Fed DDP â€” H.15 full-package zip via candidate-chain; explicit fail if
         the shape differs.
All to data/warm/usgov/; F4 snapshots; keys never in code."""
import gzip
import json
import os
import urllib.request
from datetime import datetime, timezone

import boto3
from evidence_store import capture

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
        # ops 4467: 100%-worklist materialization â€” per-dataset
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
        _bea_walk(summary, k)
        summary["bea"] = {"ok": True, "datasets": len(names),
                          "gdp_rows": len(rows),
                          "param_map": {d: len(v) for d, v in
                                        pmap.items()}}
    except Exception as e:
        summary["bea"] = {"data_unavailable": True,
                          "reason": f"{type(e).__name__}: {str(e)[:70]}"}




def _bea_walk(summary, k):
    """ops 4476: the promised table-walk â€” NIPA's full table list via
    GetParameterValues, then cursor-pull 5 tables/run (Q, Year=ALL) to
    data/warm/usgov/bea/tables/NIPA/. 100%-of-dataset convergence."""
    base = "https://apps.bea.gov/api/data/"
    st_key = "data/_state/bea-walk.json"
    try:
        st = json.loads(s3.get_object(Bucket=BUCKET,
                                      Key=st_key)["Body"].read())
    except Exception:
        st = {"tables": None, "done": []}
    if not st.get("tables"):
        try:
            u = (f"{base}?UserID={k}&method=GetParameterValues"
                 "&datasetname=NIPA&ParameterName=TableName"
                 "&ResultFormat=JSON")
            r = json.loads(urllib.request.urlopen(u, timeout=45).read())
            vals = (r.get("BEAAPI", {}).get("Results", {})
                    .get("ParamValue") or [])
            st["tables"] = sorted({v.get("TableName") for v in vals
                                   if isinstance(v, dict)} - {None})
        except Exception as e:
            summary["bea_walk"] = {"data_unavailable": True,
                                   "reason": f"{type(e).__name__}: "
                                             f"{str(e)[:60]}"}
            return
    todo = [x for x in st["tables"] if x not in set(st["done"])][:5]
    got = 0
    for tb in todo:
        try:
            u = (f"{base}?UserID={k}&method=GetData&datasetname=NIPA"
                 f"&TableName={tb}&Frequency=Q&Year=ALL"
                 "&ResultFormat=JSON")
            raw = urllib.request.urlopen(u, timeout=90).read()
            rows = (json.loads(raw).get("BEAAPI", {}).get("Results", {})
                    .get("Data") or [])
            s3.put_object(Bucket=BUCKET,
                          Key=f"data/warm/usgov/bea/tables/NIPA/"
                              f"{tb}.json.gz",
                          Body=gzip.compress(json.dumps(
                              {"table": tb, "n_rows": len(rows),
                               "rows": rows}).encode()),
                          ContentType="application/gzip")
            st["done"].append(tb)
            got += 1
        except Exception as e:
            st.setdefault("failures", {})[tb] =                 f"{type(e).__name__}: {str(e)[:50]}"
    n = len(st["tables"] or [])
    nd = len(set(st["done"]))
    st["progress_pct"] = round(100 * nd / n, 1) if n else 0
    st["status"] = "COMPLETE" if nd >= n else "converging"
    s3.put_object(Bucket=BUCKET, Key=st_key,
                  Body=json.dumps(st, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    summary["bea_walk"] = {"tables_total": n, "pulled_this_run": got,
                           "done": nd, "progress_pct":
                               st["progress_pct"]}


def _bls(summary):
    key = _key("/justhodl/bls-api-key")
    year = datetime.now(timezone.utc).year
    # BLS caps a keyed request at 20 years (10 unkeyed). A 2000..2026
    # request was silently serving only 2000..2019, leaving CPI seven years old.
    start_year = year - (19 if key else 9)
    body = {"seriesid": BLS_SERIES, "startyear": str(start_year), "endyear": str(year)}
    if key: body["registrationkey"] = key
    url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    results = {}
    try:
        req = urllib.request.Request(url, data=json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json"})
        raw = urllib.request.urlopen(req, timeout=60).read()
        parsed = json.loads(raw)
        if parsed.get("status") != "REQUEST_SUCCEEDED":
            raise ValueError("BLS request not successful")
        proof = capture(s3, BUCKET, "bls", url, raw)
        series = {row.get("seriesID"): row for row in parsed.get("Results", {}).get("series", [])}
        for sid in BLS_SERIES:
            warm_key = f"data/warm/usgov/bls/{sid}.json.gz"
            try:
                fresh_rows = series.get(sid, {}).get("data") or []
                if not fresh_rows: raise ValueError("series absent or empty; previous archive retained")
                if any(not str(row.get("year", "")).isdigit() or not start_year <= int(row["year"]) <= year for row in fresh_rows):
                    raise ValueError("observation outside requested years")
                previous = None
                prior_proof = None
                condition = {"IfNoneMatch": "*"}
                try:
                    prior_object = s3.get_object(Bucket=BUCKET, Key=warm_key)
                    condition = {"IfMatch": prior_object["ETag"]}
                    old = prior_object["Body"].read()
                    old_bytes = gzip.decompress(old)
                    previous = json.loads(old_bytes)
                    if previous.get("series") != sid or not isinstance(previous.get("data"), list):
                        raise ValueError("existing series identity or history invalid")
                    prior_proof = capture(s3, BUCKET, "warehouse", "https://"+BUCKET+".s3.amazonaws.com/"+warm_key, old_bytes)
                except Exception as exc:
                    code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
                    if code not in ("NoSuchKey", "404"): raise
                merged = {(row["year"], row["period"]): row for row in (previous or {}).get("data", [])}
                for row in fresh_rows: merged[(row["year"], row["period"])] = row
                data = [merged[k] for k in sorted(merged, reverse=True)]
                doc = {"series": sid, "generated_at": _now_iso(), "schema_version": "2.0",
                       "raw_snapshot_key": proof["key"], "evidence": proof,
                       "prior_warehouse_evidence": prior_proof, "source_url": url,
                       "retrieval_window": {"start_year": start_year, "end_year": year},
                       "n_obs": len(data), "data": data,
                       "history_basis": "current response replaces matching periods; older observations retained from prior archive"}
                s3.put_object(Bucket=BUCKET, Key=warm_key, Body=gzip.compress(json.dumps(doc, allow_nan=False).encode()),
                              ContentType="application/gzip", **condition)
                monthly = [row for row in data if row.get("period") in {"M%02d" % m for m in range(1,13)}]
                latest = max(monthly, key=lambda row: (row["year"], row["period"])) if monthly else None
                results[sid] = {"status": "updated", "n_obs": len(data),
                                "latest_monthly_period": latest["year"]+"-"+latest["period"] if latest else None}
            except Exception as exc:
                results[sid] = {"status": "failed_previous_retained", "reason": type(exc).__name__+": "+str(exc)[:120]}
        summary["bls"] = {"ok": all(row["status"] == "updated" for row in results.values()),
                          "status": parsed["status"], "series": len(series), "keyed": bool(key),
                          "retrieval_window": {"start_year": start_year, "end_year": year}, "results": results}
    except Exception as exc:
        summary["bls"] = {"ok": False, "data_unavailable": True, "reason": type(exc).__name__+": "+str(exc)[:100]}


DDP_RELEASES = ["H15", "H41", "H8", "G19", "H10", "CP"]


def _fed_ddp(summary):
    """ops 4467: full core-release sweep â€” H.15 rates, H.4.1 balance
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
    if not isinstance(event, dict) or event.get("feed") != "bls":
        _bea(summary)
    _bls(summary)
    if not isinstance(event, dict) or event.get("feed") != "bls":
        _fed_ddp(summary)
    s3.put_object(Bucket=BUCKET,
                  Key="data/warm/usgov/latest-summary.json",
                  Body=json.dumps(summary, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    print(json.dumps(summary, default=str)[:600])
    return {"statusCode": 200, "body": json.dumps(summary, default=str)}
