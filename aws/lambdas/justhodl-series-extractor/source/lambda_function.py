"""justhodl-series-extractor (ops 4547) — Perplexity's reference
implementation: parse EVERY series out of a provider's raw warm files,
not just list the files. Eurostat first (proves the pattern), StatCan
and OECD next with their own readers, same output contract.

GUARDRAILS (both explicit, both enforced):
1. series_extracted is counted honestly and kept SEPARATE from
   datasets_total until a flow is fully parsed — never folds the
   theoretical dimension cross-product in as if it were rows read.
2. Output is hard-paginated (500/page) and the UI is told to require
   a 2+ char search before rendering — this writes the data, it does
   not render it unfiltered.
"""
import csv
import gzip
import io
import json
import os
import time
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
s3 = boto3.client("s3", region_name="us-east-1")
PAGE = 500
BUDGET_S = 220  # stay well inside a 5-min cadence; resumable


def _flow_id_from_key(key):
    # data/warm/eurostat/data/NAMA_10_A64_P5.dat.gz -> NAMA_10_A64_P5
    base = key.rsplit("/", 1)[-1]
    for suf in (".dat.gz", ".gz", ".tsv.gz", ".dat"):
        if base.endswith(suf):
            return base[: -len(suf)]
    return base


def extract_eurostat(gz_bytes, flow_id, engine_index):
    """Eurostat TSV: header 'dim1,dim2,...,dimN\\TIME_PERIOD<TAB>p1<TAB>p2...'
    one row per unique dimension-combination = one series."""
    text = gzip.decompress(gz_bytes).decode("utf-8", errors="ignore")
    f = io.StringIO(text)
    hdr = f.readline().rstrip("\n")
    dims_part, sep, periods_part = hdr.partition("\\TIME_PERIOD")
    if not sep:
        return
    dims = [d.strip() for d in dims_part.split(",")]
    periods = [p.strip() for p in periods_part.split("\t") if p.strip()]
    unit_ix = dims.index("unit") if "unit" in dims else None
    geo_ix = -1 if dims else None
    for line in f:
        line = line.rstrip("\n")
        if not line:
            continue
        keypart, tab, valpart = line.partition("\t")
        if not tab:
            continue
        dimvals = [d.strip() for d in keypart.split(",")]
        vals = [v.strip() for v in valpart.split("\t")]
        last_i = None
        for i in range(len(vals) - 1, -1, -1):
            if i < len(vals) and vals[i] not in (":", "", ": ", ":u",
                                                  ":c", ":e"):
                last_i = i
                break
        if last_i is None or last_i >= len(periods):
            continue
        try:
            lv = float(vals[last_i].split(" ")[0])
        except ValueError:
            continue
        sid = "eurostat:" + flow_id + ":" + ".".join(dimvals)
        yield {
            "id": sid,
            "flow": flow_id,
            "name": flow_id + " · " + " · ".join(dimvals[:3]) +
            (" …" if len(dimvals) > 3 else ""),
            "dims": dict(zip(dims, dimvals)),
            "unit": (dimvals[unit_ix] if unit_ix is not None and
                     unit_ix < len(dimvals) else None),
            "freq": dimvals[0] if dimvals else None,
            "geo": dimvals[geo_ix] if dimvals else None,
            "first_obs": periods[0] if periods else None,
            "last_obs": periods[last_i],
            "last_value": lv,
            "status": "LIVE",
            "source_url": ("https://ec.europa.eu/eurostat/databrowser"
                           "/view/" + flow_id),
            "raw_key": None,  # filled by caller
            "engines": engine_index.get("eurostat:" + flow_id, []),
        }


EXTRACTORS = {"eurostat": extract_eurostat}


def _load_engine_index():
    try:
        ov = s3.get_object(Bucket=BUCKET,
                           Key="data/audit/engine-writes-overrides.json")
        d = json.loads(ov["Body"].read())
        idx = {}
        for eng, ws in (d.get("writes") or {}).items():
            for w in ws:
                idx.setdefault(w, []).append(eng)
        return idx
    except Exception:
        return {}


def lambda_handler(event, context):
    t0 = time.time()
    provider = (event or {}).get("provider", "eurostat")
    extractor = EXTRACTORS.get(provider)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    if not extractor:
        return {"statusCode": 400,
                "body": json.dumps({"error": f"no extractor for "
                                             f"{provider} yet"})}
    state_key = f"data/_state/series-extract-{provider}.json"
    try:
        state = json.loads(s3.get_object(Bucket=BUCKET,
                                         Key=state_key)["Body"]
                           .read())
    except Exception:
        state = {"flows_done": [], "series_count": 0, "n_pages": 0,
                 "buffer": []}
    engine_index = _load_engine_index()
    prefix = f"data/warm/{provider}/data/"
    tok = None
    all_keys = []
    while True:
        kw = {"Bucket": BUCKET, "Prefix": prefix, "MaxKeys": 1000}
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        all_keys.extend(o["Key"] for o in r.get("Contents", []))
        if not r.get("IsTruncated"):
            break
        tok = r.get("NextContinuationToken")
    todo = [k for k in all_keys
            if _flow_id_from_key(k) not in state["flows_done"]]
    buf = state.get("buffer", [])
    processed_flows = []
    for key in todo:
        if time.time() - t0 > BUDGET_S:
            break
        fid = _flow_id_from_key(key)
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            raw = obj["Body"].read()
            n_added = 0
            for rec in extractor(raw, fid, engine_index):
                rec["raw_key"] = key
                buf.append(rec)
                n_added += 1
                if len(buf) >= PAGE:
                    s3.put_object(
                        Bucket=BUCKET,
                        Key=(f"data/providers/{provider}/series/"
                             f"page-{state['n_pages']:04d}.json"),
                        Body=json.dumps({"page": state["n_pages"],
                                        "count": len(buf),
                                        "rows": buf},
                                       default=str).encode(),
                        ContentType="application/json",
                        CacheControl="no-cache")
                    state["n_pages"] += 1
                    state["series_count"] += len(buf)
                    buf = []
            state["flows_done"].append(fid)
            processed_flows.append((fid, n_added))
        except Exception as e:
            state.setdefault("errors", {})[fid] = (f"{type(e).__name__}"
                                                    f": {str(e)[:80]}")
    state["buffer"] = buf
    state["updated_at"] = now
    s3.put_object(Bucket=BUCKET, Key=state_key,
                  Body=json.dumps(state, default=str).encode(),
                  ContentType="application/json")
    # manifest: honest, SEPARATE count. Never folds into datasets_total.
    manifest = {
        "provider": provider,
        "updated_at": now,
        "flows_total": len(all_keys),
        "flows_parsed": len(state["flows_done"]),
        "series_extracted": state["series_count"] + len(buf),
        "n_pages": state["n_pages"],
        "page_size": PAGE,
        "note": ("series_extracted is a SEPARATE metric from the "
                "provider's datasets/datasets_total headline — it "
                "is not folded in until fully parsed (ops 4547)."),
        "ui_rule": "require 2+ char search before rendering rows"}
    s3.put_object(Bucket=BUCKET,
                  Key=f"data/providers/{provider}/series-manifest.json",
                  Body=json.dumps(manifest, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")
    res = {"ok": True, "provider": provider,
          "processed_flows": len(processed_flows),
          "series_this_run": sum(n for _, n in processed_flows),
          **manifest}
    print(json.dumps({k: res[k] for k in
                      ("processed_flows", "series_this_run",
                       "series_extracted", "flows_parsed",
                       "flows_total")}))
    return {"statusCode": 200, "body": json.dumps(res, default=str)}
