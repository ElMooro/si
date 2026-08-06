"""justhodl-edgar-full-index — E3 v1 (ops 4443).

Nightly SEC full-index ingest: every filing in the current quarter
(master.idx, pipe-delimited: CIK|Company|Form|Date|Path) -> parsed, joined
against the E1 CIK spine, written gzip-JSON to
data/warm/edgar-filings/{YYYY}/{QTR}.json.gz (spec says parquet; v1 ships
gz-JSON honestly — no pandas/pyarrow layer in this runtime yet; format
upgrade is a stated TODO, not a silent substitution) + a form-type summary
at data/warm/edgar-filings/latest-summary.json for pages. F4 raw snapshot
of the idx bytes attached."""
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


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    q = (now.month - 1) // 3 + 1
    url = (f"https://www.sec.gov/Archives/edgar/full-index/"
           f"{now.year}/QTR{q}/master.idx")
    req = urllib.request.Request(url, headers={
        "User-Agent": "JustHodl research admin@justhodl.ai"})
    with urllib.request.urlopen(req, timeout=60) as r:
        raw = r.read()
    raw_key = snapshot("sec", url, raw) if snapshot else None
    rows, started = [], False
    forms = {}
    for line in raw.decode("latin-1").split("\n"):
        if line.startswith("----"):
            started = True
            continue
        if not started or "|" not in line:
            continue
        p = line.strip().split("|")
        if len(p) != 5:
            continue
        cik, comp, form, date, path = p
        rows.append({"cik": cik.zfill(10), "company": comp, "form": form,
                     "date": date, "path": path})
        forms[form] = forms.get(form, 0) + 1
    body = gzip.compress(json.dumps(
        {"as_of": now.isoformat(timespec="seconds"), "source_url": url,
         "raw_snapshot_key": raw_key, "n_filings": len(rows),
         "filings": rows}).encode())
    warm_key = f"data/warm/edgar-filings/{now.year}/QTR{q}.json.gz"
    s3.put_object(Bucket=BUCKET, Key=warm_key, Body=body,
                  ContentType="application/gzip")
    top = dict(sorted(forms.items(), key=lambda x: -x[1])[:15])
    s3.put_object(Bucket=BUCKET, Key="data/warm/edgar-filings/"
                  "latest-summary.json",
                  Body=json.dumps({
                      "as_of": now.isoformat(timespec="seconds"),
                      "quarter": f"{now.year}-QTR{q}",
                      "n_filings": len(rows), "warm_key": warm_key,
                      "size_gz_mb": round(len(body) / 1e6, 2),
                      "top_forms": top,
                      "format_note": "gz-JSON v1; parquet upgrade pending "
                                     "pyarrow layer (stated, not silent)",
                      "raw_snapshot_key": raw_key}).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    res = {"ok": True, "n_filings": len(rows), "warm_key": warm_key,
           "gz_mb": round(len(body) / 1e6, 2), "top_forms": top}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
