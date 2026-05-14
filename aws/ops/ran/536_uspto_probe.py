#!/usr/bin/env python3
"""536 — Probe USPTO/PatentsView endpoints from Lambda for BUILD 12."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/536_uspto_probe.json"
lam = boto3.client("lambda", region_name="us-east-1")

PROBE_CODE = r"""
import json, urllib.request, urllib.error, time

def http_get(url, method='GET', body=None, timeout=15, headers=None):
    t0 = time.time()
    hdr = {'User-Agent':'Mozilla/5.0', 'Accept':'application/json'}
    if headers: hdr.update(headers)
    try:
        if body:
            data = body.encode('utf-8') if isinstance(body, str) else body
            req = urllib.request.Request(url, data=data, headers=hdr, method=method)
        else:
            req = urllib.request.Request(url, headers=hdr, method=method)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            txt = body.decode('utf-8', 'replace')
            return {'status': r.status, 'bytes': len(body), 'preview': txt[:2500],
                     'elapsed_ms': int((time.time()-t0)*1000)}
    except urllib.error.HTTPError as e:
        body = e.read()[:500].decode('utf-8','replace') if hasattr(e,'read') else ''
        return {'status': e.code, 'err_body': body, 'elapsed_ms': int((time.time()-t0)*1000)}
    except Exception as e:
        return {'status':'EXC','err':str(e)[:200]}


def lambda_handler(event, context):
    out = {}

    # PatentsView Search API — POST with query body
    pv_search_body = '{"q":{"_and":[{"_gte":{"patent_date":"2026-04-01"}},{"_text_any":{"assignee_organization":"Apple"}}]},"f":["patent_number","patent_title","patent_date","assignees"],"o":{"size":5}}'
    out['patentsview_search_post'] = http_get(
        "https://search.patentsview.org/api/v1/patent/",
        method='POST', body=pv_search_body,
        headers={'Content-Type': 'application/json'})

    # PatentsView Get-by-id (no auth)
    out['patentsview_g'] = http_get(
        "https://search.patentsview.org/api/v1/patent/?q={%22patent_number%22:%2211200000%22}&f=[%22patent_number%22,%22patent_title%22]")

    # USPTO PE2E Public PAIR API
    out['uspto_pe2e_status'] = http_get(
        "https://developer.uspto.gov/ds-api/oa-actions/v1/full-text-search?searchText=apple&pageNumber=0&pageSize=5")

    # USPTO Open Data Portal — patent grants
    out['uspto_grant_data'] = http_get(
        "https://developer.uspto.gov/ds-api/v1/datasets")

    # PatFT bulk (free archive)
    out['ppubs_bulk'] = http_get(
        "https://ppubs.uspto.gov/dirsearch-public/searches/searchPortal/searchWithQuery",
        method='POST',
        body='{"searchText":"apple","sort":"date desc","start":0,"size":5,"databaseName":"USPAT"}',
        headers={'Content-Type': 'application/json'})

    # Google BigQuery USPTO public dataset (would need auth)
    # Skip — needs GCP credentials

    # PatentsView old API (deprecated but maybe still up)
    out['patentsview_old_api'] = http_get(
        "https://api.patentsview.org/patents/query?q=%7B%22_and%22%3A%5B%7B%22_gte%22%3A%7B%22patent_date%22%3A%222026-04-01%22%7D%7D%5D%7D&f=%5B%22patent_number%22%2C%22patent_title%22%5D")

    # USPTO PatentsView Bulk Data downloads
    out['uspto_bulk_landing'] = http_get(
        "https://patentsview.org/download/data-download-tables")

    return {'statusCode': 200, 'body': json.dumps(out, default=str)}
"""


def zip_str(s):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", s)
    return buf.getvalue()


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    NAME = "justhodl-tmp-uspto-probe"
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role="arn:aws:iam::857687956942:role/lambda-execution-role",
            MemorySize=256, Timeout=60, Code={"ZipFile": zip_str(PROBE_CODE)})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except lam.exceptions.ResourceConflictException:
        lam.update_function_code(FunctionName=NAME, ZipFile=zip_str(PROBE_CODE))
        lam.get_waiter("function_updated").wait(FunctionName=NAME)

    _time.sleep(2)
    try:
        r = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            out["probes"] = json.loads(p["body"]) if p.get("body") else p
        except: out["raw"] = body[:2500]
    except Exception as e:
        out["err"] = str(e)[:400]

    try: lam.delete_function(FunctionName=NAME)
    except: pass

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
