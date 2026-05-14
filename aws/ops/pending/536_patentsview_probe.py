#!/usr/bin/env python3
"""536 — Probe PatentsView API endpoints for BUILD 12 USPTO patents tracker."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/536_patentsview_probe.json"
lam = boto3.client("lambda", region_name="us-east-1")

PROBE_CODE = r"""
import json, urllib.request, urllib.error, time, urllib.parse

def http_get(url, timeout=20, post_body=None):
    t0 = time.time()
    try:
        hdrs = {'User-Agent':'Mozilla/5.0', 'Accept':'application/json'}
        if post_body:
            req = urllib.request.Request(url, data=post_body.encode('utf-8'),
                headers={**hdrs, 'Content-Type': 'application/json'}, method='POST')
        else:
            req = urllib.request.Request(url, headers=hdrs)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            return {'status': r.status, 'bytes': len(body),
                    'preview': body[:2500].decode('utf-8','replace'),
                    'elapsed_ms': int((time.time()-t0)*1000)}
    except urllib.error.HTTPError as e:
        body = e.read()[:500].decode('utf-8','replace') if hasattr(e,'read') else ''
        return {'status': e.code, 'err_body': body, 'elapsed_ms': int((time.time()-t0)*1000)}
    except Exception as e:
        return {'status':'EXC','err':str(e)[:200]}


def lambda_handler(event, context):
    out = {}

    # PatentsView v2 search endpoint (current as of 2026)
    # https://search.patentsview.org/docs/v2-api/api-reference/
    # GET style with query parameter
    apple_q = urllib.parse.quote(json.dumps({
        "_and": [
            {"assignee_organization": "Apple Inc."},
            {"_gte": {"patent_date": "2025-01-01"}},
        ]
    }))
    out['v2_apple_2025_get'] = http_get(
        f"https://search.patentsview.org/api/v1/patent/?q={apple_q}&f=[%22patent_id%22,%22patent_title%22,%22patent_date%22]&o={{\"size\":3}}")

    # Try POST style (more reliable for complex queries)
    apple_body = json.dumps({
        "q": {"_and": [
            {"assignee_organization": "Apple Inc."},
            {"_gte": {"patent_date": "2025-01-01"}}
        ]},
        "f": ["patent_id", "patent_title", "patent_date"],
        "o": {"size": 3}
    })
    out['v2_apple_2025_post'] = http_get(
        "https://search.patentsview.org/api/v1/patent/", post_body=apple_body)

    # Simpler GET to test basic auth
    out['v2_basic'] = http_get(
        "https://search.patentsview.org/api/v1/patent/?q=%7B%22assignee_organization%22%3A%22Apple+Inc.%22%7D&o=%7B%22size%22%3A1%7D")

    # Alternative endpoint paths
    out['v2_query_endpoint'] = http_get(
        "https://api.patentsview.org/patents/query?q=%7B%22assignee_organization%22%3A%22Apple+Inc.%22%7D&f=%5B%22patent_title%22%5D&o=%7B%22per_page%22%3A1%7D")

    # USPTO PatentSearch API (separate, used by Patent Public Search)
    out['uspto_search'] = http_get(
        "https://developer.uspto.gov/api/products?categoryGroups=patent")

    # Bulk data download endpoint check
    out['uspto_bulk'] = http_get(
        "https://developer.uspto.gov/products/bulk_full_text/v1.0")

    # Google Patents Public Datasets (BigQuery free) — fallback via BQ would need auth

    return {'statusCode': 200, 'body': json.dumps(out, default=str)}
"""


def zip_str(s):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", s)
    return buf.getvalue()


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    NAME = "justhodl-tmp-patents-probe"
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
