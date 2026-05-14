#!/usr/bin/env python3
"""537 — Probe SEC EDGAR Form 4 + Google Patents for BUILD 12 pivot."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/537_insider_patents_probe.json"
lam = boto3.client("lambda", region_name="us-east-1")

PROBE_CODE = r"""
import json, urllib.request, urllib.error, time

def http_get(url, timeout=20, hdr_extra=None):
    t0 = time.time()
    hdrs = {
        'User-Agent':'JustHodl.AI Research support@justhodl.ai',
        'Accept':'application/json, text/html, */*',
    }
    if hdr_extra: hdrs.update(hdr_extra)
    try:
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

    # SEC EDGAR · recent Form 4 filings for a ticker (AAPL CIK 0000320193)
    out['edgar_aapl_form4_recent'] = http_get(
        "https://data.sec.gov/submissions/CIK0000320193.json")

    # SEC EDGAR · company facts (financial filings index)
    out['edgar_company_facts'] = http_get(
        "https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json")

    # SEC EDGAR · full-text search for recent Form 4 by ticker
    out['edgar_fulltext_form4_aapl'] = http_get(
        "https://efts.sec.gov/LATEST/search-index?q=%22AAPL%22&dateRange=custom&startdt=2026-04-01&enddt=2026-05-14&forms=4")

    # SEC EDGAR EDGAR full-text search (the right endpoint)
    out['edgar_fts_form4'] = http_get(
        "https://efts.sec.gov/LATEST/search-index?q=&forms=4&dateRange=custom&startdt=2026-05-01&enddt=2026-05-14&ciks=0000320193")

    # Simpler: browse recent filings page (JSON via the data subdomain)
    out['edgar_browse_aapl'] = http_get(
        "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000320193&type=4&dateb=&owner=include&count=10&output=atom")

    # FMP /stable/ insider trading endpoint (we have FMP_KEY)
    fmp_key = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
    out['fmp_insider_aapl'] = http_get(
        f"https://financialmodelingprep.com/stable/insider-trading?symbol=AAPL&limit=10&apikey={fmp_key}")
    out['fmp_insider_search'] = http_get(
        f"https://financialmodelingprep.com/stable/insider-trading?limit=20&apikey={fmp_key}")
    out['fmp_insider_summary'] = http_get(
        f"https://financialmodelingprep.com/stable/insider-trading-statistics?symbol=AAPL&apikey={fmp_key}")
    # Patents fallback via Google
    out['google_patents_aapl'] = http_get(
        "https://patents.google.com/?assignee=Apple+Inc.&type=PATENT&num=10&sort=new")

    return {'statusCode': 200, 'body': json.dumps(out, default=str)}
"""


def zip_str(s):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", s)
    return buf.getvalue()


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    NAME = "justhodl-tmp-insider-probe"
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
