#!/usr/bin/env python3
"""524 — Probe /stable/ transcript content + verify what other stable endpoints work for our needs."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/524_fmp_stable_probe.json"
lam = boto3.client("lambda", region_name="us-east-1")

PROBE_CODE = r"""
import json, urllib.request, urllib.error, time, os

FMP_KEY = os.environ.get("FMP_KEY", "")

def http_get(url, timeout=20):
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            txt = body.decode('utf-8', 'replace')
            return {'status': r.status, 'bytes': len(body),
                     'preview': txt[:2500],
                     'elapsed_ms': int((time.time()-t0)*1000),
                     'is_empty_array': txt.strip() == '[]'}
    except urllib.error.HTTPError as e:
        body = e.read()[:500].decode('utf-8','replace') if hasattr(e,'read') else ''
        return {'status': e.code, 'err_body': body, 'elapsed_ms': int((time.time()-t0)*1000)}
    except Exception as e:
        return {'status':'EXC','err':str(e)[:200]}


def lambda_handler(event, context):
    K = FMP_KEY
    out = {}

    # The endpoint that worked: dates list
    out["dates_AAPL"] = http_get(
        f"https://financialmodelingprep.com/stable/earning-call-transcript-dates?symbol=AAPL&apikey={K}")

    # Content with year+quarter (the documented param)
    for y, q in [(2026, 2), (2026, 1), (2025, 4)]:
        out[f"content_AAPL_Y{y}_Q{q}"] = http_get(
            f"https://financialmodelingprep.com/stable/earning-call-transcript?symbol=AAPL&year={y}&quarter={q}&apikey={K}")

    # Calendar (date-based, not symbol-based)
    out["calendar_2026_05"] = http_get(
        f"https://financialmodelingprep.com/stable/earnings-calendar?from=2026-05-01&to=2026-05-20&apikey={K}")

    # Test alternate stable endpoint names
    out["dates_MSFT"] = http_get(
        f"https://financialmodelingprep.com/stable/earning-call-transcript-dates?symbol=MSFT&apikey={K}")

    # Quote alternative
    out["quote_AAPL_stable"] = http_get(
        f"https://financialmodelingprep.com/stable/quote?symbol=AAPL&apikey={K}")

    return {'statusCode': 200, 'body': json.dumps(out, default=str)}
"""


def zip_str(s):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", s)
    return buf.getvalue()


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    NAME = "justhodl-tmp-fmp-stable"
    env = {}
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-earnings-nlp")
        env = {"FMP_KEY": (cfg.get("Environment") or {}).get("Variables", {}).get("FMP_KEY", "")}
    except: pass

    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role="arn:aws:iam::857687956942:role/lambda-execution-role",
            MemorySize=256, Timeout=60, Code={"ZipFile": zip_str(PROBE_CODE)},
            Environment={"Variables": env})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except lam.exceptions.ResourceConflictException:
        lam.update_function_code(FunctionName=NAME, ZipFile=zip_str(PROBE_CODE))
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
        lam.update_function_configuration(FunctionName=NAME, Environment={"Variables": env})

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
