#!/usr/bin/env python3
"""523 — Direct probe of multiple FMP transcript endpoints to find what works."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/523_fmp_transcript_probe.json"
lam = boto3.client("lambda", region_name="us-east-1")

PROBE_CODE = r"""
import json, urllib.request, urllib.error, time, os

FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")

def http_get(url, timeout=15):
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'application/json',
        })
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            txt = body.decode('utf-8', 'replace')
            return {
                'status': r.status,
                'bytes': len(body),
                'preview': txt[:1500],
                'elapsed_ms': int((time.time()-t0)*1000),
                'is_empty_array': txt.strip() == '[]',
            }
    except urllib.error.HTTPError as e:
        body = e.read()[:500].decode('utf-8','replace') if hasattr(e,'read') else ''
        return {'status': e.code, 'err_body': body, 'elapsed_ms': int((time.time()-t0)*1000)}
    except Exception as e:
        return {'status': 'EXC', 'err': str(e)[:200]}


def lambda_handler(event, context):
    out = {}
    K = FMP_KEY

    # PROBE 1: v3 earning_call_transcript AAPL (no quarter — returns list?)
    out["v3_AAPL_no_qy"] = http_get(
        f"https://financialmodelingprep.com/api/v3/earning_call_transcript/AAPL?apikey={K}")

    # PROBE 2: v3 earning_call_transcript AAPL with explicit Q1 2026
    out["v3_AAPL_Q1_2026"] = http_get(
        f"https://financialmodelingprep.com/api/v3/earning_call_transcript/AAPL?quarter=1&year=2026&apikey={K}")

    # PROBE 3: v3 with Q4 2025 (most likely to exist)
    out["v3_AAPL_Q4_2025"] = http_get(
        f"https://financialmodelingprep.com/api/v3/earning_call_transcript/AAPL?quarter=4&year=2025&apikey={K}")

    # PROBE 4: v3 Q3 2025
    out["v3_AAPL_Q3_2025"] = http_get(
        f"https://financialmodelingprep.com/api/v3/earning_call_transcript/AAPL?quarter=3&year=2025&apikey={K}")

    # PROBE 5: v4 batch (the one we used originally)
    out["v4_batch_AAPL_2025"] = http_get(
        f"https://financialmodelingprep.com/api/v4/batch_earning_call_transcript/AAPL?year=2025&apikey={K}")

    out["v4_batch_AAPL_2024"] = http_get(
        f"https://financialmodelingprep.com/api/v4/batch_earning_call_transcript/AAPL?year=2024&apikey={K}")

    # PROBE 6: v4 list by symbol (alternative endpoint name)
    out["v4_earning_call_transcript_AAPL"] = http_get(
        f"https://financialmodelingprep.com/api/v4/earning_call_transcript?symbol=AAPL&apikey={K}")

    # PROBE 7: stable (latest API) variant
    out["stable_earning_call_transcript_AAPL"] = http_get(
        f"https://financialmodelingprep.com/stable/earning-call-transcript?symbol=AAPL&apikey={K}")

    # PROBE 8: stable transcript dates
    out["stable_transcript_dates"] = http_get(
        f"https://financialmodelingprep.com/stable/earning-call-transcript-dates?symbol=AAPL&apikey={K}")

    # PROBE 9: confirm key works at all with a simple endpoint
    out["sanity_quote_AAPL"] = http_get(
        f"https://financialmodelingprep.com/api/v3/quote/AAPL?apikey={K}")

    # PROBE 10: list all transcripts for a date (latest)
    out["transcripts_latest_dates"] = http_get(
        f"https://financialmodelingprep.com/api/v4/earning-call-transcripts?symbol=AAPL&apikey={K}")

    return {'statusCode': 200, 'body': json.dumps(out, default=str)}
"""


def zip_str(s):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", s)
    return buf.getvalue()


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    NAME = "justhodl-tmp-fmp-probe"
    # Get FMP key from earnings-nlp env
    env = {}
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-earnings-nlp")
        v = (cfg.get("Environment") or {}).get("Variables") or {}
        if v.get("FMP_KEY"): env["FMP_KEY"] = v["FMP_KEY"]
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
        r = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                        LogType="Tail", Payload=b"{}")
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
