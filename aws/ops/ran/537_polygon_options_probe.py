#!/usr/bin/env python3
"""537 — Probe Polygon options endpoints for BUILD 13 (0DTE expansion)."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone, timedelta
import boto3

REPORT = "aws/ops/reports/537_polygon_options_probe.json"
lam = boto3.client("lambda", region_name="us-east-1")

# Get today and a few key dates
today = datetime.now(timezone.utc).date()
PROBE_CODE = f"""
import json, urllib.request, urllib.error, time

POLY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
TODAY = "{today.isoformat()}"
NEXT_FRI = "{(today + timedelta(days=(4 - today.weekday()) % 7 or 7)).isoformat()}"

def http_get(url, timeout=15):
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers={{
            'User-Agent':'Mozilla/5.0', 'Accept':'application/json'
        }})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            txt = body.decode('utf-8', 'replace')
            return {{'status': r.status, 'bytes': len(body), 'preview': txt[:2500],
                    'elapsed_ms': int((time.time()-t0)*1000)}}
    except urllib.error.HTTPError as e:
        body = e.read()[:500].decode('utf-8','replace') if hasattr(e,'read') else ''
        return {{'status': e.code, 'err_body': body, 'elapsed_ms': int((time.time()-t0)*1000)}}
    except Exception as e:
        return {{'status':'EXC','err':str(e)[:200]}}


def lambda_handler(event, context):
    out = {{'today': TODAY, 'next_fri': NEXT_FRI}}

    # Plan info
    out['account_status'] = http_get(
        f"https://api.polygon.io/v1/marketstatus/now?apiKey={{POLY}}")

    # Options contracts: list 0DTE SPY contracts expiring today
    out['contracts_spy_today_calls'] = http_get(
        f"https://api.polygon.io/v3/reference/options/contracts?"
        f"underlying_ticker=SPY&contract_type=call&expiration_date={{TODAY}}&limit=10&apiKey={{POLY}}")
    out['contracts_spy_next_fri'] = http_get(
        f"https://api.polygon.io/v3/reference/options/contracts?"
        f"underlying_ticker=SPY&contract_type=call&expiration_date={{NEXT_FRI}}&limit=10&apiKey={{POLY}}")

    # Snapshot: full chain
    out['snapshot_spy_chain'] = http_get(
        f"https://api.polygon.io/v3/snapshot/options/SPY?limit=20&apiKey={{POLY}}")
    out['snapshot_qqq_chain'] = http_get(
        f"https://api.polygon.io/v3/snapshot/options/QQQ?limit=20&apiKey={{POLY}}")

    # Aggregates for single 0DTE contract (test access tier)
    out['aggs_sample'] = http_get(
        f"https://api.polygon.io/v2/aggs/ticker/O:SPY260514C00580000/range/1/minute/{{TODAY}}/{{TODAY}}?adjusted=true&apiKey={{POLY}}")

    # Underlying intraday — sanity check
    out['underlying_spy'] = http_get(
        f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/minute/{{TODAY}}/{{TODAY}}?adjusted=true&limit=5&apiKey={{POLY}}")

    return {{'statusCode': 200, 'body': json.dumps(out, default=str)}}
"""


def zip_str(s):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", s)
    return buf.getvalue()


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    NAME = "justhodl-tmp-poly-probe"
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
