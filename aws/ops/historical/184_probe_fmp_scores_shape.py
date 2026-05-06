#!/usr/bin/env python3
"""
Step 184 — Probe FMP /stable/scores response to find Altman Z field name.

Step 181 confirmed altmanZ is 0/503 even after fix. My field-name
guesses didn't match. Need to see actual response.

Use the temporary probe Lambda pattern from step 183 — small inline
Lambda that does urllib.request to FMP and returns the JSON.
"""
import io
import json
import time
import zipfile
from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
PROBE_NAME = "justhodl-tmp-fmp-probe"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"

lam = boto3.client("lambda", region_name=REGION)


PROBE_CODE = '''
import json
import urllib.request

def lambda_handler(event, context):
    url = event["url"]
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            body = r.read().decode("utf-8")
            try:
                parsed = json.loads(body)
            except:
                parsed = body[:500]
            return {
                "ok": True,
                "status": r.status,
                "shape": type(parsed).__name__,
                "len": len(parsed) if isinstance(parsed, (list, dict, str)) else None,
                "data": parsed,
            }
    except Exception as e:
        return {"ok": False, "kind": type(e).__name__, "error": str(e)}
'''


def build_probe_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", PROBE_CODE)
    buf.seek(0)
    return buf.read()


with report("probe_fmp_scores_shape") as r:
    r.heading("Probe FMP /stable/scores to find Altman Z field name")

    # ─── Setup probe Lambda ─────────────────────────────────────────────
    r.section("Setup probe Lambda")
    try:
        lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError:
        pass
    try:
        lam.create_function(
            FunctionName=PROBE_NAME,
            Runtime="python3.11",
            Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": build_probe_zip()},
            Timeout=30,
            MemorySize=256,
            Architectures=["x86_64"],
        )
        time.sleep(3)
        r.ok(f"  Created {PROBE_NAME}")
    except Exception as e:
        r.fail(f"  create failed: {e}")
        raise SystemExit(1)

    # ─── Test multiple endpoints ────────────────────────────────────────
    test_cases = [
        ("scores_AAPL_stable",      f"https://financialmodelingprep.com/stable/scores?symbol=AAPL&apikey={FMP_KEY}"),
        ("scores_AAPL_v3",          f"https://financialmodelingprep.com/api/v3/score/AAPL?apikey={FMP_KEY}"),
        ("scores_AAPL_v4",          f"https://financialmodelingprep.com/api/v4/score?symbol=AAPL&apikey={FMP_KEY}"),
        ("scores_v3_companyrating", f"https://financialmodelingprep.com/api/v3/rating/AAPL?apikey={FMP_KEY}"),
        ("financial_growth_v3",     f"https://financialmodelingprep.com/api/v3/financial-growth/AAPL?limit=1&apikey={FMP_KEY}"),
    ]

    for label, url in test_cases:
        r.section(f"Test: {label}")
        # Mask key in log
        log_url = url.replace(FMP_KEY, "***")
        r.log(f"  URL: {log_url}")
        try:
            resp = lam.invoke(
                FunctionName=PROBE_NAME,
                InvocationType="RequestResponse",
                Payload=json.dumps({"url": url}),
            )
            result = json.loads(resp["Payload"].read())
            if not result.get("ok"):
                r.warn(f"  ✗ {result.get('kind')}: {result.get('error','')[:200]}")
                continue
            r.log(f"  status={result.get('status')}  shape={result.get('shape')}  len={result.get('len')}")
            data = result.get("data")

            # Find first record
            first = None
            if isinstance(data, list) and data:
                first = data[0]
            elif isinstance(data, dict):
                first = data

            if isinstance(first, dict):
                r.log(f"  Keys ({len(first)}):")
                for k in sorted(first.keys()):
                    v = first[k]
                    vs = json.dumps(v)[:80] if v is not None else "None"
                    r.log(f"    {k:35} = {vs}")

                # Look for Altman-related keys specifically
                altman_keys = [k for k in first.keys() if "altman" in k.lower() or "z_score" in k.lower() or "zscore" in k.lower()]
                if altman_keys:
                    r.ok(f"\n  🎯 ALTMAN-LIKE KEYS: {altman_keys}")
                    for k in altman_keys:
                        r.log(f"    {k} = {first[k]}")
                else:
                    r.warn(f"\n  ⚠ No keys with 'altman' or 'zscore' in name")
            else:
                r.log(f"  data: {json.dumps(data)[:400]}")
        except Exception as e:
            r.warn(f"  invoke fail: {e}")

    # ─── Cleanup ────────────────────────────────────────────────────────
    r.section("Cleanup")
    try:
        lam.delete_function(FunctionName=PROBE_NAME)
        r.ok(f"  Deleted {PROBE_NAME}")
    except Exception as e:
        r.warn(f"  delete failed: {e}")

    r.log("Done")
