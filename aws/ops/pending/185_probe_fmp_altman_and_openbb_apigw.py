#!/usr/bin/env python3
"""
Step 185 — Find working FMP Altman Z endpoint + check OpenBB API Gateway.

Two parallel diagnostics:

A. FMP Altman Z — step 184 found:
   - /stable/scores → 404 (doesn't exist)
   - /api/v3/* → 403 (Premium plan grants only /stable/)
   Need to probe more /stable/ candidates: financial-scores,
   altman-zscore, company-rating, financial-strength.

B. OpenBB API Gateway — landing page (justhodl.ai) calls
   https://i70jxru6md.execute-api.us-east-1.amazonaws.com/prod/api/v1/
   for ML Regime, Risk Level, Liquidity, US Outlook, Sector Regime,
   ML PREDICTION ENGINE. All show N/A → Gateway is dead/orphaned.
   Check if the API Gateway exists in account 857687956942.
"""
import io
import json
import time
import zipfile
from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
PROBE_NAME = "justhodl-tmp-probe"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
OPENBB_API_ID = "i70jxru6md"

lam = boto3.client("lambda", region_name=REGION)
apigw = boto3.client("apigateway", region_name=REGION)


PROBE_CODE = '''
import json
import urllib.request

def lambda_handler(event, context):
    url = event["url"]
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode("utf-8")
            try: parsed = json.loads(body)
            except: parsed = body[:500]
            return {"ok": True, "status": r.status, "data": parsed}
    except Exception as e:
        body = ""
        if hasattr(e, "read"):
            try: body = e.read().decode("utf-8", errors="replace")[:300]
            except: pass
        return {"ok": False, "kind": type(e).__name__, "error": str(e), "body": body}
'''


def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", PROBE_CODE)
    buf.seek(0)
    return buf.read()


with report("probe_fmp_altman_and_openbb_apigw") as r:
    r.heading("Find Altman endpoint + check OpenBB API Gateway")

    # ─── Setup probe Lambda ─────────────────────────────────────────────
    r.section("Setup probe Lambda")
    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    lam.create_function(
        FunctionName=PROBE_NAME, Runtime="python3.11",
        Role=ROLE_ARN, Handler="lambda_function.lambda_handler",
        Code={"ZipFile": build_zip()},
        Timeout=20, MemorySize=256, Architectures=["x86_64"],
    )
    time.sleep(3)
    r.ok("  probe Lambda ready")

    # ─── PART A: more FMP /stable/ candidates for Altman Z ──────────────
    r.section("A. FMP /stable/ candidates for Altman Z")
    fmp_candidates = [
        # Direct score endpoints
        f"https://financialmodelingprep.com/stable/financial-scores?symbol=AAPL&apikey={FMP_KEY}",
        f"https://financialmodelingprep.com/stable/financial-strength?symbol=AAPL&apikey={FMP_KEY}",
        f"https://financialmodelingprep.com/stable/company-rating?symbol=AAPL&apikey={FMP_KEY}",
        f"https://financialmodelingprep.com/stable/altman-zscore?symbol=AAPL&apikey={FMP_KEY}",
        f"https://financialmodelingprep.com/stable/altman-z?symbol=AAPL&apikey={FMP_KEY}",
        f"https://financialmodelingprep.com/stable/piotroski-score?symbol=AAPL&apikey={FMP_KEY}",
        # Components for manual computation (we already have most)
        f"https://financialmodelingprep.com/stable/balance-sheet-statement?symbol=AAPL&limit=1&apikey={FMP_KEY}",
        f"https://financialmodelingprep.com/stable/income-statement?symbol=AAPL&limit=1&apikey={FMP_KEY}",
    ]

    altman_winner = None
    for url in fmp_candidates:
        endpoint = url.split("/stable/")[1].split("?")[0]
        try:
            resp = lam.invoke(
                FunctionName=PROBE_NAME, InvocationType="RequestResponse",
                Payload=json.dumps({"url": url}),
            )
            result = json.loads(resp["Payload"].read())
            if not result.get("ok"):
                r.log(f"  {endpoint:35} ✗ {result.get('kind','?')}")
                continue
            data = result.get("data")
            first = data[0] if isinstance(data, list) and data else (data if isinstance(data, dict) else None)
            if first and isinstance(first, dict):
                r.log(f"  {endpoint:35} ✅ ({len(first)} keys)")
                # Look for Altman fields
                altman_keys = [k for k in first.keys()
                              if "altman" in k.lower() or "zscore" in k.lower() or "z_score" in k.lower()]
                if altman_keys:
                    r.ok(f"     🎯 Altman keys: {altman_keys}")
                    for k in altman_keys:
                        r.log(f"        {k} = {first[k]}")
                    if not altman_winner:
                        altman_winner = (endpoint, altman_keys[0])
                # Print all keys for the most promising endpoints
                if "score" in endpoint.lower() or "strength" in endpoint.lower() or "rating" in endpoint.lower() or "altman" in endpoint.lower():
                    r.log(f"     All keys: {sorted(first.keys())[:20]}")
            else:
                r.log(f"  {endpoint:35} ⚠ unexpected shape")
        except Exception as e:
            r.warn(f"  {endpoint}: {e}")

    if altman_winner:
        r.ok(f"\n  ✅ ALTMAN WINNER: endpoint={altman_winner[0]}, field={altman_winner[1]}")
    else:
        r.warn(f"\n  ⚠ No direct Altman endpoint found — will need to compute from balance sheet")

    # ─── PART B: Check OpenBB API Gateway state ─────────────────────────
    r.section("B. OpenBB API Gateway state (powers landing page N/A cards)")
    r.log(f"  API Gateway ID: {OPENBB_API_ID}")
    try:
        api = apigw.get_rest_api(restApiId=OPENBB_API_ID)
        r.log(f"  ✅ API Gateway EXISTS: {api.get('name','?')}")
        r.log(f"     Created: {api.get('createdDate')}")
        r.log(f"     Endpoint type: {api.get('endpointConfiguration',{}).get('types','?')}")

        # Get stages
        stages = apigw.get_stages(restApiId=OPENBB_API_ID)
        r.log(f"     Stages: {[s.get('stageName') for s in stages.get('item',[])]}")

        # Test some endpoints from the API
        for path in ["/api/v1/dashboard/overview", "/api/v1/blackswan", "/api/v1/regime", "/api/v1/ml/predictions"]:
            url = f"https://{OPENBB_API_ID}.execute-api.us-east-1.amazonaws.com/prod{path}"
            try:
                resp = lam.invoke(
                    FunctionName=PROBE_NAME, InvocationType="RequestResponse",
                    Payload=json.dumps({"url": url}),
                )
                result = json.loads(resp["Payload"].read())
                if result.get("ok"):
                    data = result.get("data")
                    preview = json.dumps(data)[:120] if data else "empty"
                    r.log(f"     {path:35} ✅ status=200  preview={preview}")
                else:
                    r.log(f"     {path:35} ✗ {result.get('kind')} {result.get('body','')[:80]}")
            except Exception as e:
                r.warn(f"     {path}: {e}")
    except ClientError as e:
        if "NotFoundException" in str(e):
            r.fail(f"  ❌ API Gateway {OPENBB_API_ID} DOES NOT EXIST")
            r.fail(f"     This is why landing page shows N/A everywhere")
        else:
            r.warn(f"  error: {e}")

    # ─── PART C: What S3 data IS available for landing page rebuild? ────
    r.section("C. Available S3 data files for rebuilding landing page")
    s3 = boto3.client("s3", region_name=REGION)
    try:
        resp = s3.list_objects_v2(Bucket="justhodl-dashboard-live", Delimiter="/", MaxKeys=200)
        root_jsons = sorted([o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".json")])
        r.log(f"  Root JSONs available ({len(root_jsons)}):")
        for k in root_jsons:
            obj = s3.head_object(Bucket="justhodl-dashboard-live", Key=k)
            sz = obj["ContentLength"]
            mod = obj["LastModified"].strftime("%Y-%m-%d %H:%M")
            r.log(f"    {k:35} {sz:>9}B  {mod}")
    except Exception as e:
        r.warn(f"  list error: {e}")

    # ─── Cleanup ────────────────────────────────────────────────────────
    r.section("Cleanup")
    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    r.log("Done")
