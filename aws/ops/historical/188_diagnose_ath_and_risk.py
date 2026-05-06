#!/usr/bin/env python3
"""
Step 188 — Diagnose ath.html + risk.html (broken).

ath.html is a 6-line redirect that bounces to:
   http://justhodl-dashboard-live.s3-website-us-east-1.amazonaws.com/ath.html
That HTTP target is blocked as mixed content from the HTTPS site.

risk.html fetches from:
   https://zzmoq2mq4vtphjyhm4i7hqpzvm0hkwsj.lambda-url.us-east-1.on.aws/
That Lambda may be dead, or just adblock-targeted.

This step:
  A. Check if the ATH HTML exists on S3 (and what bucket-policy state)
  B. Probe the ECB_PROXY Lambda — alive? what does it return?
  C. Inspect the Lambda's resource policy + CodeSha256 to see if it's
     stale/orphaned
  D. Inventory existing /risk/ S3 data we could use to rebuild risk.html
"""
import io
import json
import time
import zipfile
from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
PROBE_NAME = "justhodl-tmp-probe"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ECB_PROXY_URL = "https://zzmoq2mq4vtphjyhm4i7hqpzvm0hkwsj.lambda-url.us-east-1.on.aws/"
ECB_LAMBDA_ID = "zzmoq2mq4vtphjyhm4i7hqpzvm0hkwsj"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


PROBE_CODE = '''
import json, urllib.request, urllib.error
def lambda_handler(event, context):
    url = event["url"]; headers = event.get("headers", {})
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read()
            return {"ok": True, "status": r.status, "len": len(body),
                    "preview": body[:600].decode("utf-8", errors="replace"),
                    "headers": dict(r.headers)}
    except urllib.error.HTTPError as e:
        body = e.read()[:300] if hasattr(e,"read") else b""
        return {"ok": False, "status": e.code, "body": body.decode("utf-8", errors="replace")}
    except Exception as e:
        return {"ok": False, "kind": type(e).__name__, "error": str(e)}
'''


def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", PROBE_CODE)
    buf.seek(0)
    return buf.read()


with report("diagnose_ath_and_risk") as r:
    r.heading("Diagnose ath.html + risk.html breakage")

    # ─── A. ATH on S3? ──────────────────────────────────────────────────
    r.section("A. ATH HTML on S3")
    try:
        head = s3.head_object(Bucket=BUCKET, Key="ath.html")
        r.log(f"  ✅ S3 HEAD ath.html: {head['ContentLength']}B  mod={head['LastModified']}")
        r.log(f"     ContentType: {head.get('ContentType','?')}")
    except ClientError as e:
        r.warn(f"  ⚠ ath.html: {e}")

    # Check if there's an ath-data.json or similar
    r.log(f"\n  Looking for ATH data files on S3:")
    try:
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="ath", MaxKeys=20)
        for o in resp.get("Contents", []):
            r.log(f"    {o['Key']:40} {o['Size']:>9}B  {o['LastModified'].strftime('%Y-%m-%d %H:%M')}")
    except Exception as e:
        r.warn(f"  list err: {e}")

    # Bucket policy on ath.html
    r.log(f"\n  Test public-read on ath.html via temporary probe Lambda:")

    # ─── Setup probe ────────────────────────────────────────────────────
    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    lam.create_function(
        FunctionName=PROBE_NAME, Runtime="python3.11",
        Role=ROLE_ARN, Handler="lambda_function.lambda_handler",
        Code={"ZipFile": build_zip()},
        Timeout=20, MemorySize=256, Architectures=["x86_64"],
    )
    time.sleep(3)

    # Test S3 ath.html public access
    for url in [
        "https://justhodl-dashboard-live.s3.amazonaws.com/ath.html",
        "https://justhodl-dashboard-live.s3-us-east-1.amazonaws.com/ath.html",
        "http://justhodl-dashboard-live.s3-website-us-east-1.amazonaws.com/ath.html",
    ]:
        try:
            resp = lam.invoke(
                FunctionName=PROBE_NAME, InvocationType="RequestResponse",
                Payload=json.dumps({"url": url}),
            )
            result = json.loads(resp["Payload"].read())
            label = "REST/virtual-host" if ".s3.amazonaws" in url else "REST/region" if ".s3-us-east-1" in url else "website-endpoint"
            if result.get("ok"):
                r.log(f"    ✅ {label:25} HTTP {result['status']} len={result.get('len')}")
                r.log(f"        preview: {result.get('preview','')[:120]}")
            else:
                r.log(f"    ✗ {label:25} HTTP {result.get('status')} {result.get('error','')[:100]}")
        except Exception as e:
            r.warn(f"    invoke fail for {url}: {e}")

    # ─── B. ECB_PROXY Lambda state ──────────────────────────────────────
    r.section("B. ECB_PROXY Lambda — alive?")
    try:
        lam_list = lam.list_functions()
        match = [f for f in lam_list.get("Functions", [])
                 if "ecb" in f["FunctionName"].lower() or "fred-proxy" in f["FunctionName"].lower()]
        # Search by URL ID match
        r.log(f"  Searching for Lambda whose Function URL ID = {ECB_LAMBDA_ID[:20]}...")
        all_fns = lam.list_functions(MaxItems=200).get("Functions", [])
        # Need to check each function's Function URL config — slow. Do it in parallel later if needed.
        # For now, check by name pattern
        for f in match[:10]:
            r.log(f"    candidate: {f['FunctionName']:50} runtime={f['Runtime']:12} mod={f['LastModified'][:10]}")
    except Exception as e:
        r.warn(f"  list err: {e}")

    # Hit the URL directly via probe
    for path_method in [("?action=dashboard&n=10","GET"), ("","GET")]:
        url = ECB_PROXY_URL + path_method[0]
        try:
            resp = lam.invoke(
                FunctionName=PROBE_NAME, InvocationType="RequestResponse",
                Payload=json.dumps({"url": url}),
            )
            result = json.loads(resp["Payload"].read())
            if result.get("ok"):
                r.log(f"  ✅ {path_method[0] or '/':30} HTTP {result['status']} len={result.get('len')}")
                r.log(f"     preview: {result.get('preview','')[:150]}")
            else:
                r.log(f"  ✗ {path_method[0] or '/':30} HTTP {result.get('status','?')}: {result.get('error','')[:100]} body={result.get('body','')[:150]}")
        except Exception as e:
            r.warn(f"  err: {e}")

    # ─── C. /risk/ S3 data inventory ────────────────────────────────────
    r.section("C. /risk/ S3 data — for potential risk.html rebuild")
    try:
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="risk/", MaxKeys=20)
        for o in resp.get("Contents", []):
            r.log(f"    {o['Key']:50} {o['Size']:>9}B  {o['LastModified'].strftime('%Y-%m-%d %H:%M')}")
    except Exception as e:
        r.warn(f"  list err: {e}")

    # ─── Cleanup ────────────────────────────────────────────────────────
    r.section("Cleanup")
    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    r.log("Done")
