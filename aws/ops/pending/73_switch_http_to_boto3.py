#!/usr/bin/env python3
"""
Step 73 — Switch justhodl-intelligence http_get from public HTTPS
to boto3 SDK fetches.

Step 70 fixed the data-source paths; step 71 verified khalid_index=43
flows through. Step 72 found that repo-data.json, edge-data.json,
predictions.json, and intelligence-report.json are NOT public-readable
in the bucket policy — only data/*, screener/*, sentiment/*, plus
flow-data.json and crypto-intel.json are.

The Lambda already has IAM credentials (it's in AWS, runs as
lambda-execution-role). Switching from HTTPS public to boto3 SDK fetches
removes the dependency on public-read entirely.

CHANGE: Rewrite http_get() to:
  1. Detect when URL is for our own bucket (justhodl-dashboard-live)
  2. Extract the key from the URL
  3. Call s3.get_object() instead of urllib
  4. Fall back to public HTTPS for any other URL (none today, but
     defensive)

Existing call sites stay unchanged — they still pass full URLs.

Expected impact:
  - repo-data.json, edge-data.json now load
  - intelligence-report.json gets real ml_risk + carry_risk scores
  - signal-logger stops logging ml_risk=0/carry_risk=0
  - Calibration data quality improves immediately
"""
import io
import os
import zipfile
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

lam = boto3.client("lambda", region_name=REGION)


def build_zip(src_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for f in src_dir.rglob("*"):
            if f.is_file():
                zout.write(f, str(f.relative_to(src_dir)))
    return buf.getvalue()


def deploy(fn_name, src_dir):
    z = build_zip(src_dir)
    lam.update_function_code(FunctionName=fn_name, ZipFile=z)
    lam.get_waiter("function_updated").wait(
        FunctionName=fn_name, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
    )
    return len(z)


with report("switch_http_to_boto3") as r:
    r.heading("Step 73 — Switch justhodl-intelligence to boto3 SDK fetches")

    src_path = REPO_ROOT / "aws/lambdas/justhodl-intelligence/source/lambda_function.py"
    src = src_path.read_text(encoding="utf-8")

    old_http_get = '''def http_get(url,timeout=15):
    try:
        req=urllib_request.Request(url,headers={'User-Agent':'JustHodl-Intel/2.0','Accept':'application/json'})
        with urllib_request.urlopen(req,timeout=timeout,context=ctx) as r:return json.loads(r.read().decode('utf-8'))
    except Exception as e:print(f"FETCH_ERR[{url[:60]}]:{e}");return None'''

    new_http_get = '''def http_get(url,timeout=15):
    """Fetch JSON from a URL. For our own bucket, use boto3 SDK with
    IAM credentials so we don't depend on public-read bucket policy.
    For external URLs, fall back to anonymous HTTPS."""
    # Detect own-bucket URLs and route through boto3
    own_bucket_prefix=f"https://{BUCKET}.s3.amazonaws.com/"
    if url.startswith(own_bucket_prefix):
        key=url[len(own_bucket_prefix):]
        try:
            obj=s3.get_object(Bucket=BUCKET,Key=key)
            return json.loads(obj['Body'].read().decode('utf-8'))
        except Exception as e:
            print(f"S3_ERR[{key[:60]}]:{e}")
            return None
    # External URL fallback (anonymous HTTPS)
    try:
        req=urllib_request.Request(url,headers={'User-Agent':'JustHodl-Intel/2.0','Accept':'application/json'})
        with urllib_request.urlopen(req,timeout=timeout,context=ctx) as r:return json.loads(r.read().decode('utf-8'))
    except Exception as e:print(f"FETCH_ERR[{url[:60]}]:{e}");return None'''

    if old_http_get not in src:
        r.fail("  http_get pattern not found verbatim — cannot patch")
        raise SystemExit(1)

    src = src.replace(old_http_get, new_http_get, 1)
    r.ok("  Replaced http_get with boto3-aware version")

    # Validate
    import ast
    try:
        ast.parse(src)
    except SyntaxError as e:
        r.fail(f"  Syntax error: {e}")
        raise SystemExit(1)

    src_path.write_text(src, encoding="utf-8")
    r.ok(f"  Source valid ({len(src)} bytes), saved")

    size = deploy("justhodl-intelligence", src_path.parent)
    r.ok(f"  Deployed justhodl-intelligence ({size:,} bytes)")

    # Trigger fresh run
    r.section("Trigger fresh run with boto3 fetches")
    try:
        resp = lam.invoke(
            FunctionName="justhodl-intelligence",
            InvocationType="Event",
        )
        r.ok(f"  Async-triggered (status {resp['StatusCode']})")
    except Exception as e:
        r.fail(f"  Trigger failed: {e}")

    r.kv(
        fix="http_get routes own-bucket URLs through boto3",
        external_urls="still use anonymous HTTPS fallback",
        public_read_dependency="removed for own-bucket files",
    )
    r.log("Done")
