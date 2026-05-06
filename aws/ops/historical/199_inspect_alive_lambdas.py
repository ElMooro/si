#!/usr/bin/env python3
"""
Step 199 — Inspect the 4 highest-value 'alive-no-output' Lambdas.

Hypothesis: they don't write to S3, they return JSON inline (response
to Function URL request). Confirm by:
  A. Get function URL config
  B. Read first 5KB of source code (look for s3.put_object vs return)
  C. If function URL exists, invoke it directly
  D. If no URL, invoke directly with empty event

Targets (highest user-value):
  - volatility-monitor-agent     → /volatility.html if alive
  - dollar-strength-agent        → /dxy.html
  - bond-indices-agent           → /bonds.html or fold into /carry.html
  - fmp-stock-picks-agent        → /picks.html  (BUT: 20/21 errors, fix first)
  - justhodl-financial-secretary → /secretary.html  (12/0)
  - justhodl-news-sentiment      → /news.html (2/0, low cadence)
"""
import io, json, os, time, zipfile, base64
from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
PROBE_NAME = "justhodl-tmp-probe-199"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name=REGION)


TARGETS = [
    "volatility-monitor-agent",
    "dollar-strength-agent",
    "bond-indices-agent",
    "fmp-stock-picks-agent",
    "justhodl-financial-secretary",
    "justhodl-news-sentiment",
    "bea-economic-agent",
    "manufacturing-global-agent",
    "securities-banking-agent",
    "google-trends-agent",
    "macro-financial-intelligence",
    "justhodl-repo-monitor",  # confirmed alive-produces, double-check shape
]


def get_function_code(name):
    """Download .zip, find handler file, return source text."""
    try:
        info = lam.get_function(FunctionName=name)
        url = info["Code"]["Location"]
    except ClientError as e:
        return None, str(e)
    import urllib.request
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            zip_bytes = r.read()
    except Exception as e:
        return None, str(e)
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            # Find lambda_function.py or handler.py
            handler_cfg = info["Configuration"].get("Handler", "")
            if "." in handler_cfg:
                module_name = handler_cfg.split(".")[0]
                candidates = [f"{module_name}.py", "lambda_function.py", "handler.py", "index.py"]
            else:
                candidates = ["lambda_function.py", "handler.py", "index.py"]
            for c in candidates:
                try:
                    src = zf.read(c).decode("utf-8", errors="replace")
                    return src, c
                except KeyError: continue
            # Last resort: list .py files
            pys = [n for n in zf.namelist() if n.endswith(".py")]
            if pys:
                return zf.read(pys[0]).decode("utf-8", errors="replace"), pys[0]
    except Exception as e:
        return None, str(e)
    return None, "no .py file found"


with report("inspect_alive_lambdas") as r:
    r.heading("Inspect 12 alive Lambdas — find return shape")

    for name in TARGETS:
        r.section(f"📦 {name}")

        # Function URL?
        try:
            url_cfg = lam.get_function_url_config(FunctionName=name)
            r.log(f"  Function URL: {url_cfg.get('FunctionUrl')}  auth={url_cfg.get('AuthType')}")
        except ClientError:
            r.log(f"  Function URL: none")

        # Source code analysis
        src, fname = get_function_code(name)
        if not src:
            r.warn(f"  source unavailable: {fname}")
        else:
            r.log(f"  source: {fname}  {len(src)} chars")

            # Quick analysis flags
            has_s3_put = "put_object" in src or "s3.put" in src
            returns_dict = "return {" in src or "return jsonify" in src or "'statusCode'" in src or '"statusCode"' in src
            has_apigw = "statusCode" in src
            uses_fred = "stlouisfed" in src or "FRED" in src or "fred_api" in src.lower()
            uses_fmp = "fmpcloud" in src or "financialmodelingprep" in src or "fmp_api" in src.lower()
            uses_polygon = "polygon" in src.lower()
            uses_yfinance = "yfinance" in src or "yahoo" in src.lower()

            r.log(f"  flags: s3.put={has_s3_put} returns_dict={returns_dict} apigw_response={has_apigw}")
            r.log(f"         fred={uses_fred} fmp={uses_fmp} polygon={uses_polygon} yfin={uses_yfinance}")

            # Find the handler signature + first 25 chars of return statements
            import re
            handler_match = re.search(r'def\s+lambda_handler\([^)]*\):', src)
            if handler_match:
                start = handler_match.start()
                # Show next 800 chars
                r.log(f"  handler preview:")
                snippet = src[start:start+800]
                for line in snippet.split("\n")[:25]:
                    if line.strip(): r.log(f"    {line[:120]}")

            # Find any S3 keys hardcoded in source
            s3_keys_in_src = re.findall(r'(?:Key\s*=\s*[\'"]([^\'"]+)[\'"]|s3://[^\'"]+/([^\'"]+))', src)
            keys_found = [k for tup in s3_keys_in_src for k in tup if k]
            if keys_found:
                r.log(f"  S3 keys in source: {keys_found[:5]}")

        # Test invoke
        try:
            resp = lam.invoke(
                FunctionName=name, InvocationType="RequestResponse",
                Payload=json.dumps({}),
            )
            payload = resp["Payload"].read().decode("utf-8", errors="replace")
            r.log(f"  invoke status: {resp.get('StatusCode')}  payload {len(payload)}B")
            r.log(f"  payload preview: {payload[:600]}")
            if resp.get("FunctionError"):
                r.warn(f"  FUNCTION ERROR: {resp.get('FunctionError')}")
        except Exception as e:
            r.warn(f"  invoke fail: {e}")

    r.log("Done")
