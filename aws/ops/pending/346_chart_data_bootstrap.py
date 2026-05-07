#!/usr/bin/env python3
"""Step 346 — Bootstrap justhodl-chart-data + Lambda URL + smoke test.

Creates a NEW Lambda + Lambda URL for chart data API. Then replaces
CHART_DATA_LAMBDA_URL_PLACEHOLDER in chart-pro.html with the real URL.
"""
import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/lambda-execution-role"
NAME = "justhodl-chart-data"
REPORT = "aws/ops/reports/346_chart_data_bootstrap.json"

lam = boto3.client("lambda", region_name=REGION)


def build_zip(lambda_name):
    src_dir = f"aws/lambdas/{lambda_name}/source"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _d, files in os.walk(src_dir):
            for fn in files:
                if fn.endswith(".pyc") or "__pycache__" in root:
                    continue
                fp = os.path.join(root, fn)
                zf.write(fp, os.path.relpath(fp, src_dir))
    return buf.getvalue()


def deploy():
    zb = build_zip(NAME)
    env = {
        "FRED_API_KEY": "2f057499936072679d8843d7fce99989",
        "POLYGON_KEY": "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d",
    }
    desc = "Universal historical chart data API — FRED, ECB, OFR, Polygon, Internal."
    try:
        lam.get_function(FunctionName=NAME)
        action = "update"
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
        lam.update_function_configuration(
            FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            MemorySize=512, Timeout=60,
            Environment={"Variables": env}, Role=ROLE_ARN, Description=desc,
        )
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        action = "create"
        lam.create_function(
            FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
            MemorySize=512, Timeout=60, Code={"ZipFile": zb},
            Environment={"Variables": env}, Description=desc,
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
        try:
            lam.put_function_concurrency(FunctionName=NAME, ReservedConcurrentExecutions=10)
        except Exception:
            pass
    return {"action": action, "zip_kb": round(len(zb)/1024, 1)}


def setup_url():
    """Create or get Lambda URL with proper CORS."""
    try:
        r = lam.get_function_url_config(FunctionName=NAME)
        url = r["FunctionUrl"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        r = lam.create_function_url_config(
            FunctionName=NAME, AuthType="NONE",
            Cors={
                "AllowOrigins": ["https://justhodl.ai", "https://www.justhodl.ai"],
                "AllowMethods": ["*"],
                "AllowHeaders": ["content-type"],
                "MaxAge": 86400,
            },
        )
        url = r["FunctionUrl"]
        # Public invoke permission
        try:
            lam.add_permission(
                FunctionName=NAME, StatementId="public-url-invoke",
                Action="lambda:InvokeFunctionUrl", Principal="*",
                FunctionUrlAuthType="NONE",
            )
        except ClientError as ce:
            if ce.response["Error"]["Code"] != "ResourceConflictException":
                raise
    return url


def patch_html_placeholder(url):
    path = "chart-pro.html"
    with open(path) as f:
        html = f.read()
    new_html = html.replace("CHART_DATA_LAMBDA_URL_PLACEHOLDER/", url)
    if new_html == html:
        return {"replaced": False, "note": "placeholder not found (already patched?)"}
    with open(path, "w") as f:
        f.write(new_html)
    return {"replaced": True, "url": url}


def smoke():
    """Quick smoke test — fetch DGS10 + catalog."""
    out = {}
    started = time.time()
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                      Payload=json.dumps({
                          "rawPath": "/",
                          "queryStringParameters": {"series": "DGS10", "from": "2020-01-01"},
                          "requestContext": {"http": {"method": "GET"}},
                          "headers": {"origin": "https://justhodl.ai"},
                      }).encode())
    body = resp["Payload"].read().decode("utf-8")
    out["dgs10_test"] = {
        "status": resp.get("StatusCode"), "fn_err": resp.get("FunctionError"),
        "duration_s": round(time.time() - started, 2),
    }
    try:
        outer = json.loads(body)
        inner = json.loads(outer.get("body", "{}"))
        out["dgs10_test"]["n_obs"] = inner.get("n_obs")
        out["dgs10_test"]["source"] = inner.get("source")
        out["dgs10_test"]["last_value"] = (inner.get("data") or [{}])[-1] if inner.get("data") else None
    except Exception as e:
        out["dgs10_test"]["parse_err"] = str(e)[:200]

    # Catalog test
    started = time.time()
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                      Payload=json.dumps({
                          "rawPath": "/",
                          "queryStringParameters": {"catalog": "1"},
                          "requestContext": {"http": {"method": "GET"}},
                          "headers": {"origin": "https://justhodl.ai"},
                      }).encode())
    body = resp["Payload"].read().decode("utf-8")
    try:
        outer = json.loads(body)
        inner = json.loads(outer.get("body", "{}"))
        out["catalog_test"] = {
            "status": resp.get("StatusCode"),
            "duration_s": round(time.time() - started, 2),
            "n_categories": len(inner.get("catalog", {})),
            "categories": list(inner.get("catalog", {}).keys()),
            "n_total_series": sum(len(c.get("series", [])) for c in inner.get("catalog", {}).values()),
        }
    except Exception as e:
        out["catalog_test"] = {"parse_err": str(e)[:200]}

    # Multi-series test
    started = time.time()
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                      Payload=json.dumps({
                          "rawPath": "/",
                          "queryStringParameters": {"multi": "DGS10,VIXCLS,SPY", "from": "2024-01-01"},
                          "requestContext": {"http": {"method": "GET"}},
                          "headers": {"origin": "https://justhodl.ai"},
                      }).encode())
    body = resp["Payload"].read().decode("utf-8")
    try:
        outer = json.loads(body)
        inner = json.loads(outer.get("body", "{}"))
        out["multi_test"] = {
            "status": resp.get("StatusCode"),
            "duration_s": round(time.time() - started, 2),
            "n": inner.get("n"),
            "series_results": {k: {"source": v.get("source"), "n_obs": v.get("n_obs"),
                                    "error": v.get("error")}
                                for k, v in (inner.get("series") or {}).items()},
        }
    except Exception as e:
        out["multi_test"] = {"parse_err": str(e)[:200]}

    return out


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    print("\n══ DEPLOY ══")
    out["deploy"] = deploy()
    print(f"  {out['deploy']}")

    time.sleep(2)
    print("\n══ LAMBDA URL ══")
    url = setup_url()
    out["lambda_url"] = url
    print(f"  {url}")

    print("\n══ PATCH HTML ══")
    out["patch_html"] = patch_html_placeholder(url)
    print(f"  {out['patch_html']}")

    time.sleep(2)
    print("\n══ SMOKE TESTS ══")
    out["smoke"] = smoke()
    print(json.dumps(out["smoke"], indent=2, default=str)[:2500])

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
