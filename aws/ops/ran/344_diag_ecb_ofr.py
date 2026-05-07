#!/usr/bin/env python3
"""Step 344 — Find ECB cache structure + test OFR endpoints (run from AWS).

Uses an existing Lambda's network access by inline-invoking it with diagnostic
payload. Or creates a temporary diag Lambda.
"""
import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone

import boto3

REPORT = "aws/ops/reports/344_diag_ecb_ofr.json"
NAME = "justhodl-diag-temp"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


DIAG_CODE = '''
import json
import urllib.request
import urllib.parse
import boto3

s3 = boto3.client("s3", region_name="us-east-1")


def list_ecb_keys():
    """List S3 keys under data/ that look ECB-related."""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket="justhodl-dashboard-live", Prefix="data/"):
        for obj in page.get("Contents", []):
            k = obj["Key"].lower()
            if any(t in k for t in ("ecb", "ciss", "sov", "clifs", "ilm", "eurodollar", "stress")):
                keys.append({"key": obj["Key"], "size": obj.get("Size"), "lm": str(obj.get("LastModified"))})
    return keys[:30]


def inspect_key(key):
    """Read a JSON key and return its top-level structure."""
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
        d = json.loads(obj["Body"].read())
        if isinstance(d, dict):
            top_keys = list(d.keys())[:25]
            sample = {k: type(d[k]).__name__ for k in top_keys}
            # If there's a "ciss" or "data" or "countries" key, peek
            for hint in ("ciss", "countries", "data", "ilm", "values", "series", "observations"):
                if hint in d:
                    val = d[hint]
                    if isinstance(val, dict):
                        sample[f"{hint}.keys"] = list(val.keys())[:15]
                    elif isinstance(val, list) and val:
                        sample[f"{hint}.first_item_type"] = type(val[0]).__name__
                        if isinstance(val[0], dict):
                            sample[f"{hint}.first_item_keys"] = list(val[0].keys())[:10]
            return {"top_keys": top_keys, "sample": sample}
        elif isinstance(d, list):
            return {"top": "list", "len": len(d),
                    "first_item": d[0] if d else None}
    except Exception as e:
        return {"err": str(e)[:200]}
    return {}


def test_ofr_endpoints():
    """Test multiple OFR primary dealer fails endpoint patterns."""
    endpoints = [
        ("v1_full", "https://data.financialresearch.gov/v1/series/full?mnemonic=NYPD-PD_AFtD_TOT-A"),
        ("v1_ts",   "https://data.financialresearch.gov/v1/series/timeseries?mnemonic=NYPD-PD_AFtD_TOT-A"),
        ("ofr_full", "https://www.financialresearch.gov/short-term-funding-monitor/api/v1/series/full/?mnemonic=NYPD-PD_AFtD_TOT-A"),
        ("ofr_ts",   "https://www.financialresearch.gov/short-term-funding-monitor/api/v1/series/timeseries/NYPD-PD_AFtD_TOT-A"),
        ("ofr_data", "https://www.financialresearch.gov/short-term-funding-monitor/api/data/NYPD-PD_AFtD_TOT-A.json"),
        ("ofr_root", "https://data.financialresearch.gov/v1/series?mnemonic=NYPD-PD_AFtD_TOT-A"),
    ]
    results = {}
    for name, url in endpoints:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "diag/1.0"})
            with urllib.request.urlopen(req, timeout=10) as r:
                body = r.read().decode("utf-8")
                results[name] = {
                    "url": url,
                    "status": r.status,
                    "bytes": len(body),
                    "preview": body[:400],
                }
        except urllib.error.HTTPError as e:
            results[name] = {"url": url, "http_err": e.code, "msg": e.read().decode("utf-8", errors="replace")[:200]}
        except Exception as e:
            results[name] = {"url": url, "err": str(e)[:200]}
    return results


def lambda_handler(event, context):
    out = {}
    out["ecb_keys"] = list_ecb_keys()
    # Inspect first 5 ECB keys
    inspected = {}
    for entry in out["ecb_keys"][:5]:
        inspected[entry["key"]] = inspect_key(entry["key"])
    out["ecb_structure"] = inspected
    out["ofr_endpoints"] = test_ofr_endpoints()
    return {"statusCode": 200, "body": json.dumps(out, default=str)[:30000]}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Create ephemeral diag Lambda
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG_CODE)
    zb = buf.getvalue()

    print("[diag] Creating temp Lambda…")
    try:
        lam.create_function(
            FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
            MemorySize=256, Timeout=120, Code={"ZipFile": zb},
            Description="Temporary diagnostic Lambda — delete after use",
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception as e:
        # Update if exists
        try:
            lam.update_function_code(FunctionName=NAME, ZipFile=zb)
            lam.get_waiter("function_updated").wait(FunctionName=NAME)
        except Exception as e2:
            out["create_err"] = str(e)
            out["update_err"] = str(e2)

    time.sleep(2)
    print("[diag] Invoking…")
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        outer = json.loads(body)
        inner = json.loads(outer.get("body", "{}"))
        out["diag"] = inner
    except Exception:
        out["diag_raw"] = body[:2000]

    # Cleanup
    try:
        lam.delete_function(FunctionName=NAME)
        out["cleanup"] = "deleted"
    except Exception as e:
        out["cleanup"] = f"err: {e}"

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:8000])


if __name__ == "__main__":
    main()
