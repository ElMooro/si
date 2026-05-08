#!/usr/bin/env python3
"""Step 362 — Read 6 latent-Lambda S3 JSONs to learn schemas before UI build."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/362_six_lambda_schemas.json"
NAME = "justhodl-tmp-six-schemas"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json, boto3
s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
KEYS = [
    "data/narrative-density.json",
    "data/cross-asset-regime.json",
    "data/cross-asset-regime-state.json",
    "cot/extremes/current.json",
    "data/dealer-survey.json",
    "data/gdelt-news.json",
    "data/options-flow.json",
]
def shape(v, depth=0, max_depth=4):
    if depth > max_depth:
        return f"<truncated: {type(v).__name__}>"
    if isinstance(v, dict):
        out = {}
        for k, vv in list(v.items())[:12]:
            out[k] = shape(vv, depth+1, max_depth)
        if len(v) > 12:
            out["__more_keys__"] = list(v.keys())[12:]
        return out
    if isinstance(v, list):
        if not v:
            return "[]"
        sample = shape(v[0], depth+1, max_depth)
        return {"__type__": "list", "__len__": len(v), "__sample__": sample}
    if isinstance(v, str) and len(v) > 100:
        return f"<str:{len(v)}> {v[:100]}..."
    return v
def lambda_handler(event, context):
    out = {}
    for key in KEYS:
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            body = obj["Body"].read()
            j = json.loads(body)
            out[key] = {
                "size_bytes": len(body),
                "last_modified": str(obj.get("LastModified")),
                "schema": shape(j, 0, 4),
            }
        except s3.exceptions.NoSuchKey:
            out[key] = {"error": "NoSuchKey (Lambda has not run yet)"}
        except Exception as e:
            out[key] = {"error": f"{type(e).__name__}: {str(e)[:200]}"}
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG_CODE)
    zb = buf.getvalue()
    try:
        lam.create_function(
            FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
            MemorySize=256, Timeout=60, Code={"ZipFile": zb},
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed else parsed
    except Exception:
        out["raw"] = body[:5000]
    try:
        lam.delete_function(FunctionName=NAME)
    except Exception:
        pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
