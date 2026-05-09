#!/usr/bin/env python3
"""Step 373b — Direct Lambda invoke to see raw response headers."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/373b_stress_raw_invoke.json"
NAME = "justhodl-tmp-stress-raw"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json
import boto3

lam = boto3.client("lambda", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}

    # Build a synthetic Function URL event
    url_event = {
        "version": "2.0",
        "routeKey": "$default",
        "rawPath": "/simulate",
        "rawQueryString": "",
        "headers": {
            "content-type": "application/json",
            "origin": "https://justhodl.ai",
            "user-agent": "test",
        },
        "requestContext": {
            "http": {"method": "POST", "path": "/simulate", "protocol": "HTTP/1.1",
                      "sourceIp": "1.2.3.4", "userAgent": "test"},
        },
        "body": json.dumps({"preset": "gfc_2008"}),
        "isBase64Encoded": False,
    }

    # Invoke the actual stress-simulator Lambda directly and capture its raw output
    resp = lam.invoke(FunctionName="justhodl-stress-simulator",
                       InvocationType="RequestResponse",
                       Payload=json.dumps(url_event).encode())
    body = resp["Payload"].read().decode("utf-8")
    out["raw_response_text"] = body[:1500]

    # Parse the response (which is the Lambda's return value)
    try:
        parsed = json.loads(body)
        out["parsed_keys"] = list(parsed.keys())
        out["parsed_status"] = parsed.get("statusCode")
        out["parsed_headers"] = parsed.get("headers")
        out["parsed_body_first_200"] = (parsed.get("body") or "")[:200]
    except Exception as e:
        out["parse_error"] = str(e)

    # Now do a GET / event for comparison
    get_event = {
        "version": "2.0", "routeKey": "$default", "rawPath": "/", "rawQueryString": "",
        "headers": {"origin": "https://justhodl.ai"},
        "requestContext": {"http": {"method": "GET", "path": "/", "protocol": "HTTP/1.1",
                                       "sourceIp": "1.2.3.4", "userAgent": "test"}},
        "isBase64Encoded": False,
    }
    resp = lam.invoke(FunctionName="justhodl-stress-simulator",
                       InvocationType="RequestResponse",
                       Payload=json.dumps(get_event).encode())
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["get_response_status"] = parsed.get("statusCode")
        out["get_response_headers"] = parsed.get("headers")
    except Exception as e:
        out["get_parse_error"] = str(e)

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG_CODE)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=256, Timeout=120, Code={"ZipFile": zb})
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
