#!/usr/bin/env python3
"""Step 373c — Print every response header AWS actually sends."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/373c_stress_raw_headers.json"
NAME = "justhodl-tmp-stress-raw-headers"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json, urllib.request

URL = "https://zmwuxvqqwexxzzgrw7g2ayql6i0fqrpi.lambda-url.us-east-1.on.aws"

def get_all_headers(method, path, headers=None, body=None):
    """Return ALL response headers as a list of (name, value) tuples."""
    h = headers or {}
    if body and "Content-Type" not in h:
        h["Content-Type"] = "application/json"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(URL + path, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            # r.getheaders() returns a list of (name, value) tuples
            # preserving original casing as the server sent them
            all_h = r.getheaders()
            body = r.read().decode("utf-8", errors="ignore")
            return r.status, all_h, body
    except Exception as e:
        return None, [], f"ERROR: {type(e).__name__}: {e}"

def lambda_handler(event, context):
    out = {}

    # GET / with Origin
    s, h, body = get_all_headers("GET", "/", headers={"Origin": "https://justhodl.ai"})
    out["1_get_with_origin"] = {"status": s, "all_headers": h, "body_first_120": body[:120]}

    # POST /simulate with Origin
    s, h, body = get_all_headers("POST", "/simulate",
                                    headers={"Origin": "https://justhodl.ai"},
                                    body={"preset": "gfc_2008"})
    out["2_post_with_origin"] = {"status": s, "all_headers": h, "body_first_120": body[:120]}

    # POST /simulate without Origin
    s, h, body = get_all_headers("POST", "/simulate", body={"preset": "covid_march_2020"})
    out["3_post_no_origin"] = {"status": s, "all_headers": h, "body_first_120": body[:120]}

    # OPTIONS preflight
    s, h, body = get_all_headers("OPTIONS", "/simulate",
                                   headers={"Origin": "https://justhodl.ai",
                                            "Access-Control-Request-Method": "POST",
                                            "Access-Control-Request-Headers": "content-type"})
    out["4_options_preflight"] = {"status": s, "all_headers": h}

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
