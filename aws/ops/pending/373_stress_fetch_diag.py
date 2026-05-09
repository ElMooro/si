#!/usr/bin/env python3
"""Step 373 — Diagnose why stress.html / API fetch is failing."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/373_stress_fetch_diag.json"
NAME = "justhodl-tmp-stress-diag"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json, urllib.request, urllib.error
import boto3

lam = boto3.client("lambda", region_name="us-east-1")

def http(method, url, headers=None, body=None):
    h = headers or {}
    if body and "Content-Type" not in h:
        h["Content-Type"] = "application/json"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, dict(r.headers), r.read().decode("utf-8", errors="ignore")
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers) if hasattr(e, "headers") else {}, e.read().decode("utf-8", errors="ignore")
    except Exception as e:
        return None, {}, f"ERROR: {type(e).__name__}: {e}"

def lambda_handler(event, context):
    out = {}
    URL = "https://zmwuxvqqwexxzzgrw7g2ayql6i0fqrpi.lambda-url.us-east-1.on.aws"

    # 1. Is justhodl.ai/stress.html actually served?
    s, headers, body = http("GET", "https://justhodl.ai/stress.html")
    out["1_page_live"] = {
        "status": s,
        "size": len(body) if isinstance(body, str) else None,
        "content_type": headers.get("Content-Type"),
        "has_real_url": "zmwuxvqqwexxzzgrw7g2ayql6i0fqrpi" in body if isinstance(body, str) else False,
        "has_placeholder": "PLACEHOLDER" in body if isinstance(body, str) else False,
        "first_200": body[:200] if isinstance(body, str) else body,
    }

    # 2. Does index.html link to stress.html?
    s, headers, body = http("GET", "https://justhodl.ai/index.html")
    out["2_nav_link"] = {
        "status": s,
        "has_stress_link": "stress.html" in body if isinstance(body, str) else False,
    }

    # 3. Direct Function URL — GET /
    s, headers, body = http("GET", URL + "/", headers={"Origin": "https://justhodl.ai"})
    out["3_get_health"] = {
        "status": s,
        "cors_origin": headers.get("Access-Control-Allow-Origin"),
        "cors_methods": headers.get("Access-Control-Allow-Methods"),
        "first_300": body[:300] if isinstance(body, str) else body,
    }

    # 4. CORS preflight (OPTIONS) — what browsers send before POST
    s, headers, body = http("OPTIONS", URL + "/simulate", headers={
        "Origin": "https://justhodl.ai",
        "Access-Control-Request-Method": "POST",
        "Access-Control-Request-Headers": "Content-Type",
    })
    out["4_cors_preflight"] = {
        "status": s,
        "all_headers": {k: v for k, v in headers.items()
                         if k.lower().startswith("access-control") or k.lower() in ("vary", "origin")},
        "body": body[:200] if isinstance(body, str) else body,
    }

    # 5. POST /simulate with Origin header
    s, headers, body = http("POST", URL + "/simulate",
                              headers={"Origin": "https://justhodl.ai"},
                              body={"preset": "gfc_2008"})
    out["5_post_simulate"] = {
        "status": s,
        "cors_origin": headers.get("Access-Control-Allow-Origin"),
        "body_first_400": body[:400] if isinstance(body, str) else body,
    }

    # 6. POST without Origin (sandbox normally would not have one)
    s, headers, body = http("POST", URL + "/simulate", body={"preset": "covid_march_2020"})
    out["6_post_no_origin"] = {
        "status": s,
        "cors_origin": headers.get("Access-Control-Allow-Origin"),
        "body_first_300": body[:300] if isinstance(body, str) else body,
    }

    # 7. Lambda Function URL config — what does AWS think CORS is?
    try:
        cfg = lam.get_function_url_config(FunctionName="justhodl-stress-simulator")
        out["7_url_config"] = {
            "url": cfg.get("FunctionUrl"),
            "auth_type": cfg.get("AuthType"),
            "cors": cfg.get("Cors"),
        }
    except Exception as e:
        out["7_url_config"] = {"error": str(e)}

    # 8. Lambda runtime status
    try:
        cfg = lam.get_function(FunctionName="justhodl-stress-simulator")["Configuration"]
        out["8_lambda_status"] = {
            "state": cfg.get("State"),
            "last_update_status": cfg.get("LastUpdateStatus"),
            "runtime": cfg.get("Runtime"),
            "handler": cfg.get("Handler"),
            "last_modified": cfg.get("LastModified"),
            "code_size": cfg.get("CodeSize"),
        }
    except Exception as e:
        out["8_lambda_status"] = {"error": str(e)}

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
