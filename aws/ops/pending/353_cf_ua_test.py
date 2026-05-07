#!/usr/bin/env python3
"""Step 353 — Browser-UA test + compare known agent vs chart-data."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/353_cf_ua_test.json"
NAME = "justhodl-cf-ua"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"

def lambda_handler(event, context):
    out = {}
    for label, url in [
        ("volatility",  "https://api.justhodl.ai/agent/volatility"),
        ("chart_data",  "https://api.justhodl.ai/agent/chart-data?catalog=1"),
    ]:
        try:
            req = urllib.request.Request(url, headers={
                "Origin": "https://justhodl.ai",
                "User-Agent": UA,
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=20) as r:
                body = r.read().decode("utf-8")
                try:
                    d = json.loads(body)
                    has_cat = "catalog" in d if isinstance(d, dict) else False
                    out[label] = {"status": r.status, "size": len(body),
                        "has_catalog": has_cat, "preview": body[:150]}
                except Exception:
                    out[label] = {"status": r.status, "preview": body[:200]}
        except urllib.error.HTTPError as e:
            try: body = e.read().decode("utf-8")
            except: body = ""
            out[label] = {"http_err": e.code, "body": body[:300]}
        except Exception as e:
            out[label] = {"err": str(e)[:200]}
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
            MemorySize=256, Timeout=30, Code={"ZipFile": buf.getvalue()})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        try:
            lam.update_function_code(FunctionName=NAME, ZipFile=buf.getvalue())
            lam.get_waiter("function_updated").wait(FunctionName=NAME)
        except Exception:
            pass
    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        out["test"] = json.loads(json.loads(body).get("body", "{}"))
    except Exception:
        out["raw"] = body[:1500]
    try: lam.delete_function(FunctionName=NAME)
    except: pass
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:2500])


if __name__ == "__main__":
    main()
