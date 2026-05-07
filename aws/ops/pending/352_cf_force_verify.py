#!/usr/bin/env python3
"""Step 352 — Final verify after wrangler.toml force-trigger."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/352_cf_force_verify.json"
NAME = "justhodl-cf-force"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request
def lambda_handler(event, context):
    out = {}
    try:
        req = urllib.request.Request("https://api.justhodl.ai/agent/chart-data?catalog=1",
            headers={"Origin": "https://justhodl.ai"})
        with urllib.request.urlopen(req, timeout=20) as r:
            body = r.read().decode("utf-8")
            try:
                d = json.loads(body)
                if "catalog" in d:
                    cats = list(d["catalog"].keys())
                    out["chart_data"] = {"status": r.status, "size": len(body),
                        "n_categories": len(cats), "categories": cats}
                else:
                    out["chart_data"] = {"status": r.status, "preview": body[:200]}
            except Exception:
                out["chart_data"] = {"status": r.status, "preview": body[:200]}
    except urllib.error.HTTPError as e:
        try: body = e.read().decode("utf-8")
        except: body = ""
        out["chart_data"] = {"http_err": e.code, "body": body[:200]}
    except Exception as e:
        out["chart_data"] = {"err": str(e)[:200]}
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
        out["raw"] = body[:1000]
    try: lam.delete_function(FunctionName=NAME)
    except: pass
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:2000])


if __name__ == "__main__":
    main()
