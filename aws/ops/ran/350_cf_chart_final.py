#!/usr/bin/env python3
"""Step 350 — Verify api.justhodl.ai/agent/chart-data after forced CF Worker redeploy."""
import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone

import boto3

REPORT = "aws/ops/reports/350_cf_chart_final.json"
NAME = "justhodl-cf-final"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

lam = boto3.client("lambda", region_name="us-east-1")


DIAG_CODE = '''
import json, urllib.request

def lambda_handler(event, context):
    out = {}
    
    for label, url in [
        ("catalog", "https://api.justhodl.ai/agent/chart-data?catalog=1"),
        ("dgs10",   "https://api.justhodl.ai/agent/chart-data?series=DGS10&from=2024-01-01"),
        ("spy",     "https://api.justhodl.ai/agent/chart-data?series=SPY&kind=stock&from=2024-01-01"),
    ]:
        try:
            req = urllib.request.Request(url,
                headers={"Origin": "https://justhodl.ai", "User-Agent": "verify/1.0"})
            with urllib.request.urlopen(req, timeout=20) as r:
                body = r.read().decode("utf-8")
                try:
                    parsed = json.loads(body)
                    out[label] = {
                        "status": r.status, "size": len(body),
                        "n_obs": parsed.get("n_obs") or len(parsed.get("catalog", {})),
                        "source": parsed.get("source"),
                        "preview": body[:120],
                    }
                except Exception:
                    out[label] = {"status": r.status, "preview": body[:300]}
        except Exception as e:
            out[label] = {"err": str(e)[:300]}
    
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
        try:
            lam.update_function_code(FunctionName=NAME, ZipFile=zb)
            lam.get_waiter("function_updated").wait(FunctionName=NAME)
        except Exception:
            pass

    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        out["test"] = json.loads(json.loads(body).get("body", "{}"))
    except Exception:
        out["test_raw"] = body[:1500]

    try:
        lam.delete_function(FunctionName=NAME)
    except Exception:
        pass

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:3000])


if __name__ == "__main__":
    main()
