#!/usr/bin/env python3
"""Step 355 — Verify ECB ILM date normalization."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/355_ecb_date_verify.json"
NAME = "justhodl-test-v7"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request, re

def lambda_handler(event, context):
    out = {}
    base = "https://zsgb72zf4ayw6ajw7phbyq6wzq0haobh.lambda-url.us-east-1.on.aws/"
    
    tests = [
        ("ecb_ilm_claims", "ilm_claims_fx", "ecb"),
        ("ecb_ilm_liab", "ilm_liab_eur", "ecb"),
        ("ecb_sovciss_de", "sovciss_de", "ecb"),  # Monthly format
    ]
    
    for label, sid, kind in tests:
        url = f"{base}?series={sid}&kind={kind}"
        try:
            req = urllib.request.Request(url,
                headers={"Origin": "https://justhodl.ai"})
            with urllib.request.urlopen(req, timeout=30) as r:
                d = json.loads(r.read().decode("utf-8"))
                first = (d.get("data") or [{}])[0]
                last = (d.get("data") or [{}])[-1]
                # Validate dates are ISO YYYY-MM-DD
                first_t = first.get("time", "")
                last_t = last.get("time", "")
                is_iso_first = bool(re.match(r"^\\d{4}-\\d{2}-\\d{2}$", first_t))
                is_iso_last = bool(re.match(r"^\\d{4}-\\d{2}-\\d{2}$", last_t))
                out[label] = {
                    "n_obs": d.get("n_obs"),
                    "first_date": first_t, "first_iso": is_iso_first,
                    "last_date": last_t, "last_iso": is_iso_last,
                    "first_value": first.get("value"),
                    "last_value": last.get("value"),
                    "all_iso": is_iso_first and is_iso_last,
                }
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
            MemorySize=256, Timeout=300, Code={"ZipFile": buf.getvalue()})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        try:
            lam.update_function_code(FunctionName=NAME, ZipFile=buf.getvalue())
            lam.get_waiter("function_updated").wait(FunctionName=NAME)
            lam.update_function_configuration(FunctionName=NAME, Timeout=300)
            lam.get_waiter("function_updated").wait(FunctionName=NAME)
        except Exception as e:
            out["err"] = str(e)
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


if __name__ == "__main__":
    main()
