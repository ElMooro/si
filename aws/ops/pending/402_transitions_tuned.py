#!/usr/bin/env python3
"""Step 402 — Re-verify transitions after tuning (min_dwell 5w, chart filter 6w)."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3
REPORT = "aws/ops/reports/402_transitions_tuned.json"
NAME = "justhodl-tmp-tr-tuned"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request, time
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3  = boto3.client("s3",     region_name="us-east-1")

def fetch(url, t=15):
    req = urllib.request.Request(url, headers={"User-Agent":"JH/1.0"})
    with urllib.request.urlopen(req, timeout=t) as r: return r.read().decode(), r.status

def lambda_handler(event, context):
    out = {}
    cfg = lam.get_function_configuration(FunctionName="justhodl-global-business-cycle")
    out["lambda_last_modified"] = cfg["LastModified"]

    resp = lam.invoke(FunctionName="justhodl-global-business-cycle",
                       InvocationType="RequestResponse", Payload=b"{}")
    out["invoke_body"] = resp["Payload"].read().decode("utf-8")[:280]

    time.sleep(3)

    obj = s3.get_object(Bucket="justhodl-dashboard-live",
                          Key="data/global-business-cycle-history.json")
    h = json.loads(obj["Body"].read())
    trans = h.get("transitions", [])
    out["schema"] = h.get("schema_version")
    out["total_transitions"] = len(trans)
    chart_visible = [t for t in trans if (t.get("weeks_persisted") or 0) >= 6]
    out["chart_visible_count"] = len(chart_visible)
    out["transitions"] = trans
    out["chart_visible_transitions"] = chart_visible

    # Page check
    try:
        page, status = fetch("https://justhodl.ai/global-cycle/?cb=" + str(int(time.time())))
        out["page_status"] = status
        out["page_size"] = len(page)
        out["chart_filter_text_present"] = "chart shows persistence" in page
    except Exception as e:
        out["page_err"] = str(e)[:200]

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=512, Timeout=900, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed and parsed["body"] else parsed
    except Exception:
        out["raw"] = body[:8000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
