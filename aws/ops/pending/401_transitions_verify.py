#!/usr/bin/env python3
"""Step 401 — Verify phase-transition pipeline:
  - Trigger GBC Lambda
  - Confirm history JSON has `transitions` array with full detail
  - Confirm page contains drawTransitionsOverlay + renderTransitionsList
  - Print the detected transitions for human review
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/401_transitions_verify.json"
NAME = "justhodl-tmp-transitions"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request, time
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3  = boto3.client("s3",     region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")
TARGET = "justhodl-global-business-cycle"

def fetch(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent":"JH-verify/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="replace"), r.status

def lambda_handler(event, context):
    out = {}
    cfg = lam.get_function_configuration(FunctionName=TARGET)
    out["lambda_last_modified"] = cfg["LastModified"]
    out["code_size"] = cfg["CodeSize"]

    resp = lam.invoke(FunctionName=TARGET, InvocationType="RequestResponse", Payload=b"{}")
    out["invoke_body"] = resp["Payload"].read().decode("utf-8")[:300]

    time.sleep(3)

    # History JSON
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="data/global-business-cycle-history.json")
        h = json.loads(obj["Body"].read())
        trans = h.get("transitions", [])
        out["history"] = {
            "schema": h.get("schema_version"),
            "transitions_count": h.get("transitions_count"),
            "aggregate_dates": len(h.get("aggregate") or []),
            "transitions": trans,
        }
    except Exception as e:
        out["history"] = {"error": str(e)[:300]}

    # Page check
    try:
        page, status = fetch("https://justhodl.ai/global-cycle/?cb=" + str(int(time.time())))
        out["page"] = {
            "status": status,
            "size": len(page),
            "has_drawTransitionsOverlay": "drawTransitionsOverlay" in page,
            "has_renderTransitionsList": "renderTransitionsList" in page,
            "has_transitions_hero_card": "PHASE TRANSITIONS" in page,
            "has_trans_list": 'id="transList"' in page,
            "has_transition_line_css": "transition-line" in page,
        }
    except Exception as e:
        out["page"] = {"error": str(e)[:200]}

    # CloudWatch
    try:
        lg = f"/aws/lambda/{TARGET}"
        streams = logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime",
                                              descending=True, limit=1)
        if streams.get("logStreams"):
            stream = streams["logStreams"][0]["logStreamName"]
            ev = logs.get_log_events(logGroupName=lg, logStreamName=stream,
                                      startFromHead=False, limit=80)
            lines = [e["message"].strip() for e in ev.get("events", [])]
            out["log_transitions"] = [l for l in lines if "transition" in l.lower() or "[gbc-history]" in l][-20:]
    except Exception as e:
        out["log_err"] = str(e)[:200]

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
