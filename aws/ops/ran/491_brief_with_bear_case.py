#!/usr/bin/env python3
"""Step 491 — Invoke updated daily-brief Lambda and capture the markdown
to confirm the bear case made it into the brief."""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/491_brief_with_bear_case.json"
NAME = "justhodl-tmp-491"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, boto3, base64
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    try:
        resp = lam.invoke(FunctionName="justhodl-alpha-daily-brief",
                            InvocationType="RequestResponse", LogType="Tail",
                            Payload=b"{}")
        out["invoke_status"] = resp.get("StatusCode")
        out["fn_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        try:
            parsed = json.loads(body)
            out["invoke_response"] = json.loads(parsed["body"]) if parsed.get("body") else parsed
        except: out["invoke_raw"] = body[:1000]
        if resp.get("LogResult"):
            tail = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")
            out["log_tail"] = tail[-1500:]
    except Exception as e:
        out["invoke_err"] = str(e)[:300]
    # Fetch the freshly-written markdown
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/alpha-brief.md")
        body = obj["Body"].read().decode("utf-8")
        out["brief_markdown"] = body
        out["brief_size"] = len(body)
        out["brief_modified"] = obj["LastModified"].isoformat()[:19]
        # Sanity: count occurrences of bear case strings
        out["sanity"] = {
            "contains_bear_case_label": "Bear case" in body or "bear case" in body.lower(),
            "contains_short_persona": "Short persona" in body or "short persona" in body.lower(),
            "n_bear_mentions": body.lower().count("bear case"),
            "contains_debate_section": "DEBATE DISSENT" in body or "5-persona" in body.lower(),
            "contains_macro_stress": "macro stress" in body.lower() or "MSS" in body,
        }
    except Exception as e:
        out["brief_err"] = str(e)[:200]
    # Fetch the JSON payload too for context_summary
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/alpha-brief.json")
        out["brief_json"] = json.loads(obj["Body"].read())
    except Exception as e:
        out["brief_json_err"] = str(e)[:200]
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    print("Waiting 150s for deploy...")
    _time.sleep(150)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=512, Timeout=120, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    _time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed and parsed["body"] else parsed
    except Exception:
        out["raw"] = body[:30000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
