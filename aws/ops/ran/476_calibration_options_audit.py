#!/usr/bin/env python3
"""Step 476 — Audit calibration/latest.json + find options-flow-scanner output.

For #6: read calibration/latest.json to see what weight adjustments exist
For #7: locate the S3 output of justhodl-options-flow-scanner so we can
        wire it into the alpha-score as an 8th factor.
"""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/476_calibration_options_audit.json"
NAME = "justhodl-tmp-476"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json
import boto3
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}

    # 1. Read calibration/latest.json (#6)
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="calibration/latest.json")
        body = obj["Body"].read()
        cal = json.loads(body)
        out["calibration"] = {
            "size_kb": round(len(body)/1024, 1),
            "last_modified": obj["LastModified"].isoformat()[:19],
            "top_keys": list(cal.keys()) if isinstance(cal, dict) else None,
            "type": type(cal).__name__,
        }
        if isinstance(cal, dict):
            out["calibration"]["sample"] = {k: (str(v)[:200] if not isinstance(v,(int,float,bool,type(None))) else v)
                                              for k,v in list(cal.items())[:15]}
    except Exception as e:
        out["calibration_err"] = str(e)[:200]

    # 2. List ALL options-related S3 keys to find the scanner output
    try:
        cont = None
        all_options_keys = []
        for _ in range(10):
            args = {"Bucket": "justhodl-dashboard-live", "Prefix": "options/", "MaxKeys": 1000}
            if cont: args["ContinuationToken"] = cont
            r = s3.list_objects_v2(**args)
            for o in r.get("Contents") or []:
                all_options_keys.append({"key": o["Key"], "size_kb": round(o["Size"]/1024, 1),
                                          "lm": o["LastModified"].isoformat()[:19]})
            cont = r.get("NextContinuationToken")
            if not cont: break
        out["options_keys_all"] = sorted(all_options_keys, key=lambda x: -x["lm"].count("2026"))[:30]
    except Exception as e:
        out["options_list_err"] = str(e)[:200]

    # 3. Look at options-flow-scanner's environment + recent invocations
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-options-flow-scanner")
        out["options_scanner"] = {
            "last_modified": cfg["LastModified"][:19],
            "memory": cfg["MemorySize"],
            "timeout": cfg["Timeout"],
            "env_keys": list((cfg.get("Environment") or {}).get("Variables", {}).keys()),
            "description": cfg.get("Description", "")[:200],
        }
    except Exception as e:
        out["options_scanner_err"] = str(e)[:200]

    # 4. Try common scanner output key names
    candidates = [
        "options/flow-scanner-latest.json", "options/flow-scanner.json",
        "options/scanner.json", "options/unusual-activity.json",
        "options/sp500-flow.json", "options/stocks-flow.json",
        "options/latest.json", "options/flow-latest.json",
        "options/sweeps.json", "options/by-symbol.json",
    ]
    out["candidate_probes"] = {}
    for k in candidates:
        try:
            head = s3.head_object(Bucket="justhodl-dashboard-live", Key=k)
            out["candidate_probes"][k] = {"exists": True, "size_kb": round(head["ContentLength"]/1024, 1),
                                            "lm": head["LastModified"].isoformat()[:19]}
        except Exception:
            out["candidate_probes"][k] = {"exists": False}

    # 5. Read calibration history index
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="calibration/history-index.json")
        cal_idx = json.loads(obj["Body"].read())
        out["calibration_history_index"] = cal_idx
    except Exception as e:
        out["calibration_history_idx_err"] = str(e)[:200]

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
