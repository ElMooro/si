#!/usr/bin/env python3
"""Step 471 — Read outputs of existing Lambdas so we know what's there to connect to."""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/471_existing_outputs.json"
NAME = "justhodl-tmp-471"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json
import boto3
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
ddb = boto3.client("dynamodb", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    BUCKET = "justhodl-dashboard-live"

    # Full S3 listing to find all relevant keys
    try:
        all_keys = []
        cont_token = None
        for _ in range(5):
            args = {"Bucket": BUCKET, "MaxKeys": 1000}
            if cont_token: args["ContinuationToken"] = cont_token
            resp = s3.list_objects_v2(**args)
            all_keys.extend([{"key": o["Key"], "size_kb": round(o["Size"]/1024,1),
                              "lm": o["LastModified"].isoformat()[:19]}
                              for o in resp.get("Contents") or []])
            cont_token = resp.get("NextContinuationToken")
            if not cont_token: break
        all_keys.sort(key=lambda x: x["key"])
        out["total_s3_keys"] = len(all_keys)
        # Categorize
        macro = [k for k in all_keys if any(t in k["key"].lower() for t in ["macro","regime","nowcast","khalid","lce","vol-regime","bond"])]
        backtest = [k for k in all_keys if any(t in k["key"].lower() for t in ["backtest","outcome","calibrat","signal"])]
        options = [k for k in all_keys if "option" in k["key"].lower() or "flow" in k["key"].lower()]
        alerts = [k for k in all_keys if any(t in k["key"].lower() for t in ["alert","brief","telegram","redflag"])]
        out["macro_keys"] = macro[:30]
        out["backtest_keys"] = backtest[:30]
        out["options_keys"] = options[:20]
        out["alert_keys"] = alerts[:20]
    except Exception as e:
        out["s3_err"] = str(e)[:300]

    # For each known existing Lambda, get its env vars + last modified
    LAMBDAS = [
        "justhodl-macro-nowcast",
        "justhodl-cross-asset-regime",
        "justhodl-options-flow-scanner",
        "justhodl-options-flow",
        "justhodl-alert-router",
        "justhodl-telegram-bot",
        "justhodl-morning-brief-tg",
        "justhodl-calibrator",
        "justhodl-backtest-engine",
        "justhodl-screener-alerts",
        "justhodl-ai-brief",
        "justhodl-redflag-alerter",
    ]
    out["lambda_details"] = {}
    for name in LAMBDAS:
        try:
            cfg = lam.get_function_configuration(FunctionName=name)
            out["lambda_details"][name] = {
                "last_modified": cfg["LastModified"],
                "memory": cfg["MemorySize"], "timeout": cfg["Timeout"],
                "env_keys": list((cfg.get("Environment") or {}).get("Variables", {}).keys()),
                "description": cfg.get("Description","")[:200],
            }
        except Exception as e:
            out["lambda_details"][name] = {"err": str(e)[:120]}

    # Read a few critical sidecars
    READ_KEYS = [
        "macro/nowcast.json",
        "macro/regime-data.json",
        "macro/state.json",
        "khalid_index/data.json",
        "khalid-index.json",
        "options/flow-latest.json",
        "options/flow-scanner-latest.json",
        "options/scanner-latest.json",
    ]
    out["sidecar_samples"] = {}
    for k in READ_KEYS:
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=k)
            body = obj["Body"].read()
            data = json.loads(body)
            preview = {kk: (str(vv)[:80] if not isinstance(vv,(int,float,bool,type(None))) else vv)
                          for kk, vv in (data.items() if isinstance(data, dict) else [])}
            preview_keys = list(preview.keys())[:15]
            out["sidecar_samples"][k] = {
                "size_kb": round(len(body)/1024, 1),
                "type": type(data).__name__,
                "keys": preview_keys,
                "sample": {kk: preview[kk] for kk in preview_keys[:8]} if isinstance(data, dict) else None,
            }
        except Exception as e:
            out["sidecar_samples"][k] = {"err": str(e)[:100]}

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
                            MemorySize=512, Timeout=180, Code={"ZipFile": zb})
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
