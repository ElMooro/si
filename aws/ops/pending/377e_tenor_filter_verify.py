#!/usr/bin/env python3
"""Step 377b — Sync-invoke tenor-signal-interpreter + verify output."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/377b_tenor_invoke_verify.json"
NAME = "justhodl-tmp-tenor-verify"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json, urllib.request
import boto3

lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    # Sync invoke the tenor lambda
    try:
        resp = lam.invoke(FunctionName="justhodl-tenor-signal-interpreter",
                            InvocationType="RequestResponse", Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        out["invoke"] = {"status": resp.get("StatusCode"), "body": body[:600]}
    except Exception as e:
        out["invoke"] = {"error": str(e)}
        return {"statusCode": 200, "body": json.dumps(out, default=str)}

    # Read S3 output
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="data/auction-tenor-signals.json")
        body = obj["Body"].read()
        d = json.loads(body)
        sigs = d.get("signals", {})
        out["s3_output"] = {
            "size_bytes": len(body),
            "last_modified": str(obj.get("LastModified")),
            "schema_version": d.get("schema_version"),
            "composite_score": d.get("composite_score"),
            "any_firing": d.get("any_firing"),
            "any_watch": d.get("any_watch"),
            "fed_funds_rate": d.get("fed_funds_rate"),
            "n_auctions_in_window": d.get("n_auctions_in_window"),
            "transitions_count": len(d.get("transitions", [])),
            "states": {ch: s.get("state") for ch, s in sigs.items()},
            "fed_path_metrics": sigs.get("fed_path", {}).get("metrics"),
            "fed_path_interpretation": sigs.get("fed_path", {}).get("interpretation"),
            "eurodollar_metrics": sigs.get("eurodollar", {}).get("metrics"),
            "eurodollar_interpretation": sigs.get("eurodollar", {}).get("interpretation"),
            "qe_metrics": sigs.get("qe_imminence", {}).get("metrics"),
            "qe_interpretation": sigs.get("qe_imminence", {}).get("interpretation"),
        }
    except Exception as e:
        out["s3_output"] = {"error": str(e)}

    # Verify js module is live
    try:
        req = urllib.request.Request("https://justhodl.ai/tenor-signals.js",
                                       headers={"User-Agent": "verify"})
        with urllib.request.urlopen(req, timeout=10) as r:
            js = r.read().decode("utf-8")
            out["js_module_live"] = {"status": r.status, "size": len(js),
                                       "has_pill": "jh-tenor-pill" in js}
    except Exception as e:
        out["js_module_live"] = {"error": str(e)}

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
                            MemorySize=256, Timeout=180, Code={"ZipFile": zb})
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
