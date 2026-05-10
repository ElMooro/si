#!/usr/bin/env python3
"""Step 381 — End-to-end validation: invoke daily-report-v3 then check report.json."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/381_lce_integration_e2e.json"
NAME = "justhodl-tmp-lce-e2e"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json, time
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}

    # 1. Force daily-report-v3 to run
    try:
        resp = lam.invoke(FunctionName="justhodl-daily-report-v3",
                          InvocationType="RequestResponse", Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        out["daily_report_invoke"] = {"status": resp.get("StatusCode"),
                                        "body": body[:600]}
    except Exception as e:
        out["daily_report_invoke"] = {"error": str(e)[:300]}

    time.sleep(3)

    # 2. Read fresh report.json
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/report.json")
        d = json.loads(obj["Body"].read())

        ki = d.get("khalid_index", {})
        risk = d.get("risk_dashboard", {})
        lce_in_report = d.get("liquidity_credit_engine", {})
        tenor_in_report = d.get("tenor_signals", {})

        out["report_check"] = {
            "ki_score": ki.get("score"),
            "ki_regime": ki.get("regime"),
            "ki_lce_state": ki.get("lce_state"),
            "ki_lce_composite": ki.get("lce_composite"),
            "ki_tenor_state": ki.get("tenor_state"),
            "ki_signals_with_lce": [s for s in (ki.get("signals") or [])
                                       if s and ("LCE" in str(s) or "Tenor" in str(s))][:5],
            "risk_composite": risk.get("composite"),
            "risk_credit": risk.get("credit"),
            "risk_liquidity": risk.get("liquidity"),
            "risk_lce_overlay": risk.get("lce_overlay"),
            "report_has_lce_top_level": bool(lce_in_report),
            "lce_top_level_regime": lce_in_report.get("regime"),
            "lce_top_level_composite": lce_in_report.get("composite"),
            "report_has_tenor_top_level": bool(tenor_in_report),
            "tenor_top_level_composite": tenor_in_report.get("composite_score"),
        }
    except Exception as e:
        out["report_check"] = {"error": str(e)[:300]}

    # 3. Trigger allocator to verify LCE rule fires
    try:
        resp = lam.invoke(FunctionName="justhodl-allocator",
                          InvocationType="RequestResponse", Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        out["allocator_invoke"] = {"status": resp.get("StatusCode"),
                                     "body": body[:600]}
    except Exception as e:
        out["allocator_invoke"] = {"error": str(e)[:300]}

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
                            MemorySize=256, Timeout=600, Code={"ZipFile": zb})
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
