#!/usr/bin/env python3
"""Step 388 — Create justhodl-global-business-cycle Lambda directly from repo,
set up env vars, EventBridge schedule, trigger initial run, verify."""
import io, json, os, time, zipfile, urllib.request
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/388_gbc_create.json"
NAME = "justhodl-tmp-gbc-create"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

# Read the Lambda source from the repo (local)
LAMBDA_SOURCE = open("aws/lambdas/justhodl-global-business-cycle/source/lambda_function.py").read()

DIAG_CODE = '''
import json, time, io, zipfile, base64
import boto3

lam    = boto3.client("lambda",     region_name="us-east-1")
events = boto3.client("events",     region_name="us-east-1")
s3     = boto3.client("s3",         region_name="us-east-1")

TARGET = "justhodl-global-business-cycle"
RULE   = "justhodl-gbc-daily"
ROLE   = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACC    = "857687956942"
BUCKET = "justhodl-dashboard-live"

def lambda_handler(event, context):
    out = {}
    source = base64.b64decode(event["source_b64"]).decode("utf-8")

    # Build deployment zip
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", source)
    zip_bytes = buf.getvalue()

    # 1. Create or update the Lambda function
    try:
        lam.get_function_configuration(FunctionName=TARGET)
        # Exists — update code + config
        lam.update_function_code(FunctionName=TARGET, ZipFile=zip_bytes)
        lam.get_waiter("function_updated").wait(FunctionName=TARGET)
        lam.update_function_configuration(
            FunctionName=TARGET, Timeout=300, MemorySize=512,
            Environment={"Variables": {
                "FRED_KEY": "2f057499936072679d8843d7fce99989",
                "S3_BUCKET": BUCKET,
            }},
        )
        out["lambda_action"] = "updated_existing"
    except lam.exceptions.ResourceNotFoundException:
        # Create new
        lam.create_function(
            FunctionName=TARGET,
            Runtime="python3.12",
            Role=ROLE,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zip_bytes},
            Timeout=300,
            MemorySize=512,
            Environment={"Variables": {
                "FRED_KEY": "2f057499936072679d8843d7fce99989",
                "S3_BUCKET": BUCKET,
            }},
            Description="Global Business Cycle engine — OECD CLI across 35 economies, phase classification, regional/global aggregation, interpretation. Schedule: daily.",
            Tags={"system": "justhodl", "category": "macro", "owner": "khalid"},
        )
        out["lambda_action"] = "created_new"

    time.sleep(3)
    lam.get_waiter("function_active_v2").wait(FunctionName=TARGET)

    # 2. EventBridge schedule (daily at 12:00 UTC)
    try:
        events.put_rule(
            Name=RULE,
            ScheduleExpression="cron(0 12 * * ? *)",
            State="ENABLED",
            Description="Daily justhodl-global-business-cycle (OECD CLI)",
        )
        out["rule_created"] = True
    except Exception as e:
        out["rule_created"] = f"error: {e}"[:200]

    try:
        lam.add_permission(
            FunctionName=TARGET,
            StatementId="EventBridge-gbc-daily",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:us-east-1:{ACC}:rule/{RULE}",
        )
        out["permission_added"] = True
    except lam.exceptions.ResourceConflictException:
        out["permission_added"] = "already exists"
    except Exception as e:
        out["permission_added"] = f"error: {e}"[:200]

    try:
        events.put_targets(
            Rule=RULE,
            Targets=[{"Id": "target1",
                       "Arn": f"arn:aws:lambda:us-east-1:{ACC}:function:{TARGET}"}],
        )
        out["target_attached"] = True
    except Exception as e:
        out["target_attached"] = f"error: {e}"[:200]

    # 3. Trigger initial run
    try:
        resp = lam.invoke(FunctionName=TARGET,
                            InvocationType="RequestResponse", Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        out["initial_invoke"] = {
            "status": resp.get("StatusCode"),
            "error": resp.get("FunctionError"),
            "body": body[:600],
        }
    except Exception as e:
        out["initial_invoke"] = {"error": str(e)[:300]}

    time.sleep(3)

    # 4. Verify S3 output
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/global-business-cycle.json")
        d = json.loads(obj["Body"].read())
        agg = d.get("aggregate", {})
        by_country = d.get("by_country", {})
        n_avail = sum(1 for c in by_country.values()
                       if c.get("phase") and c["phase"] != "UNKNOWN")
        out["s3_output"] = {
            "generated_at": d.get("generated_at"),
            "elapsed_sec": d.get("elapsed_sec"),
            "global_phase": agg.get("global_phase"),
            "global_avg_cli": agg.get("global_avg_cli"),
            "expansion_breadth_pct": agg.get("expansion_breadth_pct"),
            "contraction_breadth_pct": agg.get("contraction_breadth_pct"),
            "phase_mix": agg.get("global_phase_mix_pct"),
            "n_total": len(by_country),
            "n_classified": n_avail,
            "key_countries": {iso: {"phase": (by_country.get(iso) or {}).get("phase"),
                                       "cli": (by_country.get(iso) or {}).get("cli_level")}
                                for iso in ["USA","CHN","DEU","JPN","IND","GBR","FRA",
                                              "BRA","KOR","CAN","AUS","MEX","IDN","TUR"]},
            "decisive_call": (d.get("interpretation") or {}).get("decisive_call"),
        }
    except Exception as e:
        out["s3_output"] = {"error": str(e)[:300]}

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    import base64
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG_CODE)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=512, Timeout=600, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    time.sleep(2)
    payload = json.dumps({"source_b64": base64.b64encode(LAMBDA_SOURCE.encode()).decode()})
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                       Payload=payload.encode())
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed else parsed
    except Exception:
        out["raw"] = body[:5000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
