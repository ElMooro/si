#!/usr/bin/env python3
"""Step 387 — Set up daily EventBridge schedule for justhodl-global-business-cycle,
trigger initial run, verify output."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/387_gbc_setup.json"
NAME = "justhodl-tmp-gbc-setup"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
TARGET_LAMBDA = "justhodl-global-business-cycle"
RULE_NAME = "justhodl-gbc-daily"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json, time
import boto3

lam    = boto3.client("lambda",     region_name="us-east-1")
events = boto3.client("events",     region_name="us-east-1")
s3     = boto3.client("s3",         region_name="us-east-1")

TARGET = "justhodl-global-business-cycle"
RULE   = "justhodl-gbc-daily"
ACC    = "857687956942"

def lambda_handler(event, context):
    out = {}

    # 1. Verify the Lambda exists (deploy workflow should have created it)
    try:
        cfg = lam.get_function_configuration(FunctionName=TARGET)
        out["lambda_status"] = {
            "exists": True,
            "runtime": cfg["Runtime"],
            "timeout": cfg["Timeout"],
            "memory": cfg["MemorySize"],
            "last_modified": cfg["LastModified"],
        }
    except Exception as e:
        out["lambda_status"] = {"exists": False, "error": str(e)[:200]}
        return {"statusCode": 200, "body": json.dumps(out, default=str)}

    # 2. Increase timeout + memory if needed (GBC fetches 35 series)
    try:
        if cfg.get("Timeout", 0) < 300 or cfg.get("MemorySize", 0) < 512:
            lam.update_function_configuration(
                FunctionName=TARGET, Timeout=300, MemorySize=512)
            out["config_update"] = "set timeout=300, memory=512"
            time.sleep(2)
        else:
            out["config_update"] = "already adequate"
    except Exception as e:
        out["config_update"] = f"error: {e}"[:200]

    # 3. Create or update EventBridge rule (daily at 12:00 UTC)
    try:
        events.put_rule(
            Name=RULE,
            ScheduleExpression="cron(0 12 * * ? *)",
            State="ENABLED",
            Description="Daily run of justhodl-global-business-cycle (OECD CLI engine)",
        )
        out["rule_created"] = True
    except Exception as e:
        out["rule_created"] = f"error: {e}"[:200]

    # 4. Grant EventBridge permission to invoke the Lambda
    try:
        rule_arn = f"arn:aws:events:us-east-1:{ACC}:rule/{RULE}"
        lam.add_permission(
            FunctionName=TARGET,
            StatementId="EventBridge-gbc-daily",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=rule_arn,
        )
        out["permission_added"] = True
    except lam.exceptions.ResourceConflictException:
        out["permission_added"] = "already exists"
    except Exception as e:
        out["permission_added"] = f"error: {e}"[:200]

    # 5. Attach Lambda as EventBridge target
    try:
        lambda_arn = f"arn:aws:lambda:us-east-1:{ACC}:function:{TARGET}"
        events.put_targets(
            Rule=RULE,
            Targets=[{"Id": "target1", "Arn": lambda_arn}],
        )
        out["target_attached"] = True
    except Exception as e:
        out["target_attached"] = f"error: {e}"[:200]

    # 6. Trigger initial run (sync invoke)
    try:
        resp = lam.invoke(FunctionName=TARGET, InvocationType="RequestResponse", Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        out["initial_invoke"] = {
            "status_code": resp.get("StatusCode"),
            "function_error": resp.get("FunctionError"),
            "body": body[:800],
        }
    except Exception as e:
        out["initial_invoke"] = {"error": str(e)[:400]}

    time.sleep(3)

    # 7. Verify S3 output
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="data/global-business-cycle.json")
        d = json.loads(obj["Body"].read())
        agg = d.get("aggregate", {})
        by_country = d.get("by_country", {})
        n_available = sum(1 for c in by_country.values() if c.get("phase") and c["phase"] != "UNKNOWN")
        out["s3_output"] = {
            "schema_version": d.get("schema_version"),
            "generated_at": d.get("generated_at"),
            "elapsed_sec": d.get("elapsed_sec"),
            "global_phase": agg.get("global_phase"),
            "global_avg_cli": agg.get("global_avg_cli"),
            "expansion_breadth_pct": agg.get("expansion_breadth_pct"),
            "contraction_breadth_pct": agg.get("contraction_breadth_pct"),
            "global_phase_mix_pct": agg.get("global_phase_mix_pct"),
            "n_countries_total": len(by_country),
            "n_countries_classified": n_available,
            "key_countries": {iso: (by_country.get(iso) or {}).get("phase")
                                for iso in ["USA", "CHN", "DEU", "JPN", "IND",
                                              "GBR", "FRA", "BRA", "KOR", "CAN"]},
            "decisive_call": (d.get("interpretation") or {}).get("decisive_call"),
        }
    except Exception as e:
        out["s3_output"] = {"error": str(e)[:300]}

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
                            MemorySize=512, Timeout=600, Code={"ZipFile": zb})
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
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
