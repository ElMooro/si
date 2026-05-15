#!/usr/bin/env python3
"""574 — Bootstrap justhodl-khalid-adaptive: create Lambda if missing,
attach Telegram env from SSM, register cron(20 * ? * * *), invoke once."""
import io, json, os, time as _time, base64, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/574_khalid_adaptive_bootstrap.json"
NAME = "justhodl-khalid-adaptive"
ACCOUNT = "857687956942"
REGION = "us-east-1"
ROLE = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"
RULE_NAME = f"{NAME}-hourly"
SCHEDULE = "cron(20 * ? * * *)"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Wait for auto-deploy if pending
    for i in range(20):
        try:
            cfg = lam.get_function(FunctionName=NAME)["Configuration"]
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") == "Successful":
                out["lambda_last_modified"] = cfg.get("LastModified")
                out["lambda_state"] = cfg.get("State")
                break
        except Exception: pass
        _time.sleep(6)

    # Patch Telegram env
    try:
        cfg = lam.get_function_configuration(FunctionName=NAME)
        env = (cfg.get("Environment") or {}).get("Variables", {}) or {}
        patched = False
        if not env.get("TELEGRAM_TOKEN"):
            try:
                env["TELEGRAM_TOKEN"] = ssm.get_parameter(
                    Name="/justhodl/telegram/token", WithDecryption=True
                )["Parameter"]["Value"]
                patched = True
            except Exception: pass
        if not env.get("TELEGRAM_CHAT_ID"):
            try:
                env["TELEGRAM_CHAT_ID"] = ssm.get_parameter(
                    Name="/justhodl/telegram/chat_id", WithDecryption=True
                )["Parameter"]["Value"]
                patched = True
            except Exception: pass
        if patched:
            lam.update_function_configuration(FunctionName=NAME,
                                                Environment={"Variables": env})
            lam.get_waiter("function_updated").wait(FunctionName=NAME)
            out["env_patched"] = "OK"
    except Exception as e:
        out["env_err"] = str(e)[:200]

    # Register EB rule
    try:
        events.put_rule(Name=RULE_NAME, ScheduleExpression=SCHEDULE, State="ENABLED",
                         Description="Adaptive Khalid Index hourly")
        try:
            lam.add_permission(
                FunctionName=NAME, StatementId=f"{RULE_NAME}-invoke",
                Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{RULE_NAME}",
            )
        except lam.exceptions.ResourceConflictException:
            pass
        events.put_targets(Rule=RULE_NAME, Targets=[{
            "Id": "1",
            "Arn": f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{NAME}",
        }])
        out["eventbridge_rule"] = "OK"
    except Exception as e:
        out["eventbridge_err"] = str(e)[:200]

    _time.sleep(3)

    # Smoke test invoke
    try:
        resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                           LogType="Tail", Payload=b"{}")
        out["invoke_status"] = resp.get("StatusCode")
        out["fn_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            out["response"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except: out["raw"] = body[:500]
        if resp.get("LogResult"):
            log = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")
            out["log_tail"] = log[-2200:]
    except Exception as e:
        out["invoke_err"] = str(e)[:200]

    # Read sidecar
    s3 = boto3.client("s3", region_name=REGION)
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/khalid-adaptive.json")
        body = obj["Body"].read()
        out["sidecar"] = json.loads(body)
    except Exception as e:
        out["sidecar_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
