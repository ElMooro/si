#!/usr/bin/env python3
"""582 — Bootstrap justhodl-repo-lending: direct deploy, attach EB rule,
patch FRED + Telegram env, smoke test."""
import io, json, os, time as _time, base64, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/582_repo_lending_bootstrap.json"
NAME = "justhodl-repo-lending"
ACCOUNT = "857687956942"
REGION = "us-east-1"
ROLE = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"
RULE_NAME = f"{NAME}-daily"
SCHEDULE = "cron(0 16 ? * MON-FRI *)"
SOURCE_DIR = "aws/lambdas/justhodl-repo-lending/source"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _d, files in os.walk(SOURCE_DIR):
            for fn in files:
                if fn.endswith(".pyc") or "__pycache__" in root: continue
                fp = os.path.join(root, fn)
                zf.write(fp, os.path.relpath(fp, SOURCE_DIR))
    return buf.getvalue()


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        zip_bytes = build_zip()
        out["zip_size_kb"] = round(len(zip_bytes)/1024, 1)

        env_vars = {
            "S3_BUCKET": "justhodl-dashboard-live",
            "FRED_API_KEY": "2f057499936072679d8843d7fce99989",
        }
        try:
            env_vars["TELEGRAM_TOKEN"] = ssm.get_parameter(
                Name="/justhodl/telegram/token", WithDecryption=True
            )["Parameter"]["Value"]
            env_vars["TELEGRAM_CHAT_ID"] = ssm.get_parameter(
                Name="/justhodl/telegram/chat_id", WithDecryption=True
            )["Parameter"]["Value"]
            out["tg_loaded"] = True
        except Exception as e:
            out["tg_err"] = str(e)[:100]

        try:
            lam.get_function(FunctionName=NAME)
            lam.update_function_code(FunctionName=NAME, ZipFile=zip_bytes)
            lam.get_waiter("function_updated").wait(FunctionName=NAME)
            lam.update_function_configuration(
                FunctionName=NAME, Runtime="python3.12",
                Handler="lambda_function.lambda_handler",
                MemorySize=256, Timeout=90, Role=ROLE,
                Environment={"Variables": env_vars},
            )
            lam.get_waiter("function_updated").wait(FunctionName=NAME)
            out["action"] = "update"
        except lam.exceptions.ResourceNotFoundException:
            lam.create_function(
                FunctionName=NAME, Runtime="python3.12",
                Handler="lambda_function.lambda_handler", Role=ROLE,
                MemorySize=256, Timeout=90, Architectures=["arm64"],
                Code={"ZipFile": zip_bytes},
                Environment={"Variables": env_vars},
                Description="Leverage stress + securities lending intelligence",
            )
            lam.get_waiter("function_active").wait(FunctionName=NAME)
            out["action"] = "create"

        # EB rule
        events.put_rule(Name=RULE_NAME, ScheduleExpression=SCHEDULE, State="ENABLED",
                         Description="Repo + lending daily")
        try:
            lam.add_permission(
                FunctionName=NAME, StatementId=f"{RULE_NAME}-invoke",
                Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{RULE_NAME}",
            )
        except lam.exceptions.ResourceConflictException: pass
        events.put_targets(Rule=RULE_NAME, Targets=[{
            "Id": "1",
            "Arn": f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{NAME}",
        }])
        out["eventbridge"] = "OK"

        _time.sleep(3)
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
        import traceback
        out["fatal"] = str(e)[:200]
        out["tb"] = traceback.format_exc()[:1500]

    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/repo-lending.json")
        out["sidecar"] = json.loads(obj["Body"].read())
    except Exception as e:
        out["sidecar_err"] = str(e)[:120]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
