#!/usr/bin/env python3
"""587 — Direct deploy margin-lending using correct repo path detection."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/587_margin_lending_direct.json"
REGION = "us-east-1"
ACCOUNT = "857687956942"
ROLE = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"
NAME = "justhodl-margin-lending"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    out["cwd"] = os.getcwd()
    out["GITHUB_WORKSPACE"] = os.environ.get("GITHUB_WORKSPACE", "(not set)")

    # Try multiple paths
    candidates = [
        os.path.join(os.environ.get("GITHUB_WORKSPACE", ""), "aws/lambdas/justhodl-margin-lending/source"),
        os.path.join(os.getcwd(), "aws/lambdas/justhodl-margin-lending/source"),
        "aws/lambdas/justhodl-margin-lending/source",
        "/github/workspace/aws/lambdas/justhodl-margin-lending/source",
    ]
    src_dir = None
    for c in candidates:
        if c and os.path.exists(os.path.join(c, "lambda_function.py")):
            src_dir = c
            break
    out["src_dir_resolved"] = src_dir
    out["candidates_checked"] = candidates

    if not src_dir:
        out["err"] = "source not found in any candidate path"
        out["finished"] = datetime.now(timezone.utc).isoformat()
        os.makedirs(os.path.dirname(REPORT), exist_ok=True)
        with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)
        return

    # Get TG creds (correct path)
    try:
        token = ssm.get_parameter(Name="/justhodl/telegram/bot_token",
                                    WithDecryption=True)["Parameter"]["Value"]
        chat = ssm.get_parameter(Name="/justhodl/telegram/chat_id",
                                   WithDecryption=True)["Parameter"]["Value"]
    except Exception as e:
        out["creds_err"] = str(e)[:150]
        token = chat = ""

    # Build zip
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(src_dir):
            for fn in files:
                if fn.endswith(".pyc") or "__pycache__" in root: continue
                fp = os.path.join(root, fn)
                zf.write(fp, os.path.relpath(fp, src_dir))
    zip_bytes = buf.getvalue()
    out["zip_kb"] = round(len(zip_bytes)/1024, 1)

    env_vars = {
        "S3_BUCKET": "justhodl-dashboard-live",
        "FRED_API_KEY": "2f057499936072679d8843d7fce99989",
        "TELEGRAM_TOKEN": token,
        "TELEGRAM_CHAT_ID": chat,
    }

    try:
        lam.create_function(
            FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE,
            MemorySize=256, Timeout=60,
            Code={"ZipFile": zip_bytes},
            Environment={"Variables": env_vars},
            Description="Margin debt + NYSE leverage extremes detector",
            Architectures=["arm64"],
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
        out["action"] = "create"
    except lam.exceptions.ResourceConflictException:
        lam.update_function_code(FunctionName=NAME, ZipFile=zip_bytes)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
        lam.update_function_configuration(
            FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            MemorySize=256, Timeout=60,
            Environment={"Variables": env_vars}, Role=ROLE,
        )
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
        out["action"] = "update"
    except Exception as e:
        out["deploy_err"] = str(e)[:200]
        out["finished"] = datetime.now(timezone.utc).isoformat()
        os.makedirs(os.path.dirname(REPORT), exist_ok=True)
        with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)
        return

    # EB rule
    try:
        rule_name = "justhodl-margin-lending-weekday-16"
        events.put_rule(Name=rule_name,
                         ScheduleExpression="cron(0 16 ? * MON-FRI *)",
                         State="ENABLED",
                         Description="Margin debt weekday after-close pull")
        try:
            lam.add_permission(
                FunctionName=NAME, StatementId=f"{rule_name}-invoke",
                Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{rule_name}",
            )
        except lam.exceptions.ResourceConflictException: pass
        events.put_targets(Rule=rule_name, Targets=[{
            "Id": "1",
            "Arn": f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{NAME}",
        }])
        out["eb"] = "OK"
    except Exception as e:
        out["eb_err"] = str(e)[:150]

    _time.sleep(3)

    # Smoke test
    try:
        resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                           LogType="Tail", Payload=b"{}")
        out["invoke_status"] = resp.get("StatusCode")
        out["fn_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            out["response"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except: out["raw"] = body[:300]
        if resp.get("LogResult"):
            log = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")
            out["log_tail"] = log[-1500:]
    except Exception as e:
        out["invoke_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
