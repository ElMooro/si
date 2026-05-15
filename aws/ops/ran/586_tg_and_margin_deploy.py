#!/usr/bin/env python3
"""586 — Correct SSM path /justhodl/telegram/bot_token. Patch 7 new Lambdas
with TG creds + deploy margin-lending directly (since CI/CD didn't pick it up)."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/586_tg_and_margin_deploy.json"
REGION = "us-east-1"
ACCOUNT = "857687956942"
ROLE = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)

LAMBDAS_TO_PATCH = [
    "justhodl-insider-cluster-scanner",
    "justhodl-khalid-adaptive",
    "justhodl-stress-scenarios",
    "justhodl-political-trades",
    "justhodl-reversal-radar",
    "justhodl-auction-grader",
    "justhodl-repo-lending",
]


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # 1. Get TG creds from SSM with correct paths
    try:
        token = ssm.get_parameter(Name="/justhodl/telegram/bot_token",
                                    WithDecryption=True)["Parameter"]["Value"]
        chat = ssm.get_parameter(Name="/justhodl/telegram/chat_id",
                                   WithDecryption=True)["Parameter"]["Value"]
        out["creds_found"] = {"token_present": bool(token), "chat_present": bool(chat),
                                "token_prefix": token[:12] if token else None,
                                "chat_id": chat}
    except Exception as e:
        out["creds_err"] = str(e)[:200]
        return

    # 2. Patch all 7 new Lambdas
    for name in LAMBDAS_TO_PATCH:
        try:
            cfg = lam.get_function_configuration(FunctionName=name)
            env = (cfg.get("Environment") or {}).get("Variables", {}) or {}
            env["TELEGRAM_TOKEN"] = token
            env["TELEGRAM_CHAT_ID"] = chat
            lam.update_function_configuration(FunctionName=name,
                                                Environment={"Variables": env})
            lam.get_waiter("function_updated").wait(FunctionName=name)
            out.setdefault("patched", []).append(name)
        except Exception as e:
            out.setdefault("patch_errors", {})[name] = str(e)[:100]

    # 3. Deploy margin-lending directly
    src_dir = "/root/work/si/aws/lambdas/justhodl-margin-lending/source"
    if os.path.exists(src_dir):
        # Build zip
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for root, dirs, files in os.walk(src_dir):
                for fn in files:
                    if fn.endswith(".pyc") or "__pycache__" in root:
                        continue
                    fp = os.path.join(root, fn)
                    zf.write(fp, os.path.relpath(fp, src_dir))
        zip_bytes = buf.getvalue()
        out["margin_zip_kb"] = round(len(zip_bytes)/1024, 1)

        env_vars = {
            "S3_BUCKET": "justhodl-dashboard-live",
            "FRED_API_KEY": "2f057499936072679d8843d7fce99989",
            "TELEGRAM_TOKEN": token,
            "TELEGRAM_CHAT_ID": chat,
        }

        try:
            lam.create_function(
                FunctionName="justhodl-margin-lending",
                Runtime="python3.12",
                Handler="lambda_function.lambda_handler",
                Role=ROLE,
                MemorySize=256, Timeout=60,
                Code={"ZipFile": zip_bytes},
                Environment={"Variables": env_vars},
                Description="Margin debt + NYSE leverage extremes detector",
                Architectures=["arm64"],
            )
            lam.get_waiter("function_active_v2").wait(FunctionName="justhodl-margin-lending")
            out["margin_create"] = "OK"
        except lam.exceptions.ResourceConflictException:
            lam.update_function_code(FunctionName="justhodl-margin-lending", ZipFile=zip_bytes)
            lam.get_waiter("function_updated").wait(FunctionName="justhodl-margin-lending")
            lam.update_function_configuration(
                FunctionName="justhodl-margin-lending",
                Runtime="python3.12",
                Handler="lambda_function.lambda_handler",
                MemorySize=256, Timeout=60,
                Environment={"Variables": env_vars}, Role=ROLE,
            )
            lam.get_waiter("function_updated").wait(FunctionName="justhodl-margin-lending")
            out["margin_update"] = "OK"
        except Exception as e:
            out["margin_create_err"] = str(e)[:200]

        # EB rule
        try:
            rule_name = "justhodl-margin-lending-weekday-16"
            events.put_rule(Name=rule_name,
                             ScheduleExpression="cron(0 16 ? * MON-FRI *)",
                             State="ENABLED",
                             Description="Margin debt weekday after-close pull")
            try:
                lam.add_permission(
                    FunctionName="justhodl-margin-lending",
                    StatementId=f"{rule_name}-invoke",
                    Action="lambda:InvokeFunction",
                    Principal="events.amazonaws.com",
                    SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{rule_name}",
                )
            except lam.exceptions.ResourceConflictException:
                pass
            events.put_targets(Rule=rule_name, Targets=[{
                "Id": "1",
                "Arn": f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:justhodl-margin-lending",
            }])
            out["margin_eb"] = "OK"
        except Exception as e:
            out["margin_eb_err"] = str(e)[:200]

        # Smoke invoke
        _time.sleep(2)
        try:
            resp = lam.invoke(FunctionName="justhodl-margin-lending",
                               InvocationType="RequestResponse",
                               LogType="Tail", Payload=b"{}")
            out["margin_invoke_status"] = resp.get("StatusCode")
            out["margin_fn_error"] = resp.get("FunctionError")
            body = resp["Payload"].read().decode("utf-8")
            try:
                p = json.loads(body)
                out["margin_response"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
            except: out["margin_raw"] = body[:300]
            if resp.get("LogResult"):
                log = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")
                out["margin_log"] = log[-1500:]
        except Exception as e:
            out["margin_invoke_err"] = str(e)[:200]
    else:
        out["margin_src_missing"] = src_dir

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
