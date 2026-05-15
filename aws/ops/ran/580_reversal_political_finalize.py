#!/usr/bin/env python3
"""580 — Direct deploy justhodl-reversal-radar from repo source (auto-deploy
didn't pick it up) + patch political-trades TG env."""
import io, json, os, time as _time, base64, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/580_reversal_political_finalize.json"
NAME = "justhodl-reversal-radar"
ACCOUNT = "857687956942"
REGION = "us-east-1"
ROLE = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"
RULE_NAME = f"{NAME}-hourly"
SCHEDULE = "cron(30 * ? * * *)"
SOURCE_DIR = "aws/lambdas/justhodl-reversal-radar/source"

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

    # ─── Reversal Radar: direct create ─────────────────────────────────
    try:
        zip_bytes = build_zip()
        out["zip_size_kb"] = round(len(zip_bytes)/1024, 1)

        # Load TG creds
        env_vars = {
            "S3_BUCKET": "justhodl-dashboard-live",
            "S3_KEY_OUT": "data/reversal-radar.json",
            "S3_KEY_HISTORY": "data/reversal-radar-history.json",
        }
        try:
            env_vars["TELEGRAM_TOKEN"] = ssm.get_parameter(
                Name="/justhodl/telegram/token", WithDecryption=True
            )["Parameter"]["Value"]
            env_vars["TELEGRAM_CHAT_ID"] = ssm.get_parameter(
                Name="/justhodl/telegram/chat_id", WithDecryption=True
            )["Parameter"]["Value"]
            out["tg_creds_loaded"] = True
        except Exception as e:
            out["tg_creds_err"] = str(e)[:120]

        # Create or update
        try:
            lam.get_function(FunctionName=NAME)
            lam.update_function_code(FunctionName=NAME, ZipFile=zip_bytes)
            lam.get_waiter("function_updated").wait(FunctionName=NAME)
            lam.update_function_configuration(
                FunctionName=NAME, Runtime="python3.12",
                Handler="lambda_function.lambda_handler",
                MemorySize=256, Timeout=60, Role=ROLE,
                Environment={"Variables": env_vars},
            )
            lam.get_waiter("function_updated").wait(FunctionName=NAME)
            out["reversal_action"] = "update"
        except lam.exceptions.ResourceNotFoundException:
            lam.create_function(
                FunctionName=NAME, Runtime="python3.12",
                Handler="lambda_function.lambda_handler", Role=ROLE,
                MemorySize=256, Timeout=60, Architectures=["arm64"],
                Code={"ZipFile": zip_bytes},
                Environment={"Variables": env_vars},
                Description="Reversal Radar — composite top/bottom probability detector",
            )
            lam.get_waiter("function_active").wait(FunctionName=NAME)
            out["reversal_action"] = "create"

        # EB rule
        try:
            events.put_rule(Name=RULE_NAME, ScheduleExpression=SCHEDULE, State="ENABLED",
                             Description="Reversal Radar hourly")
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
            out["reversal_eventbridge"] = "OK"
        except Exception as e:
            out["reversal_eb_err"] = str(e)[:200]

        # Smoke
        _time.sleep(3)
        resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                           LogType="Tail", Payload=b"{}")
        out["reversal_invoke_status"] = resp.get("StatusCode")
        out["reversal_fn_error"] = resp.get("FunctionError")
        try:
            body = resp["Payload"].read().decode("utf-8")
            p = json.loads(body)
            out["reversal_response"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except Exception as e: out["reversal_raw"] = body[:500] if 'body' in locals() else str(e)
        if resp.get("LogResult"):
            log = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")
            out["reversal_log_tail"] = log[-2000:]
    except Exception as e:
        import traceback
        out["reversal_fatal"] = str(e)[:200]
        out["reversal_tb"] = traceback.format_exc()[:1500]

    # ─── Political Trades: patch TG env ─────────────────────────────────
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-political-trades")
        env = (cfg.get("Environment") or {}).get("Variables", {}) or {}
        had_token = bool(env.get("TELEGRAM_TOKEN"))
        had_chat = bool(env.get("TELEGRAM_CHAT_ID"))
        patched = False
        if not had_token:
            try:
                env["TELEGRAM_TOKEN"] = ssm.get_parameter(
                    Name="/justhodl/telegram/token", WithDecryption=True
                )["Parameter"]["Value"]
                patched = True
            except Exception: pass
        if not had_chat:
            try:
                env["TELEGRAM_CHAT_ID"] = ssm.get_parameter(
                    Name="/justhodl/telegram/chat_id", WithDecryption=True
                )["Parameter"]["Value"]
                patched = True
            except Exception: pass
        if patched:
            lam.update_function_configuration(
                FunctionName="justhodl-political-trades",
                Environment={"Variables": env})
            lam.get_waiter("function_updated").wait(FunctionName="justhodl-political-trades")
            out["political_env_patched"] = "OK"
        else:
            out["political_env_already_set"] = True
    except Exception as e:
        out["political_env_err"] = str(e)[:200]

    # Read reversal sidecar
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/reversal-radar.json")
        out["reversal_sidecar"] = json.loads(obj["Body"].read())
    except Exception as e:
        out["reversal_sidecar_err"] = str(e)[:120]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
