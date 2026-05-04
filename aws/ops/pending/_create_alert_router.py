"""Create justhodl-alert-router + 30min schedule + smoke test.

Pulls Telegram bot token from existing justhodl-telegram-bot Lambda env vars
to avoid hardcoding credentials.
"""
import io
import json
import os
import time
import zipfile
import boto3
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-alert-router"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
SOURCE_DIR = "aws/lambdas/justhodl-alert-router/source"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def make_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(SOURCE_DIR):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root:
                    continue
                full = os.path.join(root, f)
                rel = os.path.relpath(full, SOURCE_DIR)
                zf.write(full, rel)
    buf.seek(0)
    return buf.read()


def fetch_telegram_token():
    """Reuse existing token from justhodl-telegram-bot Lambda."""
    candidates = ["justhodl-telegram-bot", "justhodl-morning-brief-tg"]
    for c in candidates:
        try:
            cfg = lam.get_function_configuration(FunctionName=c)
            env = (cfg.get("Environment") or {}).get("Variables") or {}
            for k in ["TELEGRAM_BOT_TOKEN", "TELEGRAM_TOKEN", "BOT_TOKEN"]:
                if k in env and env[k]:
                    return env[k], c, k
        except Exception:
            continue
    return None, None, None


def main():
    with report("create_alert_router") as r:
        r.heading("Create justhodl-alert-router + 30min schedule")

        zb = make_zip()
        r.log(f"  zip size: {len(zb):,}b")

        token, source_lambda, source_key = fetch_telegram_token()
        if token:
            r.ok(f"  ✓ telegram token sourced from {source_lambda}.{source_key}  (len={len(token)})")
        else:
            r.log("  ⚠ no token found in known Lambdas — alerts will be skipped (lambda still deploys)")

        env_vars = {}
        if token:
            env_vars["TELEGRAM_BOT_TOKEN"] = token

        try:
            lam.get_function(FunctionName=LAMBDA_NAME)
            lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zb)
            time.sleep(3)
            lam.update_function_configuration(
                FunctionName=LAMBDA_NAME,
                Runtime="python3.12",
                Handler="lambda_function.lambda_handler",
                Timeout=120,
                MemorySize=256,
                Role=ROLE_ARN,
                Environment={"Variables": env_vars},
            )
            r.ok("  ✓ updated existing")
        except lam.exceptions.ResourceNotFoundException:
            lam.create_function(
                FunctionName=LAMBDA_NAME,
                Runtime="python3.12",
                Role=ROLE_ARN,
                Handler="lambda_function.lambda_handler",
                Code={"ZipFile": zb},
                Timeout=120,
                MemorySize=256,
                Architectures=["x86_64"],
                Environment={"Variables": env_vars},
            )
            r.ok("  ✓ created")

        for _ in range(20):
            cfg = lam.get_function(FunctionName=LAMBDA_NAME)["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                break
            time.sleep(2)

        # Schedule: rate(30 minutes)
        r.heading("EventBridge schedule (every 30 minutes)")
        rule_name = f"{LAMBDA_NAME}-30min"
        events.put_rule(
            Name=rule_name,
            ScheduleExpression="rate(30 minutes)",
            State="ENABLED",
            Description="Real-time threshold scanner — sends Telegram on extreme readings",
        )
        fn_arn = lam.get_function(FunctionName=LAMBDA_NAME)["Configuration"]["FunctionArn"]
        events.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": fn_arn}])
        try:
            lam.add_permission(
                FunctionName=LAMBDA_NAME,
                StatementId=f"{rule_name}-invoke",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:857687956942:rule/{rule_name}",
            )
        except lam.exceptions.ResourceConflictException:
            pass
        r.ok("  ✓ wired")

        # Smoke test
        r.heading("Smoke test — first run")
        t0 = time.time()
        resp = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}  duration: {time.time()-t0:.1f}s")
        r.log(f"  resp: {body[:400]}")

        # Verify history written
        r.heading("S3 verify")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/alert-history.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  alert-history.json: {len(d.get('alerts', []))} alerts in history")
            r.log(f"  last_run: {d.get('last_run')}")
            r.log(f"  last_run_summary: {d.get('last_run_summary')}")
            for a in d.get("alerts", [])[-5:]:
                r.log(f"    {a.get('severity'):6s} [{a.get('category'):14s}] {a.get('title','')[:80]}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
