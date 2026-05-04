# rerun-marker: 1777898799
"""
Create justhodl-morning-brief-tg Lambda + daily 12 UTC schedule + smoke test.
Sources Telegram token from existing justhodl-telegram-bot env.
"""
import os
import time
import zipfile
import io
import json
import boto3
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-morning-brief-tg"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
SOURCE_DIR = "aws/lambdas/justhodl-morning-brief-tg/source"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def get_telegram_token():
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-telegram-bot")
        env = cfg.get("Environment", {}).get("Variables", {})
        for k in ("TELEGRAM_TOKEN", "TG_TOKEN", "BOT_TOKEN"):
            if env.get(k):
                return env[k]
    except Exception as e:
        print(f"telegram token source fail: {e}")
    # Fallback to known token
    return "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"


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


def main():
    with report("create_morning_brief_tg") as r:
        r.heading("Create justhodl-morning-brief-tg + daily schedule")

        token = get_telegram_token()
        r.log(f"  telegram token sourced: {bool(token)} (len {len(token)})")

        # Verify chat_id is set
        try:
            cid = ssm.get_parameter(Name="/justhodl/telegram/chat_id")["Parameter"]["Value"]
            r.log(f"  ✓ chat_id in SSM: {cid}")
        except Exception as e:
            r.fail(f"  ✗ chat_id missing in SSM: {e}")

        zb = make_zip()
        r.log(f"  zip size: {len(zb):,}b")

        env_vars = {"TELEGRAM_TOKEN": token}

        try:
            lam.get_function(FunctionName=LAMBDA_NAME)
            r.log(f"  function exists — updating")
            lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zb)
            time.sleep(3)
            lam.update_function_configuration(
                FunctionName=LAMBDA_NAME,
                Runtime="python3.12",
                Handler="lambda_function.lambda_handler",
                MemorySize=256,
                Timeout=60,
                Environment={"Variables": env_vars},
            )
            r.ok(f"  ✓ updated")
        except lam.exceptions.ResourceNotFoundException:
            lam.create_function(
                FunctionName=LAMBDA_NAME,
                Runtime="python3.12",
                Handler="lambda_function.lambda_handler",
                Role=ROLE_ARN,
                Code={"ZipFile": zb},
                MemorySize=256,
                Timeout=60,
                Environment={"Variables": env_vars},
                Description="Daily Telegram morning brief digest",
            )
            r.ok(f"  ✓ created")

        r.section("EventBridge schedule (daily 12 UTC ≈ 7AM ET)")
        try:
            rule_name = f"{LAMBDA_NAME}-daily"
            events.put_rule(
                Name=rule_name,
                ScheduleExpression="cron(0 12 * * ? *)",
                State="ENABLED",
                Description=f"Daily 7AM ET trigger for {LAMBDA_NAME}",
            )
            arn = lam.get_function(FunctionName=LAMBDA_NAME)["Configuration"]["FunctionArn"]
            events.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": arn}])
            try:
                lam.add_permission(
                    FunctionName=LAMBDA_NAME,
                    StatementId=f"{rule_name}-perm",
                    Action="lambda:InvokeFunction",
                    Principal="events.amazonaws.com",
                    SourceArn=f"arn:aws:events:{REGION}:857687956942:rule/{rule_name}",
                )
            except lam.exceptions.ResourceConflictException:
                pass
            r.ok(f"  ✓ wired")
        except Exception as e:
            r.fail(f"  ✗ {e}")

        time.sleep(6)

        r.section("Smoke test (will actually send to Khalid)")
        try:
            t0 = time.time()
            inv = lam.invoke(FunctionName=LAMBDA_NAME, Payload=b"{}")
            payload = inv["Payload"].read().decode()
            r.log(f"  status: {inv['StatusCode']} duration: {time.time()-t0:.1f}s")
            r.log(f"  resp: {payload[:500]}")
            if inv.get("FunctionError"):
                r.fail(f"  ✗ {inv['FunctionError']}")
                return
        except Exception as e:
            r.fail(f"  ✗ {e}")
            return

        r.section("S3 mirror verify")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/morning-brief-latest.json")
            data = json.loads(obj["Body"].read())
            r.log(f"  as_of: {data.get('as_of')}")
            r.log(f"  ok: {data.get('ok')}")
            r.log(f"  chat_id: {data.get('chat_id')}")
            text = data.get("text", "")
            r.log(f"  text len: {len(text)} chars")
            r.log(f"  text preview:")
            for line in text.split("\n")[:20]:
                r.log(f"    {line}")
        except Exception as e:
            r.fail(f"  ✗ {e}")


if __name__ == "__main__":
    main()
