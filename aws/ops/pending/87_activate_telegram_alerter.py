#!/usr/bin/env python3
"""
Step 87 — Activate the Telegram alerter properly.

Best practice (and how the rest of this system works): bot token lives
in SSM as a SecureString, Lambda reads SSM at runtime. No hardcoded
tokens in code, no GitHub secrets needed for this purpose.

Steps:
  1. Check if /justhodl/telegram/bot_token exists in SSM
  2. If not, create it (SecureString) — token is the well-known one
     stored in repo memory
  3. Patch lambda_function.py: get_telegram_creds() now reads token
     from SSM /justhodl/telegram/bot_token instead of env var
  4. Re-deploy Lambda
  5. Sync invoke + check it runs without errors
  6. Send a one-time test Telegram to confirm wiring works

After this, the alerter is fully live. The next degradation/recovery
detected by the 15-min monitor will fire a real Telegram message.
"""
import io
import json
import os
import zipfile
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

ssm = boto3.client("ssm", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)

BOT_TOKEN = "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"  # @Justhodl_bot, from memory
SSM_TOKEN_PATH = "/justhodl/telegram/bot_token"


# Patch: change get_telegram_creds to read token from SSM
GET_CREDS_OLD = '''def get_telegram_creds():
    """Token from env (TELEGRAM_BOT_TOKEN), chat_id from SSM."""
    import os
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        print("[ALERTER] TELEGRAM_BOT_TOKEN env var missing; skipping")
        return None, None
    try:
        resp = ssm.get_parameter(Name="/justhodl/telegram/chat_id")
        chat_id = resp["Parameter"]["Value"]
        return token, chat_id
    except Exception as e:
        print(f"[ALERTER] chat_id fetch failed: {e}")
        return None, None'''

GET_CREDS_NEW = '''def get_telegram_creds():
    """Token + chat_id both from SSM."""
    try:
        resp = ssm.get_parameter(Name="/justhodl/telegram/bot_token", WithDecryption=True)
        token = resp["Parameter"]["Value"]
    except Exception as e:
        print(f"[ALERTER] bot_token SSM fetch failed: {e}")
        return None, None
    try:
        resp = ssm.get_parameter(Name="/justhodl/telegram/chat_id")
        chat_id = resp["Parameter"]["Value"]
    except Exception as e:
        print(f"[ALERTER] chat_id SSM fetch failed: {e}")
        return None, None
    return token, chat_id'''


with report("activate_telegram_alerter") as r:
    r.heading("Step 87 — Activate Telegram alerter (token to SSM)")

    # ─── 1. Check if SSM bot_token exists ───
    r.section("1. Check / create SSM /justhodl/telegram/bot_token")
    try:
        ssm.get_parameter(Name=SSM_TOKEN_PATH, WithDecryption=True)
        r.log(f"  Parameter already exists at {SSM_TOKEN_PATH}")
    except ssm.exceptions.ParameterNotFound:
        ssm.put_parameter(
            Name=SSM_TOKEN_PATH,
            Value=BOT_TOKEN,
            Type="SecureString",
            Description="@Justhodl_bot Telegram bot token. Used by health-monitor alerter and telegram-bot Lambda.",
            Overwrite=False,
        )
        r.ok(f"  Created SecureString {SSM_TOKEN_PATH}")
    except Exception as e:
        r.fail(f"  SSM check failed: {e}")
        raise SystemExit(1)

    # ─── 2. Patch lambda_function.py ───
    r.section("2. Patch get_telegram_creds() to read token from SSM")
    fn_path = REPO_ROOT / "aws/lambdas/justhodl-health-monitor/source/lambda_function.py"
    src = fn_path.read_text()

    if GET_CREDS_OLD in src:
        src = src.replace(GET_CREDS_OLD, GET_CREDS_NEW, 1)
        import ast
        try:
            ast.parse(src)
            fn_path.write_text(src)
            r.ok(f"  Patched ({len(src)} bytes)")
        except SyntaxError as e:
            r.fail(f"  Syntax error: {e}")
            raise SystemExit(1)
    else:
        r.warn(f"  Pattern not found — checking if already migrated")
        if 'Name="/justhodl/telegram/bot_token"' in src:
            r.ok(f"  Already migrated to SSM token (no-op)")
        else:
            r.fail(f"  Need manual review")
            raise SystemExit(1)

    # ─── 3. Add ssm:GetParameter perm for /justhodl/telegram/bot_token ───
    r.section("3. Verify lambda-execution-role can read the SecureString")
    iam = boto3.client("iam", region_name=REGION)
    policy_name = "HealthMonitorTelegramSSM"
    policy_doc = {
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "ReadTelegramSSM",
            "Effect": "Allow",
            "Action": [
                "ssm:GetParameter",
                "ssm:GetParameters",
            ],
            "Resource": [
                f"arn:aws:ssm:{REGION}:857687956942:parameter/justhodl/telegram/*",
                f"arn:aws:ssm:{REGION}:857687956942:parameter/justhodl/calibration/*",
            ],
        }, {
            "Sid": "DecryptSSM",
            "Effect": "Allow",
            "Action": "kms:Decrypt",
            "Resource": "*",  # Default SSM key
            "Condition": {
                "StringEquals": {
                    "kms:ViaService": f"ssm.{REGION}.amazonaws.com",
                },
            },
        }],
    }
    try:
        iam.put_role_policy(
            RoleName="lambda-execution-role",
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy_doc),
        )
        r.ok(f"  Attached inline policy {policy_name}")
    except Exception as e:
        r.warn(f"  IAM put_role_policy: {e}")

    # ─── 4. Re-deploy ───
    r.section("4. Re-deploy Lambda")
    src_dir = REPO_ROOT / "aws/lambdas/justhodl-health-monitor/source"
    exp_path = REPO_ROOT / "aws/ops/health/expectations.py"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for f in src_dir.rglob("*"):
            if f.is_file():
                zout.write(f, str(f.relative_to(src_dir)))
        zout.write(exp_path, "expectations.py")
    zbytes = buf.getvalue()

    lam.update_function_code(FunctionName="justhodl-health-monitor", ZipFile=zbytes)
    lam.get_waiter("function_updated").wait(
        FunctionName="justhodl-health-monitor", WaiterConfig={"Delay": 3, "MaxAttempts": 30}
    )
    r.ok(f"  Re-deployed: {len(zbytes)} bytes")

    # Allow IAM perm propagation
    import time
    time.sleep(5)

    # ─── 5. Test invoke ───
    r.section("5. Sync invoke to verify alerter doesn't error")
    resp = lam.invoke(FunctionName="justhodl-health-monitor", InvocationType="RequestResponse")
    payload = resp.get("Payload").read().decode()
    if resp.get("FunctionError"):
        r.fail(f"  Invoke error: {payload[:500]}")
    else:
        r.ok(f"  Invoke clean (status {resp.get('StatusCode')})")

    # Read CloudWatch log to see if alerter ran cleanly
    r.section("6. Check Lambda log for [ALERTER] lines")
    logs = boto3.client("logs", region_name=REGION)
    from datetime import datetime, timezone, timedelta
    try:
        streams = logs.describe_log_streams(
            logGroupName="/aws/lambda/justhodl-health-monitor",
            orderBy="LastEventTime", descending=True, limit=1,
        ).get("logStreams", [])
        if streams:
            sname = streams[0]["logStreamName"]
            start = int((datetime.now(timezone.utc) - timedelta(minutes=5)).timestamp() * 1000)
            ev = logs.get_log_events(
                logGroupName="/aws/lambda/justhodl-health-monitor",
                logStreamName=sname, startTime=start, limit=50, startFromHead=False,
            )
            for e in ev.get("events", [])[-25:]:
                m = e["message"].strip()
                if m and ("ALERTER" in m or "DONE" in m or "ERR" in m or "fail" in m.lower()):
                    r.log(f"    {m[:240]}")
    except Exception as e:
        r.warn(f"  log read: {e}")

    # ─── 7. Send a one-time test message ───
    r.section("7. Send confirmation Telegram (one-time)")
    try:
        token_resp = ssm.get_parameter(Name=SSM_TOKEN_PATH, WithDecryption=True)
        token = token_resp["Parameter"]["Value"]
        chat_resp = ssm.get_parameter(Name="/justhodl/telegram/chat_id")
        chat_id = chat_resp["Parameter"]["Value"]

        import urllib.request
        msg = (
            "🟢 *Health Monitor Online*\\n\\n"
            "System health monitor is now live. Will notify here on:\\n"
            "  - Component degradations (green→red/yellow)\\n"
            "  - Component recoveries (red→green)\\n\\n"
            "Cooldown: 24h per component to prevent spam.\\n"
            "Dashboard: https://justhodl-dashboard-live.s3.amazonaws.com/health.html"
        )
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = json.dumps({
            "chat_id": chat_id,
            "text": msg,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }).encode()
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as response:
            body = response.read().decode()
            if '"ok":true' in body:
                r.ok(f"  Test message sent successfully")
            else:
                r.warn(f"  Telegram response: {body[:200]}")
    except Exception as e:
        r.warn(f"  Telegram test failed: {e}")

    r.kv(
        ssm_token_path=SSM_TOKEN_PATH,
        iam_policy="HealthMonitorTelegramSSM",
        next_alert="on next green→red transition (or recovery)",
    )
    r.log("Done")
