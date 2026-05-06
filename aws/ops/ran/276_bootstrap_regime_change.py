#!/usr/bin/env python3
"""Step 276 — Bootstrap regime-change detection on macro-nowcast.

  1. Confirm/add IAM perm: lambda-execution-role can PutParameter
     on /justhodl/nowcast/* (and read /justhodl/telegram/*)
  2. Deploy refreshed Lambda code (regime-change logic)
  3. SSM /justhodl/telegram/bot_token must exist as SecureString —
     create from env var if missing (so Lambda can read it without
     the token being in plain Lambda env)
  4. First sync invoke (no SSM state yet → no change detected,
     just persists the state)
  5. Simulate a regime change: write a fake old state to SSM,
     then sync invoke again. Telegram should fire.
  6. Restore real state for normal operation.
"""
import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
LAMBDA_NAME = "justhodl-macro-nowcast"
SOURCE_DIR = f"aws/lambdas/{LAMBDA_NAME}/source"
SSM_STATE = "/justhodl/nowcast/last_state"
SSM_TELEGRAM_TOKEN = "/justhodl/telegram/bot_token"
ROLE_NAME = "lambda-execution-role"
POLICY_NAME = "ssm-justhodl-nowcast-state"
TELEGRAM_TOKEN = "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"
REPORT_PATH = "aws/ops/reports/276_nowcast_regime_change.json"

lam = boto3.client("lambda", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
iam = boto3.client("iam", region_name=REGION)


def ensure_iam_policy():
    """Attach inline policy granting GetParameter + PutParameter on
    /justhodl/nowcast/* and read on /justhodl/telegram/*."""
    policy_doc = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["ssm:GetParameter", "ssm:PutParameter"],
                "Resource": [
                    f"arn:aws:ssm:{REGION}:{ACCOUNT_ID}:parameter/justhodl/nowcast/*",
                ],
            },
            {
                "Effect": "Allow",
                "Action": ["ssm:GetParameter"],
                "Resource": [
                    f"arn:aws:ssm:{REGION}:{ACCOUNT_ID}:parameter/justhodl/telegram/*",
                ],
            },
        ],
    }
    try:
        iam.get_role_policy(RoleName=ROLE_NAME, PolicyName=POLICY_NAME)
        action = "already_attached"
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntity":
            return {"err": str(e)[:200]}
        action = "creating"
    iam.put_role_policy(
        RoleName=ROLE_NAME, PolicyName=POLICY_NAME,
        PolicyDocument=json.dumps(policy_doc),
    )
    return {"policy_name": POLICY_NAME, "action": action}


def ensure_telegram_token_in_ssm():
    """Make sure /justhodl/telegram/bot_token exists as SecureString."""
    try:
        ssm.get_parameter(Name=SSM_TELEGRAM_TOKEN, WithDecryption=True)
        return {"status": "already_exists"}
    except ClientError as e:
        if e.response["Error"]["Code"] != "ParameterNotFound":
            return {"err": str(e)[:200]}
    ssm.put_parameter(
        Name=SSM_TELEGRAM_TOKEN, Value=TELEGRAM_TOKEN,
        Type="SecureString", Overwrite=False,
        Description="Telegram bot token for @Justhodl_bot — used by macro-nowcast for regime change alerts",
    )
    return {"status": "created"}


def deploy_lambda():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _d, files in os.walk(SOURCE_DIR):
            for fn in files:
                if fn.endswith(".pyc") or "__pycache__" in root:
                    continue
                fp = os.path.join(root, fn)
                zf.write(fp, os.path.relpath(fp, SOURCE_DIR))
    zip_bytes = buf.getvalue()
    lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zip_bytes)
    lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_NAME)
    return {"zip_size": len(zip_bytes)}


def sync_invoke(label=""):
    print(f"[276] sync invoke {label}…")
    started = time.time()
    inv = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse",
                     Payload=b"{}")
    elapsed = round(time.time() - started, 2)
    payload = json.loads(inv["Payload"].read())
    body = payload.get("body")
    try:
        body_parsed = json.loads(body) if isinstance(body, str) else body
    except Exception:
        body_parsed = body
    return {
        "status": inv.get("StatusCode"),
        "func_err": inv.get("FunctionError"),
        "elapsed_s": elapsed,
        "body": body_parsed,
    }


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # Step 1: IAM
        out["iam"] = ensure_iam_policy()
        time.sleep(2)

        # Step 2: Telegram token in SSM
        out["telegram_token_ssm"] = ensure_telegram_token_in_ssm()

        # Step 3: Deploy refreshed code
        out["deploy"] = deploy_lambda()

        # Step 4: First invoke — establishes baseline state
        out["invoke_1_baseline"] = sync_invoke("baseline (no SSM state yet)")

        # Step 5: Simulate regime change — write a FAKE old state to SSM
        # then invoke again. The Lambda should detect the difference
        # and fire Telegram.
        fake_old = {
            "regime": "EXPANSION",            # different from current SLOWING
            "score": 0.500,
            "ts": "2026-04-01T00:00:00+00:00",
        }
        ssm.put_parameter(
            Name=SSM_STATE, Value=json.dumps(fake_old),
            Type="String", Overwrite=True,
        )
        out["fake_old_state_written"] = fake_old

        # Step 6: Second invoke — should detect change + fire Telegram
        out["invoke_2_simulate_change"] = sync_invoke("simulating regime change")

        # Step 7: Confirm SSM was updated to current state
        try:
            cur = json.loads(ssm.get_parameter(Name=SSM_STATE)["Parameter"]["Value"])
            out["ssm_state_after"] = cur
        except Exception as e:
            out["ssm_state_err"] = str(e)[:200]

    except Exception as e:
        import traceback
        out["fatal_error"] = str(e)
        out["traceback"] = traceback.format_exc()

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:5000])
    return 0 if "fatal_error" not in out else 1


if __name__ == "__main__":
    raise SystemExit(main())
