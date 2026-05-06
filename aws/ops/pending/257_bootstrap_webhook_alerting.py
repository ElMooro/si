#!/usr/bin/env python3
"""Step 257 — Bootstrap webhook alerting on justhodl-alert-router.

  1. Create SSM SecureString /justhodl/alerts/webhook_urls with default '[]'
     (empty list — no webhooks active until Khalid adds URLs)
  2. Verify lambda-execution-role can read /justhodl/alerts/* (add IAM
     policy if missing). The role already reads /justhodl/telegram/*
     and /justhodl/calibration/* so a permissive add for /justhodl/* is safe.
  3. Synchronously invoke alert-router to verify the new code path
     loads the empty list cleanly without errors.
  4. Persist the run summary to aws/ops/reports/.

Adding a real webhook URL afterwards is a one-liner:
    aws ssm put-parameter --name /justhodl/alerts/webhook_urls \\
      --type SecureString \\
      --value '["https://hooks.slack.com/services/T0/B0/abc"]' \\
      --overwrite
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
LAMBDA_NAME = "justhodl-alert-router"
SSM_PARAM = "/justhodl/alerts/webhook_urls"
ROLE_NAME = "lambda-execution-role"
POLICY_NAME = "ssm-justhodl-alerts-read"
REPORT_PATH = "aws/ops/reports/257_webhook_bootstrap.json"

ssm = boto3.client("ssm", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
iam = boto3.client("iam", region_name=REGION)


def ensure_ssm_param():
    try:
        cur = ssm.get_parameter(Name=SSM_PARAM, WithDecryption=False)["Parameter"]
        return {"created": False, "type": cur["Type"], "version": cur.get("Version"),
                "current_value_length": len(cur.get("Value") or "")}
    except ClientError as e:
        if e.response["Error"]["Code"] != "ParameterNotFound":
            raise
    ssm.put_parameter(
        Name=SSM_PARAM,
        Value="[]",
        Type="SecureString",
        Description=("JSON list of webhook URLs for justhodl-alert-router. Each item "
                     "is either a plain URL string or {url, type, min_severity}. "
                     "Slack/Discord/generic auto-detected by URL. min_severity "
                     "default 'LOW'."),
        Tags=[{"Key": "project", "Value": "justhodl"},
              {"Key": "purpose", "Value": "webhook-alerting"}],
    )
    return {"created": True, "type": "SecureString", "value": "[]"}


def ensure_iam_perm():
    """Attach a minimal inline policy to lambda-execution-role granting
    ssm:GetParameter on /justhodl/alerts/*. Idempotent."""
    policy_doc = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["ssm:GetParameter", "ssm:GetParameters"],
            "Resource": f"arn:aws:ssm:{REGION}:{ACCOUNT_ID}:parameter/justhodl/alerts/*",
        }],
    }
    try:
        existing = iam.get_role_policy(RoleName=ROLE_NAME, PolicyName=POLICY_NAME)
        return {"already_attached": True, "policy_name": POLICY_NAME}
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntity":
            return {"err": str(e), "fallback": "broad ssm read may already cover this"}
    try:
        iam.put_role_policy(
            RoleName=ROLE_NAME,
            PolicyName=POLICY_NAME,
            PolicyDocument=json.dumps(policy_doc),
        )
        return {"created": True, "policy_name": POLICY_NAME}
    except ClientError as e:
        # Some accounts use blanket policies attached at the role level — that's
        # also fine. Don't fail the bootstrap on IAM hiccups.
        return {"err": str(e)[:200],
                "note": "Lambda may still work via existing broad SSM read perms"}


def invoke_test():
    """Synchronously invoke alert-router. Returns the parsed payload."""
    print(f"[257] invoking {LAMBDA_NAME}…")
    started = time.time()
    resp = lam.invoke(
        FunctionName=LAMBDA_NAME, InvocationType="RequestResponse", Payload=b"{}",
    )
    elapsed = round(time.time() - started, 1)
    func_err = resp.get("FunctionError")
    payload_raw = resp["Payload"].read()
    try:
        body = json.loads(payload_raw)
        if "body" in body and isinstance(body["body"], str):
            body["body_parsed"] = json.loads(body["body"])
    except Exception:
        body = {"raw": payload_raw[:300].decode(errors="replace")}
    return {"function_error": func_err, "elapsed_s": elapsed,
            "status_code": resp.get("StatusCode"), "payload": body}


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        out["ssm_param"] = ensure_ssm_param()
        out["iam"] = ensure_iam_perm()
        # Wait for the most recent deploy of alert-router to settle (the push
        # that includes this script also pushes the new alert-router source).
        time.sleep(8)
        out["test_invoke"] = invoke_test()
        out["duration_s"] = round(time.time() - started, 1)
    except Exception as e:
        import traceback
        out["fatal_error"] = str(e)
        out["traceback"] = traceback.format_exc()
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:3000])
    return 0 if "fatal_error" not in out else 1


if __name__ == "__main__":
    raise SystemExit(main())
