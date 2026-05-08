#!/usr/bin/env python3
"""
Step 359 — bootstrap PWA push backend.
  1. Create DynamoDB table justhodl-push-subscriptions (PK=endpoint_hash, on-demand)
  2. Generate VAPID P-256 keypair (pure-Python ECDSA, no external deps)
  3. Write public key to SSM /justhodl/push/vapid-public-key (plain)
  4. Write private key to SSM /justhodl/push/vapid-private-key (SecureString)
  5. Write subject to SSM /justhodl/push/vapid-subject
  6. Generate + write admin token to SSM /justhodl/push/admin-token (SecureString)
  7. Add IAM policy ddb-push-subscriptions to lambda-execution-role
  8. Add IAM policy ssm-push-vapid to lambda-execution-role
Idempotent — safe to re-run; only creates what's missing.
"""
import base64
import json
import os
import secrets
import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REPORT = "aws/ops/reports/359_pwa_setup.json"

# ─── Pure-Python P-256 ECDSA — same algo as the Lambda for consistency ───
P256_P  = 0xffffffff00000001000000000000000000000000ffffffffffffffffffffffff
P256_N  = 0xffffffff00000000ffffffffffffffffbce6faada7179e84f3b9cac2fc632551
P256_A  = P256_P - 3
P256_GX = 0x6b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296
P256_GY = 0x4fe342e2fe1a7f9b8ee7eb4a7c0f9e162bce33576b315ececbb6406837bf51f5

def _ec_add(P1, P2):
    if P1 is None: return P2
    if P2 is None: return P1
    x1, y1 = P1
    x2, y2 = P2
    if x1 == x2:
        if (y1 + y2) % P256_P == 0: return None
        m = (3 * x1 * x1 + P256_A) * pow(2 * y1, -1, P256_P) % P256_P
    else:
        m = (y2 - y1) * pow(x2 - x1, -1, P256_P) % P256_P
    x3 = (m * m - x1 - x2) % P256_P
    y3 = (m * (x1 - x3) - y1) % P256_P
    return (x3, y3)

def _ec_mul(k, P):
    if k == 0 or P is None: return None
    k = k % P256_N
    result = None
    addend = P
    while k:
        if k & 1: result = _ec_add(result, addend)
        addend = _ec_add(addend, addend)
        k >>= 1
    return result

def generate_vapid_keypair():
    """Generate a fresh P-256 keypair. Returns (priv_hex, public_b64url)."""
    priv_int = secrets.randbelow(P256_N - 1) + 1
    point = _ec_mul(priv_int, (P256_GX, P256_GY))
    if point is None:
        raise RuntimeError("Generated invalid private scalar (try again)")
    x, y = point
    pub_bytes = b"\x04" + x.to_bytes(32, "big") + y.to_bytes(32, "big")
    pub_b64 = base64.urlsafe_b64encode(pub_bytes).rstrip(b"=").decode("ascii")
    priv_hex = priv_int.to_bytes(32, "big").hex()
    return priv_hex, pub_b64

# ─── AWS clients ───
ddb = boto3.client("dynamodb", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")
iam = boto3.client("iam")

ROLE_NAME = "lambda-execution-role"
DDB_TABLE = "justhodl-push-subscriptions"
ACCOUNT = "857687956942"

def step_create_ddb(out):
    """Create DDB table if not exists."""
    try:
        ddb.describe_table(TableName=DDB_TABLE)
        out["ddb"] = "already_exists"
        return
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
    print(f"[359] creating DDB table {DDB_TABLE}…")
    ddb.create_table(
        TableName=DDB_TABLE,
        AttributeDefinitions=[{"AttributeName": "endpoint_hash", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "endpoint_hash", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
        Tags=[{"Key": "project", "Value": "justhodl"}, {"Key": "feature", "Value": "pwa-push"}],
    )
    waiter = ddb.get_waiter("table_exists")
    waiter.wait(TableName=DDB_TABLE)
    out["ddb"] = "created"

def step_vapid_keys(out):
    """Generate keypair and write to SSM if not present."""
    try:
        ssm.get_parameter(Name="/justhodl/push/vapid-public-key")
        out["vapid"] = "already_exists"
        return
    except ClientError as e:
        if e.response["Error"]["Code"] != "ParameterNotFound":
            raise
    print("[359] generating fresh P-256 VAPID keypair…")
    priv_hex, pub_b64 = generate_vapid_keypair()
    ssm.put_parameter(Name="/justhodl/push/vapid-public-key", Value=pub_b64,
                      Type="String", Description="VAPID P-256 public key (base64url uncompressed)",
                      Overwrite=True)
    ssm.put_parameter(Name="/justhodl/push/vapid-private-key", Value=priv_hex,
                      Type="SecureString", Description="VAPID P-256 private scalar (32 bytes hex)",
                      Overwrite=True)
    ssm.put_parameter(Name="/justhodl/push/vapid-subject",
                      Value="mailto:contact@justhodl.ai",
                      Type="String", Overwrite=True)
    out["vapid"] = {"public_key_preview": pub_b64[:20] + "...", "public_key_length": len(pub_b64)}

def step_admin_token(out):
    try:
        ssm.get_parameter(Name="/justhodl/push/admin-token", WithDecryption=False)
        out["admin_token"] = "already_exists"
        return
    except ClientError as e:
        if e.response["Error"]["Code"] != "ParameterNotFound":
            raise
    token = secrets.token_hex(32)
    ssm.put_parameter(Name="/justhodl/push/admin-token", Value=token,
                      Type="SecureString", Description="Admin token for /send endpoint",
                      Overwrite=True)
    out["admin_token"] = "created (32-char hex)"

def step_iam_ddb(out):
    """Attach inline policy to Lambda role for DDB access."""
    policy_name = "ddb-push-subscriptions"
    policy_doc = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem",
                       "dynamodb:DeleteItem", "dynamodb:Scan"],
            "Resource": f"arn:aws:dynamodb:us-east-1:{ACCOUNT}:table/{DDB_TABLE}",
        }],
    }
    iam.put_role_policy(RoleName=ROLE_NAME, PolicyName=policy_name,
                        PolicyDocument=json.dumps(policy_doc))
    out["iam_ddb"] = "attached"

def step_iam_ssm(out):
    policy_name = "ssm-push-vapid"
    policy_doc = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["ssm:GetParameter", "ssm:GetParameters"],
            "Resource": [
                f"arn:aws:ssm:us-east-1:{ACCOUNT}:parameter/justhodl/push/*",
            ],
        }],
    }
    iam.put_role_policy(RoleName=ROLE_NAME, PolicyName=policy_name,
                        PolicyDocument=json.dumps(policy_doc))
    out["iam_ssm"] = "attached"

def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "steps": {}}
    try:
        step_create_ddb(out["steps"])
        step_vapid_keys(out["steps"])
        step_admin_token(out["steps"])
        step_iam_ddb(out["steps"])
        step_iam_ssm(out["steps"])
        out["status"] = "success"
    except Exception as e:
        out["status"] = "error"
        out["error"] = f"{type(e).__name__}: {e}"
        print(f"[359] FAIL: {e}")
        raise
    finally:
        out["finished"] = datetime.now(timezone.utc).isoformat()
        os.makedirs(os.path.dirname(REPORT), exist_ok=True)
        with open(REPORT, "w") as f:
            json.dump(out, f, indent=2, default=str)
        print(f"[359] wrote {REPORT}")

if __name__ == "__main__":
    main()
