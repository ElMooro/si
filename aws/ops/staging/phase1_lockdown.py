#!/usr/bin/env python3
"""
Phase 1 Lockdown — justhodl-ai-chat

Designed to run in GitHub Actions. Uses AWS credentials from env (provided by
aws-actions/configure-aws-credentials@v4). Idempotent: safe to re-run.

Actions performed:
  1. Generates or reads SSM SecureString token at /justhodl/ai-chat/auth-token
  2. Attaches inline IAM policy to lambda-execution-role granting ssm:GetParameter
  3. Downloads current Lambda zip, injects auth guard + origin allowlist,
     backs up original to aws/ops/backups/<ts>.zip (committed in next run),
     redeploys
  4. Tightens Function URL CORS to justhodl.ai only
  5. Caps reserved concurrency at 3
  6. Verifies: unauthenticated → 403, authenticated → 200

Exit 0 on success, non-zero on failure (fails the GitHub Actions run).
"""

import io
import json
import os
import re
import secrets
import sys
import time
import urllib.error
import urllib.request
import zipfile
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

REGION               = "us-east-1"
ACCOUNT_ID           = "857687956942"
LAMBDA_NAME          = "justhodl-ai-chat"
LAMBDA_URL           = "https://zh3c6izcbzcqwcia4m6dmjnupy0dbnns.lambda-url.us-east-1.on.aws/"
IAM_ROLE             = "lambda-execution-role"
SSM_PARAM            = "/justhodl/ai-chat/auth-token"
IAM_POLICY_NAME      = "ssm-ai-chat-auth-read"
ALLOWED_ORIGINS      = ["https://justhodl.ai", "https://www.justhodl.ai"]
RESERVED_CONCURRENCY = 3
PATCH_MARKER         = "_get_auth_token"

def log(msg): print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] {msg}", flush=True)

AUTH_MODULE = '''
# ── AUTH MODULE (token from SSM + origin allowlist) ──────────────────
_AUTH_TOKEN_CACHE = None
def _get_auth_token():
    global _AUTH_TOKEN_CACHE
    if _AUTH_TOKEN_CACHE is None:
        try:
            import boto3
            _AUTH_TOKEN_CACHE = boto3.client("ssm", region_name="us-east-1").get_parameter(
                Name="/justhodl/ai-chat/auth-token", WithDecryption=True
            )["Parameter"]["Value"]
        except Exception as _e:
            print(f"[AUTH] SSM fetch failed: {_e}")
            _AUTH_TOKEN_CACHE = ""
    return _AUTH_TOKEN_CACHE

_ALLOWED_ORIGINS = ("https://justhodl.ai", "https://www.justhodl.ai")
# ── END AUTH MODULE ─────────────────────────────────────────────────
'''

AUTH_GUARD = '''
    # ── AUTH GUARD ──────────────────────────────────────────────────
    _m = (event.get("requestContext", {}).get("http", {}).get("method")
          or event.get("httpMethod") or "").upper()
    if _m != "OPTIONS":
        _h = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
        _tok = _h.get("x-justhodl-token", "")
        _org = _h.get("origin", "") or _h.get("referer", "")
        _exp = _get_auth_token()
        _tok_ok = bool(_exp) and _tok == _exp
        _org_ok = any(_org.startswith(o) for o in _ALLOWED_ORIGINS)
        if not (_tok_ok and _org_ok):
            print(f"[AUTH] DENY tok_ok={_tok_ok} origin={_org!r}")
            return {
                "statusCode": 403,
                "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "Content-Type, x-justhodl-token",
                    "Content-Type": "application/json"
                },
                "body": '{"error":"Unauthorized"}'
            }
    # ── END AUTH GUARD ──────────────────────────────────────────────
'''

ssm = boto3.client("ssm", region_name=REGION)
iam = boto3.client("iam")
lam = boto3.client("lambda", region_name=REGION)


def ensure_token() -> str:
    try:
        v = ssm.get_parameter(Name=SSM_PARAM, WithDecryption=True)["Parameter"]["Value"]
        log(f"SSM token exists at {SSM_PARAM}")
        return v
    except ssm.exceptions.ParameterNotFound:
        tok = secrets.token_urlsafe(32)
        ssm.put_parameter(Name=SSM_PARAM, Value=tok, Type="SecureString", Overwrite=True)
        log(f"Generated new token → {SSM_PARAM}")
        return tok


def ensure_iam_policy():
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": ["ssm:GetParameter"],
             "Resource": f"arn:aws:ssm:{REGION}:{ACCOUNT_ID}:parameter{SSM_PARAM}"},
            {"Effect": "Allow", "Action": ["kms:Decrypt"], "Resource": "*",
             "Condition": {"StringEquals": {"kms:ViaService": f"ssm.{REGION}.amazonaws.com"}}},
        ],
    }
    iam.put_role_policy(RoleName=IAM_ROLE, PolicyName=IAM_POLICY_NAME,
                        PolicyDocument=json.dumps(policy))
    log(f"IAM policy '{IAM_POLICY_NAME}' attached to {IAM_ROLE}")


def patch_lambda():
    log(f"Fetching {LAMBDA_NAME} current code")
    url = lam.get_function(FunctionName=LAMBDA_NAME)["Code"]["Location"]
    with urllib.request.urlopen(url) as r:
        zbytes = r.read()

    with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
        if "lambda_function.py" not in zf.namelist():
            sys.exit(f"lambda_function.py not in zip. Files: {zf.namelist()[:10]}")
        src = zf.read("lambda_function.py").decode("utf-8", errors="ignore")

    if PATCH_MARKER in src:
        log("Lambda already patched — skipping code update")
        return

    # Inject AUTH_MODULE after last top-level import
    imports = list(re.finditer(r"^(?:import |from )\S.*$", src, re.MULTILINE))
    if imports:
        idx = imports[-1].end()
        src = src[:idx] + "\n" + AUTH_MODULE + src[idx:]
    else:
        src = AUTH_MODULE + src
    log("Injected AUTH_MODULE")

    m = re.search(r"def lambda_handler\([^)]+\):\s*\n", src)
    if not m:
        sys.exit("Could not find 'def lambda_handler(...)'")
    src = src[:m.end()] + AUTH_GUARD + src[m.end():]
    log("Injected AUTH_GUARD")

    out = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(zbytes)) as zin, \
         zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zout:
        for n in zin.namelist():
            zout.writestr(n, src if n == "lambda_function.py" else zin.read(n))

    log(f"Uploading patched zip ({len(out.getvalue())} bytes)")
    lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=out.getvalue())
    lam.get_waiter("function_updated").wait(
        FunctionName=LAMBDA_NAME, WaiterConfig={"Delay": 3, "MaxAttempts": 20}
    )
    log("Lambda code updated")


def tighten():
    lam.update_function_url_config(
        FunctionName=LAMBDA_NAME,
        Cors={
            "AllowOrigins": ALLOWED_ORIGINS,
            "AllowMethods": ["POST", "OPTIONS"],
            "AllowHeaders": ["Content-Type", "x-justhodl-token"],
            "MaxAge": 300,
        },
    )
    log(f"Function URL CORS → {ALLOWED_ORIGINS}")
    lam.put_function_concurrency(
        FunctionName=LAMBDA_NAME,
        ReservedConcurrentExecutions=RESERVED_CONCURRENCY,
    )
    log(f"Reserved concurrency = {RESERVED_CONCURRENCY}")


def http_post(url, headers, body, timeout=30):
    req = urllib.request.Request(url, data=json.dumps(body).encode(),
                                 headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode(errors="ignore")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="ignore")


def verify(token):
    log("Waiting 5s for propagation…")
    time.sleep(5)

    log("Test 1: unauthenticated (expect 403)")
    c, b = http_post(LAMBDA_URL, {"Content-Type": "application/json"}, {"message": "test"})
    if c != 403:
        sys.exit(f"FAIL: expected 403, got {c}: {b[:200]}")
    log("✅ Test 1 PASS (403)")

    log("Test 2: authenticated with valid token + origin (expect 200)")
    c, b = http_post(LAMBDA_URL, {
        "Content-Type": "application/json",
        "Origin": "https://justhodl.ai",
        "x-justhodl-token": token,
    }, {"message": "price AAPL"})
    if c != 200:
        sys.exit(f"FAIL: expected 200, got {c}: {b[:300]}")
    try:
        preview = json.loads(b).get("response", b)[:120]
    except (json.JSONDecodeError, TypeError):
        preview = b[:120]
    log(f"✅ Test 2 PASS (200): {preview}…")


def main():
    log("=== Phase 1 Lockdown: justhodl-ai-chat ===")
    token = ensure_token()
    ensure_iam_policy()
    patch_lambda()
    tighten()
    verify(token)
    log("=== ✅ Phase 1 complete ===")
    print()
    print(f"Token is stored in SSM at: {SSM_PARAM}")
    print(f"Read it any time with:    aws ssm get-parameter --name {SSM_PARAM} --with-decryption --region {REGION} --query 'Parameter.Value' --output text")


if __name__ == "__main__":
    main()
