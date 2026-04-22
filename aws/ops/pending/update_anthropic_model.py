#!/usr/bin/env python3
"""
Update deprecated Anthropic model string across all JustHodl Lambdas.

Anthropic deprecated `claude-3-haiku-20240307`. Current Haiku is
`claude-haiku-4-5-20251001`. Every Lambda standardized on the old string
during the March 28 fix now returns 404 from the Messages API.

Lambdas updated:
  - justhodl-ai-chat
  - justhodl-telegram-bot
  - justhodl-morning-intelligence
  - justhodl-investor-agents

Idempotent. Safe to re-run. Skips functions that are already updated.
Preserves everything else in the zip (other files, nested dirs, binaries).
"""

import io
import json
import sys
import time
import urllib.error
import urllib.request
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION    = "us-east-1"
OLD_MODEL = "claude-3-haiku-20240307"
NEW_MODEL = "claude-haiku-4-5-20251001"
# Also replace any stale older references that might have survived
LEGACY_MODELS = [
    "claude-3-5-sonnet-20241022",
    "claude-3-opus-20240229",
    "claude-sonnet-4-20250514",
    "claude-opus-4-5",
    "claude-3-sonnet-20240229",
]

LAMBDAS = [
    "justhodl-ai-chat",
    "justhodl-telegram-bot",
    "justhodl-morning-intelligence",
    "justhodl-investor-agents",
]

# Post-deploy smoke test
AI_CHAT_URL = "https://zh3c6izcbzcqwcia4m6dmjnupy0dbnns.lambda-url.us-east-1.on.aws/"
SSM_TOKEN_PARAM = "/justhodl/ai-chat/auth-token"

lam = boto3.client("lambda", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def log(msg):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)


def update_one(fn_name: str) -> str:
    """
    Returns one of: 'updated', 'already', 'not-found', 'no-py'.
    """
    log(f"──── {fn_name} ────")
    try:
        fn = lam.get_function(FunctionName=fn_name)
    except lam.exceptions.ResourceNotFoundException:
        log(f"  not found in region {REGION} — skip")
        return "not-found"

    url = fn["Code"]["Location"]
    with urllib.request.urlopen(url) as r:
        zbytes = r.read()

    # Find the handler .py file
    with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
        py_files = [n for n in zf.namelist() if n.endswith(".py")]
        if "lambda_function.py" in py_files:
            target = "lambda_function.py"
        elif py_files:
            target = py_files[0]
        else:
            log(f"  no .py file in zip — skip")
            return "no-py"

        src = zf.read(target).decode("utf-8", errors="ignore")

    if OLD_MODEL not in src and not any(m in src for m in LEGACY_MODELS):
        if NEW_MODEL in src:
            log(f"  already on {NEW_MODEL}")
            return "already"
        log(f"  no known model strings found in {target} — inspect manually")
        return "no-py"

    # Count replacements for logging
    total = 0
    for old in [OLD_MODEL] + LEGACY_MODELS:
        n = src.count(old)
        if n:
            log(f"  {target}: {n}× {old} → {NEW_MODEL}")
            src = src.replace(old, NEW_MODEL)
            total += n

    if total == 0:
        return "already"

    # Repackage preserving all other zip entries
    out = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(zbytes)) as zin, \
         zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zout:
        for name in zin.namelist():
            zout.writestr(name, src if name == target else zin.read(name))

    log(f"  uploading patched zip ({len(out.getvalue())} bytes)")
    lam.update_function_code(FunctionName=fn_name, ZipFile=out.getvalue())
    lam.get_waiter("function_updated").wait(
        FunctionName=fn_name, WaiterConfig={"Delay": 3, "MaxAttempts": 20}
    )
    log(f"  ✅ {fn_name} updated")
    return "updated"


def smoke_test():
    log("Smoke test against justhodl-ai-chat")
    try:
        token = ssm.get_parameter(Name=SSM_TOKEN_PARAM, WithDecryption=True)["Parameter"]["Value"]
    except ClientError as e:
        log(f"  SSM token fetch failed: {e} — skipping smoke test")
        return

    req = urllib.request.Request(
        AI_CHAT_URL,
        data=json.dumps({"message": "price AAPL in one short line"}).encode(),
        headers={
            "Content-Type": "application/json",
            "Origin": "https://justhodl.ai",
            "x-justhodl-token": token,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            status = r.status
            body = r.read().decode(errors="ignore")
    except urllib.error.HTTPError as e:
        status = e.code
        body = e.read().decode(errors="ignore")

    if status != 200:
        sys.exit(f"  ✗ smoke test FAILED ({status}): {body[:300]}")

    try:
        preview = json.loads(body).get("response", body)[:160]
    except (json.JSONDecodeError, TypeError):
        preview = body[:160]
    log(f"  ✅ smoke test PASS: {preview}")


def main():
    log("=== Update Anthropic model across JustHodl Lambdas ===")
    log(f"    {OLD_MODEL} → {NEW_MODEL}")

    results = {}
    for fn in LAMBDAS:
        try:
            results[fn] = update_one(fn)
        except Exception as e:
            results[fn] = f"error: {e}"
            log(f"  ✗ exception: {e}")

    log("")
    log("Summary:")
    for fn, status in results.items():
        log(f"  {fn}: {status}")

    # Only smoke-test if ai-chat was actually updated or was already fine
    if results.get("justhodl-ai-chat") in ("updated", "already"):
        log("")
        time.sleep(3)  # settle
        smoke_test()

    errored = [f for f, s in results.items() if isinstance(s, str) and s.startswith("error")]
    if errored:
        sys.exit(f"One or more updates failed: {errored}")

    log("=== ✅ Done ===")


if __name__ == "__main__":
    main()
