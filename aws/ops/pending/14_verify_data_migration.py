#!/usr/bin/env python3
"""
Verify Phase 3b migration landed + run end-to-end smoke test.

1. For each of the 7 migrated Lambdas, download its LIVE code from AWS
   and check that it references 'data/report.json' (not the orphan).
2. Run a smoke test against justhodl-ai-chat — ask about the market
   regime. If the response contains '[REGIME]' or '[DATA]', the fix
   didn't take. If it contains actual values, the bug is fixed.
"""

import io
import json
import os
import urllib.request
import urllib.error
import zipfile

from ops_report import report
import boto3

REGION = "us-east-1"
MIGRATED = [
    "justhodl-ai-chat",
    "justhodl-bloomberg-v8",
    "justhodl-chat-api",
    "justhodl-crypto-intel",
    "justhodl-investor-agents",
    "justhodl-morning-intelligence",
    "justhodl-signal-logger",
]

AI_CHAT_URL = "https://zh3c6izcbzcqwcia4m6dmjnupy0dbnns.lambda-url.us-east-1.on.aws/"
SSM_PARAM = "/justhodl/ai-chat/auth-token"

lam = boto3.client("lambda", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def check_live_code(fn_name: str):
    """Returns (still_has_orphan, has_fresh_ref)"""
    cfg = lam.get_function(FunctionName=fn_name)
    code_url = cfg["Code"]["Location"]
    with urllib.request.urlopen(code_url, timeout=20) as resp:
        zbytes = resp.read()

    has_orphan = False
    has_fresh = False
    for entry in zipfile.ZipFile(io.BytesIO(zbytes)).namelist():
        if not entry.endswith(".py"):
            continue
        with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
            src = zf.read(entry).decode("utf-8", errors="ignore")
        if "'data.json'" in src or '"data.json"' in src:
            has_orphan = True
        if "data/report.json" in src:
            has_fresh = True

    last_modified = cfg["Configuration"]["LastModified"]
    return has_orphan, has_fresh, last_modified


with report("verify_data_migration") as r:
    r.heading("Verify Phase 3b data.json migration")

    r.section("Live Lambda code inspection (7 Lambdas)")
    all_clean = True
    for fn_name in MIGRATED:
        try:
            orphan, fresh, lm = check_live_code(fn_name)
            status = "✓ clean" if (fresh and not orphan) else ("✗ still orphan" if orphan else "? unclear")
            if orphan or not fresh:
                all_clean = False
            r.log(f"  {fn_name} | orphan-ref: {orphan} | fresh-ref: {fresh} | deployed: {lm}")
            r.kv(lambda_name=fn_name, orphan_ref=str(orphan), fresh_ref=str(fresh),
                 last_deployed=lm, status=status)
        except Exception as e:
            all_clean = False
            r.fail(f"  {fn_name} check failed: {e}")

    if all_clean:
        r.ok("All 7 Lambdas have fresh code deployed (no orphan data.json refs)")
    else:
        r.warn("Some Lambdas still have orphan refs OR haven't redeployed yet")

    r.section("End-to-end smoke test: ai-chat with regime query")

    try:
        token = ssm.get_parameter(Name=SSM_PARAM, WithDecryption=True)["Parameter"]["Value"]
    except Exception as e:
        r.fail(f"Can't read SSM token: {e}")
        token = None

    if token:
        # Ask a question that will definitely invoke the data.json read path
        req = urllib.request.Request(
            AI_CHAT_URL,
            data=json.dumps({"message": "What is the current market regime and Khalid Index?"}).encode(),
            headers={
                "Content-Type": "application/json",
                "Origin": "https://justhodl.ai",
                "x-justhodl-token": token,
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = resp.read().decode(errors="ignore")
            try:
                data = json.loads(body)
                answer = data.get("response", body[:500])
            except Exception:
                answer = body[:500]

            has_placeholder = any(
                ph in answer for ph in ("[REGIME]", "[DATA]", "[SCORE]", "[KHALID_INDEX]")
            )

            r.log(f"  Query: What is the current market regime and Khalid Index?")
            r.log(f"  Response:")
            for line in answer.splitlines():
                r.log(f"    {line[:200]}")

            if has_placeholder:
                r.fail("  ✗ Response STILL contains placeholders — migration incomplete")
                r.kv(smoke_test="ai-chat", has_placeholder="yes", verdict="FAIL")
            else:
                r.ok("  ✓ No placeholders in response — fresh data is flowing")
                r.kv(smoke_test="ai-chat", has_placeholder="no", verdict="PASS")
        except urllib.error.HTTPError as e:
            r.fail(f"  HTTP {e.code}: {e.read().decode()[:200]}")
        except Exception as e:
            r.fail(f"  Request failed: {type(e).__name__}: {e}")

    r.log("Done")
