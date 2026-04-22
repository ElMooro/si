#!/usr/bin/env python3
"""
Diagnose why justhodl-chat-api returns HTTP 400 even after model fix.

The lambda swallows the Anthropic error body. This script:
  1. Patches chat-api to catch urllib.error.HTTPError specifically and
     include the response body in the return payload
  2. Deploys the patched version
  3. Re-invokes with a minimal message
  4. Reports the actual Anthropic error — usually tells us exactly what's wrong
"""

import io
import json
import os
import re
import zipfile
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

lam = boto3.client("lambda", region_name=REGION)


def build_zip(src_dir: Path) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for f in src_dir.rglob("*"):
            if f.is_file():
                zout.write(f, str(f.relative_to(src_dir)))
    return buf.getvalue()


with report("diagnose_chat_api") as r:
    r.heading("Diagnose chat-api HTTP 400")

    path = REPO_ROOT / "aws/lambdas/justhodl-chat-api/source/lambda_function.py"

    r.section("Step 1: patch except to include HTTPError body")
    text = path.read_text(encoding="utf-8")
    original = text

    # Improve the except block to include the response body on HTTPError
    # Original: except Exception as e:
    #             return{'statusCode':500,'headers':headers,'body':json.dumps({'error':str(e)})}
    new_except = (
        "    except urllib.error.HTTPError as e:\n"
        "        detail = ''\n"
        "        try: detail = e.read().decode('utf-8', errors='ignore')[:600]\n"
        "        except Exception: pass\n"
        "        return {'statusCode':500,'headers':headers,'body':json.dumps({'error':f'HTTP {e.code}: {e.reason}', 'anthropic_body': detail, 'key_prefix': ANTHROPIC_KEY[:12]})}\n"
        "    except Exception as e:\n"
        "        return {'statusCode':500,'headers':headers,'body':json.dumps({'error':str(e)})}"
    )
    text = re.sub(
        r"    except Exception as e:\n        return\{'statusCode':500,'headers':headers,'body':json\.dumps\(\{'error':str\(e\)\}\)\}",
        new_except,
        text,
        count=1,
    )
    # Also need to import urllib.error since we reference it
    if "import urllib.error" not in text:
        text = text.replace("import urllib.request", "import urllib.request, urllib.error", 1)

    if text != original:
        path.write_text(text, encoding="utf-8")
        r.ok("  Patched — HTTPError now returns Anthropic body")
    else:
        r.warn("  Patch didn't match — pattern miss")

    r.section("Step 2: deploy patched chat-api")
    src_dir = REPO_ROOT / "aws/lambdas/justhodl-chat-api/source"
    zbytes = build_zip(src_dir)
    lam.update_function_code(FunctionName="justhodl-chat-api", ZipFile=zbytes)
    lam.get_waiter("function_updated").wait(
        FunctionName="justhodl-chat-api", WaiterConfig={"Delay": 3, "MaxAttempts": 20}
    )
    r.ok(f"  Deployed ({len(zbytes)} bytes)")

    r.section("Step 3: re-invoke with minimal message")
    resp = lam.invoke(
        FunctionName="justhodl-chat-api",
        InvocationType="RequestResponse",
        Payload=json.dumps({
            "httpMethod": "POST",
            "body": json.dumps({"messages": [{"role": "user", "content": "hello"}]}),
        }).encode(),
    )
    body = resp["Payload"].read().decode("utf-8", errors="ignore")
    r.log(f"  Outer response:")
    r.log(f"    {body}")

    # Parse nested body for the real error
    try:
        outer = json.loads(body)
        if "body" in outer:
            inner = json.loads(outer["body"])
            r.log("")
            r.log(f"  Inner body parsed:")
            for k, v in inner.items():
                r.log(f"    {k}: {str(v)[:400]}")
            if "anthropic_body" in inner:
                r.log("")
                r.log(f"  ANTHROPIC ERROR: {inner['anthropic_body']}")
                r.kv(status="got_anthropic_error", detail=inner["anthropic_body"][:200])
            else:
                r.kv(status="unknown", body=body[:200])
    except Exception as e:
        r.warn(f"  Couldn't parse response: {e}")

    r.log("Done")
