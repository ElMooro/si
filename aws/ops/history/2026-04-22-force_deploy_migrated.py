#!/usr/bin/env python3
"""
Force-deploy + verify the 7 migrated Lambdas.

The deploy-lambdas workflow missed the migration commit due to a
checkout race. This script deploys directly from the runner, then
verifies the deploys worked and runs an end-to-end smoke test.
"""

import io
import json
import os
import sys
import time
import urllib.request
import urllib.error
import zipfile
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

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


def build_zip(src_dir: Path) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for f in src_dir.rglob("*"):
            if f.is_file():
                zout.write(f, str(f.relative_to(src_dir)))
    return buf.getvalue()


def inspect_live_code(fn_name: str) -> dict:
    cfg = lam.get_function(FunctionName=fn_name)
    code_url = cfg["Code"]["Location"]
    with urllib.request.urlopen(code_url, timeout=20) as resp_:
        zbytes = resp_.read()
    has_orphan = has_fresh = False
    with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
        for entry in zf.namelist():
            if not entry.endswith(".py"):
                continue
            src = zf.read(entry).decode("utf-8", errors="ignore")
            if "'data.json'" in src or '"data.json"' in src:
                has_orphan = True
            if "data/report.json" in src:
                has_fresh = True
    return {
        "last_modified": cfg["Configuration"]["LastModified"],
        "has_orphan": has_orphan,
        "has_fresh": has_fresh,
    }


with report("force_deploy_and_verify") as r:
    r.heading("Force-deploy + verify 7 migrated Lambdas")

    # 1. Deploy
    r.section("Step 1: Deploy (directly from runner)")
    deployed = 0
    failed = 0
    for fn_name in MIGRATED:
        src_dir = REPO_ROOT / "aws" / "lambdas" / fn_name / "source"
        if not src_dir.exists():
            r.fail(f"{fn_name}: source dir missing")
            failed += 1
            continue

        # Double-check repo has fresh ref
        has_fresh = any(
            "data/report.json" in py.read_text(encoding="utf-8", errors="ignore")
            for py in src_dir.rglob("*.py")
        )
        if not has_fresh:
            r.warn(f"{fn_name}: repo doesn't have data/report.json — skipping")
            continue

        try:
            zbytes = build_zip(src_dir)
            lam.update_function_code(FunctionName=fn_name, ZipFile=zbytes)
            lam.get_waiter("function_updated").wait(
                FunctionName=fn_name, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
            )
            r.ok(f"{fn_name}: deployed ({len(zbytes)//1024} KB)")
            deployed += 1
        except Exception as e:
            r.fail(f"{fn_name}: deploy failed: {type(e).__name__}: {e}")
            failed += 1

    r.log(f"Deployed {deployed}/{len(MIGRATED)} ({failed} failed)")

    # 2. Verify each live code now has fresh reference
    r.section("Step 2: Verify live code references data/report.json")
    time.sleep(5)  # propagation
    all_clean = True
    for fn_name in MIGRATED:
        try:
            info = inspect_live_code(fn_name)
            status = "✓ clean" if info["has_fresh"] and not info["has_orphan"] else "✗ stale"
            if not (info["has_fresh"] and not info["has_orphan"]):
                all_clean = False
            r.log(f"  {fn_name} | orphan: {info['has_orphan']} | fresh: {info['has_fresh']} | deployed: {info['last_modified']}")
            r.kv(lambda_name=fn_name, has_fresh=str(info["has_fresh"]),
                 has_orphan=str(info["has_orphan"]),
                 last_deployed=info["last_modified"], status=status)
        except Exception as e:
            r.fail(f"{fn_name}: inspection failed: {e}")
            all_clean = False

    # 3. End-to-end smoke test
    r.section("Step 3: End-to-end smoke test with real query")
    try:
        token = ssm.get_parameter(Name=SSM_PARAM, WithDecryption=True)["Parameter"]["Value"]
        req = urllib.request.Request(
            AI_CHAT_URL,
            data=json.dumps({"message": "What is the current market regime and Khalid Index? Include the exact numeric value and timestamp."}).encode(),
            headers={
                "Content-Type": "application/json",
                "Origin": "https://justhodl.ai",
                "x-justhodl-token": token,
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=45) as resp:
            body = resp.read().decode(errors="ignore")
        data = json.loads(body)
        answer = data.get("response", body[:500])

        # Signals of stale data:
        stale_signals = [
            "N/A",
            "unavailable",
            "Unknown timestamp",
            "[REGIME]", "[DATA]", "[SCORE]",
            "49/100",  # the specific stale khalid_index value
        ]
        found_stale = [s for s in stale_signals if s in answer]

        r.log("  Full response:")
        for line in answer.splitlines():
            r.log(f"    {line[:200]}")

        if found_stale:
            r.warn(f"  Response contains stale-data signals: {found_stale}")
            r.kv(verdict="STILL_STALE", stale_signals=",".join(found_stale))
        else:
            r.ok("  No stale signals — fresh data is flowing")
            r.kv(verdict="FRESH")

    except Exception as e:
        r.fail(f"  Smoke test failed: {type(e).__name__}: {e}")
        r.kv(verdict="ERROR", detail=str(e)[:100])

    r.log("Done")

