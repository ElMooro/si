#!/usr/bin/env python3
"""
Fix 3 Lambdas broken after the data.json → data/report.json migration.

Problem 1 (2 Lambdas): khalid_index changed shape
  OLD:  "khalid_index": 49                                 ← number
  NEW:  "khalid_index": {"score": 48, "regime": "NEUTRAL", "signals": [...], "ts": ...}

  Affected:
    - justhodl-morning-intelligence: float(ki) crashes on dict
    - justhodl-signal-logger: float(ki) crashes on dict

  Fix: extract .get("score") when the value is a dict.
  Also: top-level "regime" no longer exists — read it from
        khalid_index.regime instead.

Problem 2 (1 Lambda): retired Claude model
  justhodl-chat-api hardcodes claude-sonnet-4-20250514 which Anthropic retired.
  Fix: bump to claude-haiku-4-5-20251001 (same as ai-chat, morning-intel,
       investor-agents already use).

This script edits the repo source files, then deploys directly to AWS
(same force-deploy pattern as Phase 3b) — no workflow race to worry about.
"""

import io
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


def deploy(fn_name: str):
    src_dir = REPO_ROOT / "aws" / "lambdas" / fn_name / "source"
    zbytes = build_zip(src_dir)
    lam.update_function_code(FunctionName=fn_name, ZipFile=zbytes)
    lam.get_waiter("function_updated").wait(
        FunctionName=fn_name, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
    )
    return len(zbytes)


def edit_file(path: Path, substitutions: list) -> int:
    """Each substitution is (description, pattern, replacement). Returns count applied."""
    text = path.read_text(encoding="utf-8", errors="ignore")
    total = 0
    for desc, pattern, replacement in substitutions:
        new_text, n = re.subn(pattern, replacement, text)
        if n > 0:
            total += n
            text = new_text
    if total > 0:
        path.write_text(text, encoding="utf-8")
    return total


with report("fix_shape_and_model") as r:
    r.heading("Fix 3 broken Lambdas: khalid_index shape + retired model")

    # ─────────────────────────────────────────────────────────
    # Fix 1: justhodl-morning-intelligence
    # ─────────────────────────────────────────────────────────
    r.section("justhodl-morning-intelligence")
    path = REPO_ROOT / "aws/lambdas/justhodl-morning-intelligence/source/lambda_function.py"
    subs = [
        (
            "Extract .score when khalid_index is a dict",
            # Original: "khalid_adj":round(float(ki)*kw,1) if ki else 0,
            r'"khalid_adj":round\(float\(ki\)\*kw,1\) if ki else 0,',
            r'"khalid_adj":round(float(ki["score"] if isinstance(ki, dict) else ki)*kw,1) if ki else 0,',
        ),
        (
            "Read regime from khalid_index.regime (falls back gracefully)",
            # Original: "khalid_regime":d.get("regime") or regime_d.get("khalid","UNKNOWN"),
            r'"khalid_regime":d\.get\("regime"\) or regime_d\.get\("khalid","UNKNOWN"\),',
            r'"khalid_regime":(ki.get("regime") if isinstance(ki, dict) else None) or d.get("regime") or regime_d.get("khalid","UNKNOWN"),',
        ),
    ]
    n = edit_file(path, subs)
    if n > 0:
        r.ok(f"  Applied {n} edit(s)")
        size = deploy("justhodl-morning-intelligence")
        r.ok(f"  Deployed ({size // 1024} KB)")
        r.kv(lambda_name="morning-intelligence", edits=n, status="deployed")
    else:
        r.warn(f"  No edits applied — patterns didn't match")
        r.kv(lambda_name="morning-intelligence", edits=0, status="no-match")

    # ─────────────────────────────────────────────────────────
    # Fix 2: justhodl-signal-logger
    # ─────────────────────────────────────────────────────────
    r.section("justhodl-signal-logger")
    path = REPO_ROOT / "aws/lambdas/justhodl-signal-logger/source/lambda_function.py"
    subs = [
        (
            "Handle dict shape for khalid_index",
            # Original:
            #     ki=d.get("khalid_index")
            #     if ki is not None:
            #         ki=float(ki)
            r'ki=d\.get\("khalid_index"\)\n    if ki is not None:\n        ki=float\(ki\)',
            'ki=d.get("khalid_index")\n    if ki is not None:\n        if isinstance(ki, dict): ki=float(ki.get("score", 0))\n        else: ki=float(ki)',
        ),
        (
            "Fall back to khalid_index.regime if top-level regime absent",
            # Original: regime=d.get("regime","")
            r'regime=d\.get\("regime",""\)',
            r'regime=d.get("regime","") or (d.get("khalid_index",{}).get("regime","") if isinstance(d.get("khalid_index"), dict) else "")',
        ),
    ]
    n = edit_file(path, subs)
    if n > 0:
        r.ok(f"  Applied {n} edit(s)")
        size = deploy("justhodl-signal-logger")
        r.ok(f"  Deployed ({size // 1024} KB)")
        r.kv(lambda_name="signal-logger", edits=n, status="deployed")
    else:
        r.warn(f"  No edits applied — patterns didn't match")
        r.kv(lambda_name="signal-logger", edits=0, status="no-match")

    # ─────────────────────────────────────────────────────────
    # Fix 3: justhodl-chat-api (retired model)
    # ─────────────────────────────────────────────────────────
    r.section("justhodl-chat-api")
    path = REPO_ROOT / "aws/lambdas/justhodl-chat-api/source/lambda_function.py"
    subs = [
        (
            "Replace retired claude-sonnet-4-20250514 with claude-haiku-4-5-20251001",
            r'"model"\s*:\s*"claude-sonnet-4-20250514"',
            '"model": "claude-haiku-4-5-20251001"',
        ),
    ]
    n = edit_file(path, subs)
    if n > 0:
        r.ok(f"  Applied {n} edit(s)")
        size = deploy("justhodl-chat-api")
        r.ok(f"  Deployed ({size // 1024} KB)")
        r.kv(lambda_name="chat-api", edits=n, status="deployed")
    else:
        r.warn(f"  No edits applied — patterns didn't match")
        r.kv(lambda_name="chat-api", edits=0, status="no-match")

    # ─────────────────────────────────────────────────────────
    # Re-test each fixed Lambda
    # ─────────────────────────────────────────────────────────
    r.section("Re-invoking each fixed Lambda to confirm green")

    import json, base64

    test_payloads = {
        "justhodl-morning-intelligence": {},
        "justhodl-signal-logger":         {},
        "justhodl-chat-api": {
            "httpMethod": "POST",
            "body": json.dumps({"messages": [{"role": "user", "content": "hi"}]}),
        },
    }

    for fn_name, payload in test_payloads.items():
        try:
            resp = lam.invoke(
                FunctionName=fn_name,
                InvocationType="RequestResponse",
                LogType="Tail",
                Payload=json.dumps(payload).encode(),
            )
            fn_err = resp.get("FunctionError")
            body = resp["Payload"].read().decode("utf-8", errors="ignore")

            if fn_err:
                r.fail(f"  {fn_name}: FunctionError={fn_err}")
                r.log(f"    {body[:200]}")
                tail = base64.b64decode(resp.get("LogResult", "")).decode() if resp.get("LogResult") else ""
                for line in tail.splitlines()[-8:]:
                    r.log(f"    {line[:200]}")
                r.kv(retest=fn_name, verdict="STILL_BROKEN")
            else:
                # Check for nested statusCode>=400
                bad = False
                try:
                    d = json.loads(body)
                    if isinstance(d, dict) and d.get("statusCode", 200) >= 400:
                        bad = True
                except Exception:
                    pass
                if bad:
                    r.warn(f"  {fn_name}: 200 wrapper but inner status>=400")
                    r.log(f"    {body[:250]}")
                    r.kv(retest=fn_name, verdict="INNER_ERROR")
                else:
                    r.ok(f"  {fn_name}: green ({len(body)} bytes)")
                    r.log(f"    preview: {body[:180]}")
                    r.kv(retest=fn_name, verdict="OK", size=len(body))
        except Exception as e:
            r.fail(f"  {fn_name}: {type(e).__name__}: {e}")
            r.kv(retest=fn_name, verdict="INVOKE_ERROR")

    r.log("Done")
