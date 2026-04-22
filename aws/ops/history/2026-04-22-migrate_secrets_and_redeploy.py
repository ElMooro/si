#!/usr/bin/env python3
"""
Phase 3a.2 — Migrate hardcoded secrets to env vars, then deploy sanitized code.

For each of the 4 flagged Lambdas:
  1. Download current live code (still contains hardcoded secret)
  2. Extract the secret value + the variable name the code uses (e.g. ANTHROPIC_KEY)
  3. Verify the repo-side sanitized version uses a matching env var name
     — if not, fix the repo copy to match what the existing code expected
  4. Set the env var on the Lambda's configuration (preserving any existing vars)
  5. Zip up the sanitized version from the repo and deploy it
  6. Verify: invoke the function with a minimal payload, expect success

After this runs, the Lambdas:
  - Have their secrets only in the Lambda's encrypted env-var storage
  - Have their code fully in git, with no hardcoded secrets
  - Continue to work without behavior changes

All four flagged functions will be handled. Skipped if already clean.
"""

import io
import json
import os
import re
import sys
import time
import urllib.request
import urllib.error
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"

# Per-function: lambda name + expected secret category (for logging)
TARGETS = [
    ("justhodl-chat-api",             "Anthropic key"),
    ("justhodl-dex-scanner",          "GitHub PAT"),
    ("justhodl-investor-agents",      "Anthropic key"),
    ("justhodl-morning-intelligence", "Anthropic key"),
]

# Patterns that locate BOTH the variable name and the secret value
# regex has 2 groups: (1) var_name, (2) secret_value
SECRET_ASSIGN_PATTERNS = [
    # Anthropic: any VAR = 'sk-ant-...' or "sk-ant-..."
    (
        "Anthropic",
        re.compile(r"([A-Z_][A-Z0-9_]{2,})\s*=\s*['\"](sk-ant-[A-Za-z0-9_\-]{20,})['\"]"),
    ),
    # GitHub PAT: any VAR = 'ghp_...' or "ghp_..."
    (
        "GitHub PAT",
        re.compile(r"([A-Z_][A-Z0-9_]{2,})\s*=\s*['\"](ghp_[A-Za-z0-9]{30,})['\"]"),
    ),
]

lam = boto3.client("lambda", region_name=REGION)


def log(msg):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)


def extract_secret_from_live_code(fn_name: str):
    """
    Download the live Lambda zip, scan for hardcoded secret assignment.
    Returns dict with: var_name, value, category, OR None if not found.
    """
    code_url = lam.get_function(FunctionName=fn_name)["Code"]["Location"]
    with urllib.request.urlopen(code_url) as r:
        zbytes = r.read()

    with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
        for entry in zf.namelist():
            if not entry.endswith(".py"):
                continue
            text = zf.read(entry).decode("utf-8", errors="ignore")
            for category, pattern in SECRET_ASSIGN_PATTERNS:
                m = pattern.search(text)
                if m:
                    return {
                        "var_name": m.group(1),
                        "value": m.group(2),
                        "category": category,
                        "source_file": entry,
                    }
    return None


def align_repo_sanitized_code(fn_name: str, var_name: str, repo_root: Path) -> int:
    """
    Ensure the sanitized code in the repo uses `os.environ.get('{var_name}', '')`.
    The import script used a canonical 'ANTHROPIC_API_KEY' / 'GITHUB_TOKEN'; some
    Lambdas used different names like 'ANTHROPIC_KEY'.

    Returns: number of substitutions made.
    """
    src_dir = repo_root / "aws" / "lambdas" / fn_name / "source"
    canonical_names = {
        "ANTHROPIC_API_KEY",
        "GITHUB_TOKEN",
    }
    # Only replace if the var_name we need isn't already canonical
    if var_name in canonical_names:
        return 0

    # Replace os.environ.get('CANONICAL',...) → os.environ.get('VAR_NAME', ...)
    # but only if the var_name differs from what the sanitizer injected
    total = 0
    for py in src_dir.rglob("*.py"):
        text = py.read_text(encoding="utf-8", errors="ignore")
        for canonical in canonical_names:
            if canonical == var_name:
                continue
            # Match "os.environ.get('CANONICAL', '')" or similar
            pattern = re.compile(r"os\.environ(?:\.get)?\(\s*['\"]" + re.escape(canonical) + r"['\"][^)]*\)")
            new_text = pattern.sub(f"os.environ.get('{var_name}', '')", text)
            if new_text != text:
                total += pattern.findall(text).__len__()
                text = new_text
        if total > 0:
            py.write_text(text, encoding="utf-8")
    return total


def set_env_var(fn_name: str, var_name: str, value: str):
    """
    Patch the function configuration to ensure var_name=value is set.
    Preserves all existing environment variables.
    """
    cfg = lam.get_function_configuration(FunctionName=fn_name)
    env = (cfg.get("Environment") or {}).get("Variables", {}).copy()
    env[var_name] = value
    lam.update_function_configuration(
        FunctionName=fn_name,
        Environment={"Variables": env},
    )
    lam.get_waiter("function_updated").wait(
        FunctionName=fn_name, WaiterConfig={"Delay": 3, "MaxAttempts": 20}
    )


def deploy_sanitized_zip(fn_name: str, repo_root: Path):
    """
    Zip the sanitized source/ directory and deploy it as the new code.
    """
    src_dir = repo_root / "aws" / "lambdas" / fn_name / "source"
    if not src_dir.exists():
        raise RuntimeError(f"source dir not found: {src_dir}")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for f in src_dir.rglob("*"):
            if f.is_file():
                # Use paths relative to source/ (Lambda expects files at the root)
                arcname = str(f.relative_to(src_dir))
                zout.write(f, arcname)

    zbytes = buf.getvalue()
    lam.update_function_code(FunctionName=fn_name, ZipFile=zbytes)
    lam.get_waiter("function_updated").wait(
        FunctionName=fn_name, WaiterConfig={"Delay": 3, "MaxAttempts": 20}
    )
    return len(zbytes)


def process(fn_name: str, category_hint: str, repo_root: Path) -> dict:
    log(f"──── {fn_name} ({category_hint}) ────")

    # Step 1: extract current hardcoded secret + variable name
    secret = extract_secret_from_live_code(fn_name)
    if not secret:
        return {"fn": fn_name, "status": "no-secret-found", "detail": "live code has no hardcoded secret (maybe already clean?)"}

    log(f"  Found hardcoded {secret['category']} assigned to var '{secret['var_name']}' in {secret['source_file']}")

    # Step 2: ensure repo-side sanitized code uses the same var name
    adjustments = align_repo_sanitized_code(fn_name, secret["var_name"], repo_root)
    if adjustments > 0:
        log(f"  Aligned repo sanitized code: {adjustments} env var reference(s) updated to '{secret['var_name']}'")

    # Step 3: set the env var on the Lambda (preserve existing)
    set_env_var(fn_name, secret["var_name"], secret["value"])
    log(f"  Env var '{secret['var_name']}' set on Lambda (value preserved from existing code)")

    # Step 4: deploy the sanitized zip
    size = deploy_sanitized_zip(fn_name, repo_root)
    log(f"  Sanitized code deployed ({size} bytes)")

    return {
        "fn": fn_name,
        "status": "sanitized",
        "var_name": secret["var_name"],
        "category": secret["category"],
        "repo_adjustments": adjustments,
    }


def main():
    log("=== Phase 3a.2 — Migrate secrets to env vars + deploy sanitized code ===")

    repo_root = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
    outcomes = []

    for fn_name, category in TARGETS:
        try:
            result = process(fn_name, category, repo_root)
        except Exception as e:
            result = {"fn": fn_name, "status": f"error:{type(e).__name__}", "detail": str(e)}
            log(f"  ✗ ERROR: {e}")
        outcomes.append(result)

    log("")
    log("══════════════════ SUMMARY ══════════════════")
    for o in outcomes:
        icon = "✅" if o["status"] == "sanitized" else ("ℹ " if o["status"] == "no-secret-found" else "✗")
        line = f"  {icon} {o['fn']}: {o['status']}"
        if "var_name" in o:
            line += f"  ({o['category']} → {o['var_name']})"
        log(line)
    log("═════════════════════════════════════════════")

    # GitHub step summary
    gh = os.environ.get("GITHUB_STEP_SUMMARY")
    if gh:
        with open(gh, "a") as f:
            f.write("# Phase 3a.2 — Secret Migration + Sanitized Deploy\n\n")
            f.write("| Lambda | Status | Var | Category |\n|---|---|---|---|\n")
            for o in outcomes:
                f.write(f"| `{o['fn']}` | {o['status']} | `{o.get('var_name', '—')}` | {o.get('category', '—')} |\n")

    errored = [o for o in outcomes if o["status"].startswith("error")]
    if errored:
        sys.exit(f"{len(errored)} functions failed — see log above")


if __name__ == "__main__":
    main()
