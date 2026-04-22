#!/usr/bin/env python3
"""
Rotate the Anthropic API key across every Lambda that uses it.

Reads the new key value from the ANTHROPIC_API_KEY_NEW environment variable
(set by GitHub Actions from a repo secret). Never logs the value.

For each target function:
  - Reads current env vars (preserving all other keys)
  - Updates the Anthropic-related env var(s) to the new value
  - Waits for function_updated

Also enumerates every Lambda in the account to find ANY that reference an
Anthropic-style env var, in case we missed one. Functions with a matching
var get the new value applied automatically.

After updating, runs smoke tests against:
  - justhodl-ai-chat (via Worker or direct Lambda URL with auth)

Prints the old key's prefix for reference when deleting at
console.anthropic.com/settings/keys.
"""

import io
import json
import os
import sys
import time
import urllib.request
import urllib.error
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"

# Known targets from Phase 3a.2 — these use Anthropic API
KNOWN_ANTHROPIC_VARS = {
    "justhodl-chat-api":              "ANTHROPIC_API_KEY",
    "justhodl-investor-agents":       "ANTHROPIC_KEY",
    "justhodl-morning-intelligence":  "ANTHROPIC_KEY",
}

# Anthropic-style env var names to auto-detect
ANTHROPIC_VAR_NAMES = {
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_KEY",
    "ANTHROPIC_API_TOKEN",
    "CLAUDE_API_KEY",
    "CLAUDE_KEY",
}

# Smoke test: live ai-chat URL + SSM auth token
AI_CHAT_URL = "https://zh3c6izcbzcqwcia4m6dmjnupy0dbnns.lambda-url.us-east-1.on.aws/"
SSM_TOKEN_PARAM = "/justhodl/ai-chat/auth-token"

lam = boto3.client("lambda", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def log(msg):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)


def discover_anthropic_lambdas():
    """Enumerate all Lambdas and return a dict of {fn_name: var_name_used}."""
    discovered = {}
    paginator = lam.get_paginator("list_functions")
    for page in paginator.paginate():
        for fn in page["Functions"]:
            env = (fn.get("Environment") or {}).get("Variables", {}) or {}
            for var in env:
                if var in ANTHROPIC_VAR_NAMES:
                    discovered[fn["FunctionName"]] = var
                    break
    return discovered


def update_env_var(fn_name: str, var_name: str, new_value: str) -> str:
    """Replace just one env var; preserve all others. Returns old prefix for logging."""
    cfg = lam.get_function_configuration(FunctionName=fn_name)
    env = (cfg.get("Environment") or {}).get("Variables", {}).copy()

    old_value = env.get(var_name, "")
    old_prefix = old_value[:12] if old_value else "(was unset)"

    if old_value == new_value:
        return "(already-new)"

    env[var_name] = new_value
    lam.update_function_configuration(
        FunctionName=fn_name,
        Environment={"Variables": env},
    )
    lam.get_waiter("function_updated").wait(
        FunctionName=fn_name, WaiterConfig={"Delay": 3, "MaxAttempts": 20}
    )
    return old_prefix


def smoke_test_ai_chat() -> tuple[bool, str]:
    """Confirm the chat Lambda still works with the rotated key."""
    try:
        token = ssm.get_parameter(Name=SSM_TOKEN_PARAM, WithDecryption=True)["Parameter"]["Value"]
    except ClientError as e:
        return False, f"SSM fetch failed: {e}"

    req = urllib.request.Request(
        AI_CHAT_URL,
        data=json.dumps({"message": "TSLA price in one short line"}).encode(),
        headers={
            "Content-Type": "application/json",
            "Origin": "https://justhodl.ai",
            "x-justhodl-token": token,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            body = r.read().decode(errors="ignore")
        data = json.loads(body)
        return True, data.get("response", "")[:120]
    except urllib.error.HTTPError as e:
        return False, f"HTTP {e.code}: {e.read().decode()[:200]}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def smoke_test_investor_agents() -> tuple[bool, str]:
    """Invoke investor-agents via its Function URL if available."""
    try:
        url = lam.get_function_url_config(FunctionName="justhodl-investor-agents")["FunctionUrl"]
    except ClientError as e:
        return False, f"Function URL lookup failed: {e}"

    req = urllib.request.Request(
        url.rstrip("/") + "/",
        data=json.dumps({"ticker": "AAPL"}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=90) as r:
            body = r.read().decode(errors="ignore")
        data = json.loads(body)
        consensus = data.get("consensus", {}).get("signal", "")
        n_agents = len(data.get("agents", []))
        return True, f"{n_agents} agents returned, consensus={consensus}"
    except urllib.error.HTTPError as e:
        return False, f"HTTP {e.code}: {e.read().decode()[:200]}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def main():
    new_key = os.environ.get("ANTHROPIC_API_KEY_NEW", "").strip()
    if not new_key:
        sys.exit("ANTHROPIC_API_KEY_NEW is not set. Add it as a GitHub secret.")
    if not new_key.startswith("sk-ant-"):
        sys.exit("ANTHROPIC_API_KEY_NEW doesn't look like an Anthropic API key")

    log("=== Anthropic key rotation ===")
    log(f"New key prefix: {new_key[:12]}…  length: {len(new_key)}")

    # Discover every Lambda with an Anthropic env var
    log("Scanning all Lambdas for Anthropic env vars…")
    discovered = discover_anthropic_lambdas()
    log(f"Found {len(discovered)} function(s) using an Anthropic-style env var:")
    for fn, var in sorted(discovered.items()):
        log(f"  - {fn} uses {var}")

    # Sanity check: make sure our known targets are in the discovery set
    for fn, var in KNOWN_ANTHROPIC_VARS.items():
        if fn not in discovered:
            log(f"  ⚠ known target {fn} not discovered (maybe missing env var '{var}' entirely?)")

    # Merge: prefer the discovered var_name but keep KNOWN as floor
    targets = {**KNOWN_ANTHROPIC_VARS, **discovered}

    # Rotate each
    log("")
    log(f"Rotating {len(targets)} function(s)…")
    results = []
    for fn_name in sorted(targets):
        var_name = targets[fn_name]
        log(f"──── {fn_name} [{var_name}] ────")
        try:
            old_prefix = update_env_var(fn_name, var_name, new_key)
            log(f"  ✓ updated (old prefix was: {old_prefix}…)")
            results.append((fn_name, var_name, old_prefix, None))
        except ClientError as e:
            msg = f"{e.response['Error']['Code']}: {e.response['Error']['Message']}"
            log(f"  ✗ FAILED: {msg}")
            results.append((fn_name, var_name, None, msg))

    # Smoke tests
    log("")
    log("Waiting 5s for env var propagation, then running smoke tests…")
    time.sleep(5)

    ok_chat, chat_out = smoke_test_ai_chat()
    log(f"  ai-chat: {'✓ PASS' if ok_chat else '✗ FAIL'} — {chat_out}")

    ok_inv, inv_out = smoke_test_investor_agents()
    log(f"  investor-agents: {'✓ PASS' if ok_inv else '✗ FAIL'} — {inv_out}")

    # Summary
    log("")
    log("══════════════════ ROTATION SUMMARY ══════════════════")
    old_prefixes = set()
    for fn, var, old_prefix, err in results:
        if err:
            log(f"  ✗ {fn}.{var}: {err}")
        else:
            log(f"  ✓ {fn}.{var}: rotated (old prefix: {old_prefix})")
            if old_prefix and old_prefix != "(already-new)":
                old_prefixes.add(old_prefix)

    log("")
    log("Smoke tests:")
    log(f"  ai-chat:         {'PASS' if ok_chat else 'FAIL'}")
    log(f"  investor-agents: {'PASS' if ok_inv else 'FAIL'}")
    log("═══════════════════════════════════════════════════════")

    if old_prefixes:
        log("")
        log("⚠ NEXT STEP — delete the OLD key at console.anthropic.com/settings/keys")
        log(f"   Its prefix is: {', '.join(sorted(old_prefixes))}")

    # GitHub step summary
    gh = os.environ.get("GITHUB_STEP_SUMMARY")
    if gh:
        with open(gh, "a") as f:
            f.write("# Anthropic Key Rotation\n\n")
            f.write("| Lambda | Env var | Result | Old prefix |\n|---|---|---|---|\n")
            for fn, var, old_prefix, err in results:
                if err:
                    f.write(f"| `{fn}` | `{var}` | ❌ {err} | — |\n")
                else:
                    f.write(f"| `{fn}` | `{var}` | ✅ | `{old_prefix}…` |\n")
            f.write(f"\n## Smoke tests\n\n")
            f.write(f"- `ai-chat`: {'✅ PASS' if ok_chat else '❌ FAIL'} — `{chat_out[:80]}`\n")
            f.write(f"- `investor-agents`: {'✅ PASS' if ok_inv else '❌ FAIL'} — `{inv_out[:80]}`\n")
            if old_prefixes:
                f.write(f"\n## Next step\n\nDelete old key(s) with prefix `{list(old_prefixes)[0]}…` at [console.anthropic.com/settings/keys](https://console.anthropic.com/settings/keys).\n")

    errored = [r for r in results if r[3]]
    if errored or not (ok_chat and ok_inv):
        sys.exit("Rotation did not fully succeed — see above")


if __name__ == "__main__":
    main()
