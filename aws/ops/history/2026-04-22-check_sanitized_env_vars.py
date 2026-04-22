#!/usr/bin/env python3
"""
Verify that every Lambda we sanitized has the expected env var set.

If the hardcoded secret was replaced with `os.environ.get('X')`, X must
already exist in the Lambda's environment — otherwise the sanitized
version will crash at runtime.

For each of the 4 sanitized Lambdas:
  - Check for the expected env var name
  - Report present/missing
  - If missing: log which var needs to be set, and for what reason

Read-only. No deploys. Just a health check before 5_redeploy_sanitized.
"""

import json
import sys
import boto3

REGION = "us-east-1"
lam = boto3.client("lambda", region_name=REGION)

# (lambda_name, required_env_var)
CHECKS = [
    ("justhodl-chat-api",             "ANTHROPIC_API_KEY"),
    ("justhodl-dex-scanner",          "GITHUB_TOKEN"),
    ("justhodl-investor-agents",      "ANTHROPIC_API_KEY"),
    ("justhodl-morning-intelligence", "ANTHROPIC_API_KEY"),
]


def main():
    print("=== Env var readiness check for sanitized Lambdas ===\n")
    report = []

    for fn_name, required_var in CHECKS:
        try:
            cfg = lam.get_function_configuration(FunctionName=fn_name)
        except lam.exceptions.ResourceNotFoundException:
            report.append((fn_name, required_var, "function-missing"))
            continue

        env_vars = (cfg.get("Environment") or {}).get("Variables", {})
        present_vars = sorted(env_vars.keys())
        has_var = required_var in env_vars

        if has_var:
            status = "OK"
            val_preview = env_vars[required_var][:10] + "…" if env_vars[required_var] else "(empty!)"
            print(f"✅ {fn_name}")
            print(f"     {required_var} is set ({val_preview})")
        else:
            status = "NEEDS_SETUP"
            print(f"⚠️  {fn_name}")
            print(f"     {required_var} NOT set — would break after redeploy")
            print(f"     Currently has env vars: {present_vars}")

        report.append((fn_name, required_var, status))
        print()

    ok = [r for r in report if r[2] == "OK"]
    missing = [r for r in report if r[2] == "NEEDS_SETUP"]
    not_found = [r for r in report if r[2] == "function-missing"]

    print("══════════════════ SUMMARY ══════════════════")
    print(f"  Ready for redeploy:  {len(ok)} / {len(CHECKS)}")
    print(f"  Needs env var set:   {len(missing)}")
    print(f"  Function not found:  {len(not_found)}")
    print("═════════════════════════════════════════════")

    if missing:
        print()
        print("To proceed safely, we need to set these env vars before redeploying")
        print("the sanitized code. They can be sourced from the live code's existing")
        print("hardcoded values (pre-sanitization) which are preserved in the audit.")
        print("A follow-up script will handle this.")

    # GitHub step summary
    import os
    gh = os.environ.get("GITHUB_STEP_SUMMARY")
    if gh:
        with open(gh, "a") as f:
            f.write("# Env var check for sanitized Lambdas\n\n")
            f.write("| Lambda | Required env var | Status |\n")
            f.write("|--------|------------------|--------|\n")
            for fn, var, status in report:
                icon = "✅" if status == "OK" else "⚠️"
                f.write(f"| `{fn}` | `{var}` | {icon} {status} |\n")


if __name__ == "__main__":
    main()
