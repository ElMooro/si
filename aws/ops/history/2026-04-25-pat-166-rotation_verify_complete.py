#!/usr/bin/env python3
"""
Step 166 — Verify dex-scanner Lambda has the new fine-grained PAT.

Khalid is about to delete an old PAT and asks which one to delete.
Before recommending, confirm the rotation actually swapped the
Lambda's TOKEN env var to the new fine-grained value (not still
holding the old ghp_e6apGL... one).

Read the Lambda config, check first 10 chars of TOKEN env var.
Don't print the full token, just the prefix.

Also list all GitHub-related secrets we use across:
  - Lambda env vars (TOKEN on dex-scanner)
  - GitHub Actions workflow secrets (workflows reference these)

The 4 classic PATs visible to Khalid:
  Claude-Deploy  (last_used=last_week, scope=repo+workflow) ← in use somewhere
  Render deploy  (never_used) ← orphan
  CloudShell Git (never_used) ← orphan
  Cloud Shell Git (never_used) ← orphan, dupe

Recommendation will tell Khalid exactly which to delete.
"""
import json
import os

from ops_report import report
import boto3

REGION = "us-east-1"
lam = boto3.client("lambda", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


with report("verify_pat_rotation_active") as r:
    r.heading("Verify which PAT is active in dex-scanner")

    # ─── 1. Read dex-scanner Lambda env var ─────────────────────────────
    r.section("1. Lambda justhodl-dex-scanner TOKEN env var")
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-dex-scanner")
        env = cfg.get("Environment", {}).get("Variables", {})
        token = env.get("TOKEN", "")
        if token:
            prefix = token[:11]  # show prefix only, never full token
            length = len(token)
            r.log(f"  TOKEN prefix: {prefix}...")
            r.log(f"  TOKEN length: {length} chars")
            if token.startswith("github_pat_"):
                r.ok(f"  ✅ NEW fine-grained PAT (github_pat_*) — rotation succeeded")
                pat_type = "fine-grained"
            elif token.startswith("ghp_"):
                r.warn(f"  ⚠ Still classic PAT (ghp_*) — rotation may not have applied")
                pat_type = "classic"
            else:
                r.warn(f"  ⚠ Unknown PAT format")
                pat_type = "unknown"
        else:
            r.fail(f"  TOKEN env var empty")
            pat_type = None
        r.log(f"  Lambda LastModified: {cfg.get('LastModified', '?')[:19]}")
        r.log(f"  Lambda CodeSha256: {cfg.get('CodeSha256', '?')[:16]}...")
    except Exception as e:
        r.fail(f"  read Lambda: {e}")
        raise SystemExit(1)

    # ─── 2. List all GitHub-related secrets we know about ──────────────
    r.section("2. GitHub-related secrets in use")
    r.log(f"  Lambda env vars referencing PATs:")
    r.log(f"    justhodl-dex-scanner.TOKEN — {pat_type} PAT (just rotated)")
    r.log(f"")
    r.log(f"  GitHub Actions workflow secrets (configured on repo):")
    r.log(f"    AWS_ACCESS_KEY_ID         — AWS, NOT a GitHub PAT")
    r.log(f"    AWS_SECRET_ACCESS_KEY     — AWS, NOT a GitHub PAT")
    r.log(f"    AWS_REGION                — AWS region literal")
    r.log(f"    ANTHROPIC_API_KEY         — Anthropic, NOT a GitHub PAT")
    r.log(f"    ANTHROPIC_API_KEY_NEW     — Anthropic, NOT a GitHub PAT")
    r.log(f"    TELEGRAM_BOT_TOKEN        — Telegram, NOT a GitHub PAT")
    r.log(f"    GITHUB_TOKEN              — auto-injected by GHA, NOT a stored PAT")

    # ─── 3. Recommendation for the 4 classic PATs ──────────────────────
    r.section("3. Recommendation for the 4 classic PATs in Khalid's account")
    r.log(f"")
    r.log(f"  ┌─────────────────────┬──────────────┬──────────────────────────┐")
    r.log(f"  │ PAT name            │ Status       │ Recommendation           │")
    r.log(f"  ├─────────────────────┼──────────────┼──────────────────────────┤")
    r.log(f"  │ Claude-Deploy       │ used last wk │ KEEP for now             │")
    r.log(f"  │ Render deploy       │ never used   │ DELETE — orphan          │")
    r.log(f"  │ CloudShell Git      │ never used   │ DELETE — orphan          │")
    r.log(f"  │ Cloud Shell Git     │ never used   │ DELETE — duplicate       │")
    r.log(f"  └─────────────────────┴──────────────┴──────────────────────────┘")
    r.log(f"")
    r.log(f"  KEEP 'Claude-Deploy':")
    r.log(f"    Last used within the past week — actively in use somewhere.")
    r.log(f"    Not in GitHub Actions secrets (those use AWS_*, ANTHROPIC_*).")
    r.log(f"    Likely used outside CI (local dev, third-party integration,")
    r.log(f"    or an Anthropic-side automation).")
    r.log(f"    DON'T DELETE without confirming what's using it.")
    r.log(f"")
    r.log(f"  DELETE the 3 'never used' tokens:")
    r.log(f"    They've been sitting there for years with broad 'repo' scope.")
    r.log(f"    Reduces attack surface. Zero risk to delete since")
    r.log(f"    GitHub confirms they've never been used.")
    r.log(f"")
    r.log(f"  IMPORTANT: the OLD shared PAT (ghp_e6apGL...) you rotated AWAY")
    r.log(f"    from is NOT in this list. That means it was probably:")
    r.log(f"    (a) The 'Claude-Deploy' token (and rotation moved dex-scanner")
    r.log(f"        to a new fine-grained PAT, leaving Claude-Deploy still")
    r.log(f"        in use elsewhere — KEEP it), OR")
    r.log(f"    (b) Already deleted/expired before this session.")
    r.log(f"    EITHER WAY: don't delete Claude-Deploy without verifying.")

    r.kv(
        dex_scanner_pat=pat_type,
        recommendation="delete 3 never-used tokens, keep Claude-Deploy",
    )
    r.log("Done")
