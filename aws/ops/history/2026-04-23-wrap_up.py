#!/usr/bin/env python3
"""
Rest-of-day wrap-up — three independent tasks in one run.

TASK A — Sector rotation $M formatter
  Secretary v2.2 email shows "Healthcare (XLV) $89,180,020.0M" because
  upstream options-flow stores flows in raw dollars but the secretary
  label says "$M". Divide by 1e6 before formatting.

TASK B — dex-scanner TOKEN audit
  The Lambda code reads os.environ['TOKEN']. Need to:
    - Check what's currently set (mask first 8 chars for safety)
    - If the prefix matches the known Claude-Deploy PAT, flag for rotation
    - Emit a recommendation for the user to rotate at GitHub

  IMPORTANT: We will NOT rotate the PAT ourselves — rotating a PAT
  requires going to github.com/settings/tokens which is a manual step
  Khalid has to do. But we CAN inventory what's there so he knows.

TASK C — S3 public HTTPS 403 investigation
  The bucket policy claims public read for data/* but browsers get 403.
  We HTTP-fetch each key the dashboards read + report status codes.
  Also inspect the current bucket policy and any misconfigurations.

Each task is independent — failures don't cascade.
"""

import io
import json
import os
import re
import ssl
import time
import urllib.request
import urllib.error
import zipfile
from pathlib import Path
from datetime import datetime, timezone, timedelta

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
SEC = REPO_ROOT / "aws/lambdas/justhodl-financial-secretary/source/lambda_function.py"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def build_zip(src_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for f in src_dir.rglob("*"):
            if f.is_file():
                zout.write(f, str(f.relative_to(src_dir)))
    return buf.getvalue()


with report("wrap_up") as r:
    r.heading("Rest-of-day wrap-up — sector fmt + dex-scanner TOKEN + S3 403")

    # ═══════════════════════════════════════════════════════
    # TASK A — Sector rotation $M formatter
    # ═══════════════════════════════════════════════════════
    r.section("TASK A — Fix sector rotation flow label ($M)")
    src = SEC.read_text(encoding="utf-8")

    # The current format line divides by nothing. We need to divide by 1e6
    # to get real millions. Target:
    #   leaders.append(f"{name} (inflow ${float(flow):+.0f}M)")
    # Becomes:
    #   leaders.append(f"{name} (inflow ${float(flow)/1e6:+.0f}M)")
    changed_count = 0
    old_in = 'leaders.append(f"{name} (inflow ${float(flow):+.0f}M)")'
    new_in = 'leaders.append(f"{name} (inflow ${float(flow)/1e6:+.0f}M)")'
    if old_in in src:
        src = src.replace(old_in, new_in, 1)
        changed_count += 1
        r.ok("  Inflow format patched")

    old_out = 'laggards.append(f"{name} (outflow ${float(flow):+.0f}M)")'
    new_out = 'laggards.append(f"{name} (outflow ${float(flow)/1e6:+.0f}M)")'
    if old_out in src:
        src = src.replace(old_out, new_out, 1)
        changed_count += 1
        r.ok("  Outflow format patched")

    if changed_count == 2:
        import ast
        try:
            ast.parse(src)
            SEC.write_text(src, encoding="utf-8")
            r.ok(f"  Secretary source updated ({len(src)} bytes)")
            # Deploy
            z = build_zip(SEC.parent)
            lam.update_function_code(FunctionName="justhodl-financial-secretary", ZipFile=z)
            lam.get_waiter("function_updated").wait(
                FunctionName="justhodl-financial-secretary",
                WaiterConfig={"Delay": 3, "MaxAttempts": 20},
            )
            r.ok(f"  Secretary deployed ({len(z)} bytes)")
            r.kv(task="A", status="shipped")
        except SyntaxError as e:
            r.fail(f"  SYNTAX ERROR: {e}")
            r.kv(task="A", status="failed")
    else:
        r.warn(f"  Only {changed_count}/2 format sites matched — skipping deploy")
        r.kv(task="A", status="skipped", matched=changed_count)

    # ═══════════════════════════════════════════════════════
    # TASK B — dex-scanner TOKEN audit
    # ═══════════════════════════════════════════════════════
    r.section("TASK B — dex-scanner TOKEN env var inventory")
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-dex-scanner")
        env = cfg.get("Environment", {}).get("Variables", {}) or {}
        token = env.get("TOKEN", "")
        if not token:
            r.warn("  No TOKEN env var set on justhodl-dex-scanner")
            r.log("  This means the Lambda can't authenticate to GitHub API — likely already broken")
            r.kv(task="B", token_status="missing")
        else:
            # Compare prefix with known Claude-Deploy PAT
            masked = token[:8] + "…" + token[-4:] if len(token) > 12 else "[short]"
            r.log(f"  TOKEN present (masked): {masked}")
            r.log(f"  Length: {len(token)} chars")
            r.log(f"  Prefix (first 4): {token[:4]}")

            # Known Claude-Deploy PAT prefix
            known_pat_prefix = "ghp_e6ap"
            if token.startswith(known_pat_prefix):
                r.warn("  ⚠ TOKEN is the Claude-Deploy PAT — same one used for GitHub Actions")
                r.log("  Recommendation: generate a separate PAT for this Lambda only")
                r.log("    1. Visit https://github.com/settings/tokens")
                r.log("    2. Generate new token 'justhodl-dex-scanner-pat'")
                r.log("       Scopes: repo (needed for dex.html PUT)")
                r.log("    3. aws lambda update-function-configuration \\")
                r.log(f"         --function-name justhodl-dex-scanner \\")
                r.log(f"         --environment 'Variables={{TOKEN=<new_pat>}}'")
                r.log("    4. (Optional) revoke the old Claude-Deploy PAT once Actions workflows are updated")
                r.kv(task="B", token_status="same-as-deploy-pat", action_needed="manual-rotation")
            else:
                r.log(f"  TOKEN is a DIFFERENT secret from the Claude-Deploy PAT")
                r.log("  If this PAT was exposed in git history earlier (yes, we rotated ghp_e6ap…),")
                r.log("  inspect whether THIS specific PAT has ever been in any commit.")
                r.kv(task="B", token_status="different-pat", review_needed="history-scan")
    except lam.exceptions.ResourceNotFoundException:
        r.warn("  justhodl-dex-scanner not found (may have been deleted in Phase 2 cleanup)")
        r.kv(task="B", token_status="lambda-not-found")
    except Exception as e:
        r.fail(f"  Audit failed: {e}")
        r.kv(task="B", token_status="error", error=str(e)[:100])

    # ═══════════════════════════════════════════════════════
    # TASK C — S3 public HTTPS 403 investigation
    # ═══════════════════════════════════════════════════════
    r.section("TASK C — S3 public HTTPS accessibility check")

    # Known dashboard HTML files probably read these JSON data files
    data_files_to_test = [
        "data/report.json",
        "data/secretary-latest.json",
        "data/fred-cache.json",
        "flow-data.json",
        "crypto-intel.json",
        "data/dashboard.json",
        "data/intelligence-report.json",
    ]

    # Fetch bucket policy first
    r.log("\n  → Bucket policy:")
    try:
        pol = s3.get_bucket_policy(Bucket=BUCKET)
        policy = json.loads(pol["Policy"])
        for stmt in policy.get("Statement", []):
            sid = stmt.get("Sid", "")
            effect = stmt.get("Effect", "")
            principal = stmt.get("Principal", "")
            actions = stmt.get("Action", "")
            resource = stmt.get("Resource", "")
            condition = stmt.get("Condition", {})
            r.log(f"    [{sid}] {effect} {principal} {actions} on {resource}")
            if condition:
                r.log(f"      Condition: {condition}")
    except Exception as e:
        r.warn(f"    Bucket policy fetch failed: {e}")

    # Fetch public access block
    r.log("\n  → Public access block:")
    try:
        pab = s3.get_public_access_block(Bucket=BUCKET)
        conf = pab.get("PublicAccessBlockConfiguration", {})
        r.log(f"    BlockPublicAcls: {conf.get('BlockPublicAcls')}")
        r.log(f"    IgnorePublicAcls: {conf.get('IgnorePublicAcls')}")
        r.log(f"    BlockPublicPolicy: {conf.get('BlockPublicPolicy')}")
        r.log(f"    RestrictPublicBuckets: {conf.get('RestrictPublicBuckets')}")
    except Exception as e:
        r.log(f"    No public access block (or fetch failed): {e}")

    # Fetch website config
    r.log("\n  → Website config:")
    try:
        ws = s3.get_bucket_website(Bucket=BUCKET)
        r.log(f"    Index: {ws.get('IndexDocument', {}).get('Suffix')}")
        r.log(f"    Error: {ws.get('ErrorDocument', {}).get('Key')}")
    except Exception as e:
        r.log(f"    No website config: {e}")

    # Try two URL patterns per file: virtual-hosted-style + website endpoint
    r.log("\n  → HTTPS probe results:")
    ctx = ssl.create_default_context()
    probe_results = {}

    for key in data_files_to_test:
        for url_type, url in [
            ("virtual-hosted", f"https://{BUCKET}.s3.amazonaws.com/{key}"),
            ("path-style",     f"https://s3.amazonaws.com/{BUCKET}/{key}"),
            ("website",        f"http://{BUCKET}.s3-website-us-east-1.amazonaws.com/{key}"),
        ]:
            try:
                req = urllib.request.Request(url, method="HEAD")
                with urllib.request.urlopen(req, timeout=8, context=ctx) as resp:
                    status = resp.status
                    probe_results.setdefault(key, []).append((url_type, status, None))
                    break  # If one works, don't keep trying other styles for this key
            except urllib.error.HTTPError as e:
                probe_results.setdefault(key, []).append((url_type, e.code, e.reason))
            except Exception as e:
                probe_results.setdefault(key, []).append((url_type, None, str(e)[:60]))

    for key, attempts in probe_results.items():
        r.log(f"    {key}:")
        for (url_type, status, err) in attempts:
            if status == 200:
                r.log(f"      ✓ {url_type}: 200 OK")
                break
            elif status:
                r.log(f"      ✗ {url_type}: {status} {err or ''}")
            else:
                r.log(f"      ? {url_type}: {err}")

    # Summary
    anyok = sum(1 for attempts in probe_results.values() if any(s == 200 for _, s, _ in attempts))
    all403 = sum(1 for attempts in probe_results.values() if all(s == 403 for _, s, _ in attempts if s))
    r.kv(task="C", files_tested=len(data_files_to_test), any_accessible=anyok, all_403=all403)

    if anyok == 0:
        r.log("\n  CONCLUSION: no data files are publicly accessible via HTTPS.")
        r.log("  If dashboards read via S3 directly (not CloudFront), they're broken for visitors.")
        r.log("  Likely fixes:")
        r.log("    a) Update bucket policy to explicitly allow s3:GetObject for data/* arn")
        r.log("    b) Remove BlockPublicPolicy from public access block if enabled")
        r.log("    c) Put CloudFront in front of the bucket (best for prod)")
    elif anyok == len(data_files_to_test):
        r.log("\n  CONCLUSION: all files publicly accessible. Dashboards should work for visitors.")
    else:
        r.log(f"\n  CONCLUSION: mixed — {anyok}/{len(data_files_to_test)} accessible.")

    r.log("Done")
