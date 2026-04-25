#!/usr/bin/env python3
"""
Step 96 — Apply per-Lambda dispositions for the 7 broken Lambdas.

Root causes identified in step 95:

  Handler config bugs (just point to wrong module/file):
    - global-liquidity-agent-v2 — handler points to khalid_no_email,
      file is global_liquidity_fixed.py
    - treasury-auto-updater — handler points to updater, file is
      lambda_function.py
    - news-sentiment-agent — handler points to lambda_news_agent,
      file is lambda_function.py BUT the file is a 7-line stub

  Real code bugs:
    - daily-liquidity-report — passes ACL= to put_object, but bucket
      doesn't allow ACLs. Remove the ACL parameter.
    - ecb-data-daily-updater — assumes indicators is list of dicts,
      but it's now list of strings. Handle both.

  Permission issues:
    - fmp-stock-picks-agent — calls SES SendEmail without perm.
      Quickest safe action: comment out the email block.

  Dead/superseded code:
    - justhodl-data-collector — calls dead api.justhodl.ai (broken
      since CF migration), only writes a single 'data.json' file
      that's the legacy orphan we already documented as stale.
      DISABLE the EB rule.

This step:
  1. Fix global-liquidity-agent-v2 handler (was 'khalid_no_email.lambda_handler')
  2. Fix treasury-auto-updater handler
  3. DELETE the EB rule for news-sentiment-agent + DISABLE additional
     rules — the file is a stub so the Lambda has no business logic
  4. Patch daily-liquidity-report source: remove ACL param
  5. Patch ecb-data-daily-updater: handle both string/dict indicators
  6. Patch fmp-stock-picks-agent: comment out SES block (preserves
     the rest of the stock-picking logic)
  7. DISABLE justhodl-data-collector EB rule (writes to dead orphan)

All disable operations are reversible. All source patches go through
git so we can `git revert` if any of them break things further.
"""
import io
import json
import os
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

lam = boto3.client("lambda", region_name=REGION)
eb = boto3.client("events", region_name=REGION)


def update_handler(name, new_handler, r):
    """Update a Lambda's handler config."""
    try:
        cur = lam.get_function_configuration(FunctionName=name)
        old_handler = cur.get("Handler")
        if old_handler == new_handler:
            r.log(f"    {name}: handler already {new_handler}, skipping")
            return False
        lam.update_function_configuration(FunctionName=name, Handler=new_handler)
        lam.get_waiter("function_updated").wait(
            FunctionName=name, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
        )
        r.ok(f"    {name}: handler {old_handler} → {new_handler}")
        return True
    except Exception as e:
        r.fail(f"    {name}: handler update failed: {e}")
        return False


def disable_eb_rules_for_lambda(name, r):
    """Disable all EB rules targeting this Lambda. Returns list of disabled rule names."""
    target_arn = f"arn:aws:lambda:us-east-1:857687956942:function:{name}"
    try:
        rule_names = eb.list_rule_names_by_target(TargetArn=target_arn).get("RuleNames", [])
        disabled = []
        for rn in rule_names:
            try:
                d = eb.describe_rule(Name=rn)
                if d.get("State") == "DISABLED":
                    r.log(f"    {rn}: already disabled, skipping")
                    continue
                eb.disable_rule(Name=rn)
                disabled.append(rn)
                r.ok(f"    {rn}: disabled (was {d.get('ScheduleExpression', '?')})")
            except Exception as e:
                r.fail(f"    {rn}: disable failed: {e}")
        return disabled
    except Exception as e:
        r.fail(f"  eb lookup for {name} failed: {e}")
        return []


def deploy_lambda_source(name, source_files, r):
    """Re-deploy a Lambda from local source files. source_files = list of (relpath, content_str)."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for relpath, content in source_files:
            info = zipfile.ZipInfo(relpath)
            info.external_attr = 0o644 << 16
            zout.writestr(info, content)
    zbytes = buf.getvalue()
    try:
        lam.update_function_code(FunctionName=name, ZipFile=zbytes)
        lam.get_waiter("function_updated").wait(
            FunctionName=name, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
        )
        r.ok(f"    {name}: deployed {len(zbytes)}B")
        return True
    except Exception as e:
        r.fail(f"    {name}: deploy failed: {e}")
        return False


def test_invoke(name, r):
    """Sync test invoke a Lambda. Returns True if no FunctionError."""
    try:
        resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse")
        if resp.get("FunctionError"):
            payload = resp.get("Payload").read().decode()
            r.warn(f"    {name}: still erroring: {payload[:200]}")
            return False
        r.ok(f"    {name}: invoke clean ({resp.get('StatusCode')})")
        return True
    except Exception as e:
        r.fail(f"    {name}: invoke failed: {e}")
        return False


with report("fix_or_dispose_broken_lambdas") as r:
    r.heading("Apply per-Lambda fixes/dispositions")

    results = {}

    # ────────────────────────────────────────────────────────────────────
    # 1. global-liquidity-agent-v2 — handler config bug
    # ────────────────────────────────────────────────────────────────────
    r.section("1. global-liquidity-agent-v2 — fix handler")
    name = "global-liquidity-agent-v2"
    src_dir = REPO_ROOT / "aws/lambdas" / name / "source"
    # The file is global_liquidity_fixed.py
    target_file = src_dir / "global_liquidity_fixed.py"
    if target_file.exists():
        # Look for the entry point function
        content = target_file.read_text()
        # Find a top-level `def lambda_handler` or similar
        entry_match = re.search(r"^def\s+(\w+)\s*\(", content, re.MULTILINE)
        if entry_match:
            entry_fn = entry_match.group(1)
            new_handler = f"global_liquidity_fixed.{entry_fn}"
            r.log(f"    Found entry function: {entry_fn}")
            results[name] = update_handler(name, new_handler, r)
            if results[name]:
                test_invoke(name, r)
        else:
            r.warn(f"    No def found in {target_file.name}")
            results[name] = False
    else:
        r.warn(f"    Source file not found: {target_file}")
        results[name] = False

    # ────────────────────────────────────────────────────────────────────
    # 2. treasury-auto-updater — handler config bug
    # ────────────────────────────────────────────────────────────────────
    r.section("2. treasury-auto-updater — fix handler")
    name = "treasury-auto-updater"
    src_dir = REPO_ROOT / "aws/lambdas" / name / "source"
    target_file = src_dir / "lambda_function.py"
    if target_file.exists():
        content = target_file.read_text()
        # Look for the lambda_handler def
        entry_match = re.search(r"^def\s+(lambda_handler|handler|main)\s*\(", content, re.MULTILINE)
        if entry_match:
            entry_fn = entry_match.group(1)
            new_handler = f"lambda_function.{entry_fn}"
            r.log(f"    Found entry: {entry_fn}")
            results[name] = update_handler(name, new_handler, r)
            if results[name]:
                test_invoke(name, r)
        else:
            r.warn(f"    No lambda_handler-like def found")
            results[name] = False
    else:
        r.warn(f"    No lambda_function.py found")
        results[name] = False

    # ────────────────────────────────────────────────────────────────────
    # 3. news-sentiment-agent — disable (file is a 7-line stub)
    # ────────────────────────────────────────────────────────────────────
    r.section("3. news-sentiment-agent — disable EB (7-line stub, no real code)")
    name = "news-sentiment-agent"
    src_dir = REPO_ROOT / "aws/lambdas" / name / "source"
    # Check source size to confirm it's a stub
    has_stub = False
    for p in src_dir.rglob("*.py"):
        if p.read_text().count("\n") < 20:
            has_stub = True
            r.log(f"    {p.name} is {p.read_text().count(chr(10))} lines — confirmed stub")
            break
    if has_stub:
        results[name] = disable_eb_rules_for_lambda(name, r)
    else:
        r.log(f"    Source is not a stub; need real fix not disable")
        results[name] = []

    # ────────────────────────────────────────────────────────────────────
    # 4. daily-liquidity-report — patch source: remove ACL param
    # ────────────────────────────────────────────────────────────────────
    r.section("4. daily-liquidity-report — remove ACL= from put_object")
    name = "daily-liquidity-report"
    src_dir = REPO_ROOT / "aws/lambdas" / name / "source"
    target_file = src_dir / "lambda_function.py"
    if target_file.exists():
        content = target_file.read_text()
        # Remove `ACL='public-read',` or `ACL='...',` patterns
        new_content = re.sub(
            r"""\s*ACL\s*=\s*['"][^'"]*['"]\s*,?""",
            "",
            content,
        )
        if new_content != content:
            target_file.write_text(new_content)
            r.ok(f"    Patched: removed ACL= argument")
            # Re-deploy with updated source
            source_files = []
            for p in src_dir.rglob("*"):
                if p.is_file():
                    source_files.append((str(p.relative_to(src_dir)), p.read_text(encoding="utf-8", errors="ignore")))
            results[name] = deploy_lambda_source(name, source_files, r)
            if results[name]:
                test_invoke(name, r)
        else:
            r.warn(f"    No ACL= pattern found in source")
            results[name] = False
    else:
        results[name] = False

    # ────────────────────────────────────────────────────────────────────
    # 5. ecb-data-daily-updater — handle list of strings shape
    # ────────────────────────────────────────────────────────────────────
    r.section("5. ecb-data-daily-updater — handle string-or-dict indicators")
    name = "ecb-data-daily-updater"
    src_dir = REPO_ROOT / "aws/lambdas" / name / "source"
    target_file = src_dir / "lambda_function.py"
    if target_file.exists():
        content = target_file.read_text()
        # The bug: code uses `indicator.get('symbol', '')` but indicator is a string.
        # Fix: handle both shapes by checking type first.
        old_pattern = "if 'CISS' in indicator.get('symbol', '') and 'SS_CI' in indicator.get('symbol', ''):"
        new_pattern = """# Handle both legacy dict shape and newer string shape
        sym = indicator.get('symbol', '') if isinstance(indicator, dict) else (indicator or '')
        if 'CISS' in sym and 'SS_CI' in sym:"""
        if old_pattern in content:
            content_new = content.replace(old_pattern, new_pattern, 1)
            target_file.write_text(content_new)
            r.ok(f"    Patched: now handles both dict and string indicators")
            source_files = []
            for p in src_dir.rglob("*"):
                if p.is_file():
                    source_files.append((str(p.relative_to(src_dir)), p.read_text(encoding="utf-8", errors="ignore")))
            results[name] = deploy_lambda_source(name, source_files, r)
            if results[name]:
                test_invoke(name, r)
        else:
            r.warn(f"    Expected pattern not found; manual review")
            results[name] = False
    else:
        results[name] = False

    # ────────────────────────────────────────────────────────────────────
    # 6. fmp-stock-picks-agent — comment out SES block (no perms)
    # ────────────────────────────────────────────────────────────────────
    r.section("6. fmp-stock-picks-agent — disable SES send (lacks IAM perm)")
    name = "fmp-stock-picks-agent"
    src_dir = REPO_ROOT / "aws/lambdas" / name / "source"
    target_file = src_dir / "lambda_function.py"
    if target_file.exists():
        content = target_file.read_text()
        # Find ses.send_email( ... ) call(s), wrap in try/except + log warning
        # Simpler approach: replace the call so it's a no-op
        ses_pattern = r"ses\.send_email\("
        if re.search(ses_pattern, content):
            # Wrap in try/except + comment
            patched = re.sub(
                r"(\s+)ses\.send_email\(",
                r"\1# SES perms missing — disabled 2026-04-25 by ops/96\n\1_disabled_send_email = lambda **kw: None\n\1_disabled_send_email(  # was: ses.send_email(",
                content,
                count=0,
            )
            if patched != content:
                target_file.write_text(patched)
                r.ok(f"    Patched: SES send_email calls neutralized")
                source_files = []
                for p in src_dir.rglob("*"):
                    if p.is_file():
                        source_files.append((str(p.relative_to(src_dir)), p.read_text(encoding="utf-8", errors="ignore")))
                results[name] = deploy_lambda_source(name, source_files, r)
                if results[name]:
                    test_invoke(name, r)
        else:
            r.warn(f"    No ses.send_email pattern found")
            results[name] = False
    else:
        results[name] = False

    # ────────────────────────────────────────────────────────────────────
    # 7. justhodl-data-collector — disable (calls dead api, writes orphan)
    # ────────────────────────────────────────────────────────────────────
    r.section("7. justhodl-data-collector — disable (calls dead api.justhodl.ai)")
    name = "justhodl-data-collector"
    results[name] = disable_eb_rules_for_lambda(name, r)

    # ────────────────────────────────────────────────────────────────────
    # Summary
    # ────────────────────────────────────────────────────────────────────
    r.section("Summary")
    successes = []
    failures = []
    disabled_rules = []
    for n, res in results.items():
        if isinstance(res, list):
            disabled_rules.extend(res)
            r.log(f"  {n:40} DISABLED {len(res)} rule(s)")
        elif res:
            successes.append(n)
            r.log(f"  {n:40} FIXED")
        else:
            failures.append(n)
            r.log(f"  {n:40} FAILED — needs manual review")

    r.kv(
        fixed=len(successes),
        rules_disabled=len(disabled_rules),
        failed_or_needs_review=len(failures),
    )
    r.log("Done")
