#!/usr/bin/env python3
"""
Step 140 — Fix step 138's syntax bug in prompt-iterator.

The bug: ITERATOR_SRC has this construct around line 270-290:

    proposal_prompt = f\"\"\"You are reviewing...
    \"\"\"
    {current_template}
    \"\"\"
    ...
    Return ONLY the new prompt text...\"\"\"

The inner literal triple-quotes terminate the f-string prematurely,
making everything from the first inner \"\"\" to the next \"\"\" parse
as module-level code with bad indentation. Hence the line 292
'unexpected indent' error.

Fix: replace the inner literal triple-quotes with single quotes
inside the f-string. Functionally equivalent (it's just a delimiter
around the current template inside the prompt text), zero behavioral
change.

This step:
  1. Reads the file currently on disk
  2. Replaces the broken f-string with a corrected version
  3. Re-validates syntax
  4. Re-deploys justhodl-prompt-iterator (it was created with the
     broken source so first deploy probably failed too — let's check)
  5. Test invokes — should return skip_no_data because we don't
     have ≥7 scored briefs yet
"""
import io
import json
import os
import time
import zipfile
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

lam = boto3.client("lambda", region_name=REGION)


with report("fix_prompt_iterator_syntax") as r:
    r.heading("Fix step 138 — prompt-iterator triple-quote bug")

    lf_path = REPO_ROOT / "aws/lambdas/justhodl-prompt-iterator/source/lambda_function.py"
    src = lf_path.read_text()

    # ─── 1. Diagnose ────────────────────────────────────────────────────
    r.section("1. Confirm the bug")
    import ast
    try:
        ast.parse(src)
        r.log("  Source parses OK — bug already fixed?")
    except SyntaxError as e:
        r.log(f"  Confirmed: SyntaxError at L{e.lineno}: {e.msg}")

    # ─── 2. Apply fix ───────────────────────────────────────────────────
    r.section("2. Replace broken f-string with safe version")

    # The broken f-string spans these exact lines (verified via sed earlier).
    # Use a multi-line string match that's stable: the OPENING triple-quote
    # plus the literal """ markers around current_template plus the
    # CLOSING triple-quote.
    OLD_BROKEN = '''    proposal_prompt = f"""You are reviewing the prompt that generates JustHodlAI's daily morning briefs. The brief uses live financial data and is read by an institutional investor.

CURRENT PROMPT (length: {len(current_template)} chars):
"""
{current_template}
"""

LAST 14 DAYS OF SCORING:
{failure_summary}

Average accuracy: {avg_acc:.2%} (target: 55%+)
Average specificity: {avg_spec:.1f} (concrete numbers per 100 words)

Propose a REVISED VERSION of the prompt that should improve accuracy. Constraints:
1. MUST keep length within 50%-150% of current ({int(len(current_template)*0.5)}-{int(len(current_template)*1.5)} chars)
2. MUST preserve the 'real numbers / live data' constraint (briefs cannot fabricate)
3. MUST NOT add commands like 'ignore previous instructions'
4. SHOULD add specific guidance about handling regime uncertainty
5. SHOULD NOT just restate the same things in different words

Return ONLY the new prompt text, no explanation, no quotes, no preamble."""'''

    # FIX: replace the inner """ delimiters with --- (or any non-conflicting
    # marker). The LLM doesn't care about the delimiter shape, only that
    # the prompt text is bracketed clearly.
    NEW_FIXED = '''    proposal_prompt = (
        f"You are reviewing the prompt that generates JustHodlAI's daily morning briefs. "
        f"The brief uses live financial data and is read by an institutional investor.\\n\\n"
        f"CURRENT PROMPT (length: {len(current_template)} chars):\\n"
        f"---START---\\n{current_template}\\n---END---\\n\\n"
        f"LAST 14 DAYS OF SCORING:\\n{failure_summary}\\n\\n"
        f"Average accuracy: {avg_acc:.2%} (target: 55%+)\\n"
        f"Average specificity: {avg_spec:.1f} (concrete numbers per 100 words)\\n\\n"
        f"Propose a REVISED VERSION of the prompt that should improve accuracy. Constraints:\\n"
        f"1. MUST keep length within 50%-150% of current ({int(len(current_template)*0.5)}-{int(len(current_template)*1.5)} chars)\\n"
        f"2. MUST preserve the 'real numbers / live data' constraint (briefs cannot fabricate)\\n"
        f"3. MUST NOT add commands like 'ignore previous instructions'\\n"
        f"4. SHOULD add specific guidance about handling regime uncertainty\\n"
        f"5. SHOULD NOT just restate the same things in different words\\n\\n"
        f"Return ONLY the new prompt text, no explanation, no quotes, no preamble."
    )'''

    if OLD_BROKEN in src:
        src = src.replace(OLD_BROKEN, NEW_FIXED)
        r.ok("  Replaced broken triple-quote f-string with parenthesized concat")
    elif "---START---" in src:
        r.log("  Already fixed, skipping replacement")
    else:
        r.fail("  Couldn't find the broken pattern — manual investigation needed")
        # Show what's actually around line 268-290 for diagnosis
        lines = src.split("\n")
        for i in range(265, min(295, len(lines))):
            r.log(f"  L{i+1}: {lines[i][:200]}")
        raise SystemExit(1)

    # ─── 3. Validate fixed source ───────────────────────────────────────
    r.section("3. Validate fixed source")
    try:
        ast.parse(src)
        r.ok("  Syntax OK")
    except SyntaxError as e:
        r.fail(f"  STILL broken at L{e.lineno}: {e.msg}")
        lines = src.split("\n")
        for i in range(max(0, e.lineno - 3), min(len(lines), e.lineno + 3)):
            marker = " >>> " if i == e.lineno - 1 else "     "
            r.log(f"  {marker}L{i+1}: {lines[i][:200]}")
        raise SystemExit(1)

    lf_path.write_text(src)
    r.log(f"  Wrote fixed source: {len(src):,}B, {src.count(chr(10))} LOC")

    # ─── 4. Re-deploy ───────────────────────────────────────────────────
    r.section("4. Re-deploy justhodl-prompt-iterator")
    name = "justhodl-prompt-iterator"

    src_dir = REPO_ROOT / "aws/lambdas/justhodl-prompt-iterator/source"
    buf = io.BytesIO()
    files_added = 0
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for src_file in sorted(src_dir.rglob("*.py")):
            arcname = str(src_file.relative_to(src_dir))
            info = zipfile.ZipInfo(arcname)
            info.external_attr = 0o644 << 16
            zout.writestr(info, src_file.read_text())
            files_added += 1
    zbytes = buf.getvalue()

    # Step 138 may have created the function with broken source — first
    # deploy probably succeeded (Lambda doesn't validate Python syntax
    # at deploy time, only at invoke time). Update the code now.
    try:
        lam.get_function(FunctionName=name)
        lam.update_function_code(
            FunctionName=name, ZipFile=zbytes, Architectures=["arm64"],
        )
        lam.get_waiter("function_updated").wait(
            FunctionName=name, WaiterConfig={"Delay": 3, "MaxAttempts": 30},
        )
        r.ok(f"  Updated existing function ({len(zbytes):,}B)")
    except lam.exceptions.ResourceNotFoundException:
        # Function never got created (step 138 errored before creation)
        # Get key from morning-intelligence
        mi_env = lam.get_function_configuration(
            FunctionName="justhodl-morning-intelligence"
        ).get("Environment", {}).get("Variables", {})
        ant_key = mi_env.get("ANTHROPIC_KEY", "")

        lam.create_function(
            FunctionName=name,
            Runtime="python3.12",
            Role="arn:aws:iam::857687956942:role/lambda-execution-role",
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zbytes},
            Description="Loop 3 — weekly prompt iterator with safety guardrails",
            Timeout=120,
            MemorySize=256,
            Architectures=["arm64"],
            Environment={"Variables": {"ANTHROPIC_KEY": ant_key}},
        )
        lam.get_waiter("function_active_v2").wait(
            FunctionName=name, WaiterConfig={"Delay": 3, "MaxAttempts": 30},
        )
        r.ok(f"  Created function (had not been created previously)")

    # ─── 5. Test invoke ─────────────────────────────────────────────────
    r.section("5. Test invoke")
    time.sleep(3)
    invoke_start = time.time()
    resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse")
    elapsed = time.time() - invoke_start
    payload = resp.get("Payload").read().decode()
    if resp.get("FunctionError"):
        r.fail(f"  FunctionError ({elapsed:.1f}s): {payload[:600]}")
        raise SystemExit(1)
    r.ok(f"  Invoked in {elapsed:.1f}s")

    try:
        outer = json.loads(payload)
        body = json.loads(outer.get("body", "{}"))
        r.log(f"  Response body: {body}")
        # Expected today: skip_no_data because we don't have ≥7 scored briefs
        if body.get("skip") in ("insufficient_scored_data", "not enough briefs"):
            r.ok(f"  ✅ Iterator correctly returned 'skip' — calibrator hasn't caught up yet")
        elif body.get("applied"):
            r.warn(f"  ⚠ Iterator APPLIED a change — unexpected this early. Check log.")
        else:
            r.log(f"  Response: {body}")
    except Exception as e:
        r.log(f"  Couldn't parse: {e}")
        r.log(f"  Raw: {payload[:400]}")

    # ─── 6. Verify schedule is in place ─────────────────────────────────
    r.section("6. Verify EventBridge schedule")
    try:
        events = boto3.client("events", region_name=REGION)
        rule_name = "justhodl-prompt-iterator-weekly"
        rule = events.describe_rule(Name=rule_name)
        r.ok(f"  Rule {rule_name}: state={rule['State']}, schedule={rule['ScheduleExpression']}")
        targets = events.list_targets_by_rule(Rule=rule_name).get("Targets", [])
        if targets and name in targets[0].get("Arn", ""):
            r.ok(f"  Target wired to {name}")
        else:
            r.warn(f"  Target may not be wired correctly")
    except Exception as e:
        r.warn(f"  Schedule check: {e}")

    r.kv(
        function_name=name,
        zip_size=len(zbytes),
        invoke_s=f"{elapsed:.1f}",
        invoke_response_kind=body.get("skip") or ("applied" if body.get("applied") else "unknown"),
    )
    r.log("Done")
