#!/usr/bin/env python3
"""
Step 164 — Patch calibrator to filter legacy outcomes.

Step 163 tagged 4,410 legacy outcomes with is_legacy=true. They\\'ll
auto-purge in 30 days via TTL, but until then they\\'re still in the
table. The calibrator currently scans with:

  Attr(\"correct\").exists()

which matches the 4,410 legacy records too because they have a
correct attribute (just with value None). We need to:

  1. Filter to outcomes where correct is True or False (not None)
  2. AND where is_legacy is NOT True

This is defense in depth — even if compute_accuracy_stats correctly
ignores correct=None records (it does — n_correct + n_wrong only
count True/False), the scan still pulls all 4,410 records into
memory and into the n_total count for stats reporting.

After this patch, calibrator only loads valid scored outcomes.

Also patches reports-builder if it has the same pattern.
"""
import io
import os
import time
import zipfile
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


with report("filter_legacy_in_calibrator") as r:
    r.heading("Patch calibrator to filter is_legacy + correct=None")

    # ─── 1. Patch calibrator ───────────────────────────────────────────
    r.section("1. Patch justhodl-calibrator")
    cal_path = REPO_ROOT / "aws/lambdas/justhodl-calibrator/source/lambda_function.py"
    src = cal_path.read_text()
    r.log(f"  Source: {len(src):,}B")

    OLD_FILTER = '''    all_outcomes = scan_all(
        OUTCOMES_TABLE,
        Attr("correct").exists()
    )'''

    NEW_FILTER = '''    # Filter: correct must be True or False (excludes correct=None / legacy
    # records from pre-baseline-fix era — see step 163, 2026-04-25). Also
    # explicitly exclude is_legacy=true tagged records as defense in depth
    # (they\\'ll auto-purge via TTL ~30 days from tagging).
    all_outcomes = scan_all(
        OUTCOMES_TABLE,
        (Attr("correct").eq(True) | Attr("correct").eq(False)) &
        Attr("is_legacy").ne(True)
    )'''

    if OLD_FILTER in src:
        src = src.replace(OLD_FILTER, NEW_FILTER)
        r.ok(f"  Patched calibrator filter")
    elif "is_legacy" in src and "Attr" in src:
        r.log(f"  Already patched")
    else:
        r.fail(f"  Couldn\\'t find anchor")
        raise SystemExit(1)

    # Verify syntax
    import ast
    try:
        ast.parse(src)
        r.ok(f"  Syntax OK")
    except SyntaxError as e:
        r.fail(f"  Syntax: {e}")
        raise SystemExit(1)

    cal_path.write_text(src)

    # ─── 2. Patch reports-builder if it scans outcomes ──────────────────
    r.section("2. Check + patch reports-builder")
    rb_path = REPO_ROOT / "aws/lambdas/justhodl-reports-builder/source/lambda_function.py"
    rb_src = rb_path.read_text()

    # Find scan(s) of outcomes
    if "OUTCOMES_TABLE" in rb_src and "scan(" in rb_src:
        # Different filter patterns possible; let me see what we need to change
        r.log(f"  reports-builder reads outcomes table; checking filter pattern")
        # Look for any FilterExpression involving correct or is_legacy
        if "is_legacy" in rb_src:
            r.log(f"  Already filtered for is_legacy")
        elif "Attr(\"correct\")" in rb_src:
            r.log(f"  Has correct filter but not is_legacy — patching")
            # Generic patch: anywhere Attr("correct").exists() shows up
            OLD_RB = 'Attr("correct").exists()'
            NEW_RB = '(Attr("correct").eq(True) | Attr("correct").eq(False)) & Attr("is_legacy").ne(True)'
            rb_src = rb_src.replace(OLD_RB, NEW_RB)
            try:
                ast.parse(rb_src)
                rb_path.write_text(rb_src)
                r.ok(f"  Patched reports-builder")
            except SyntaxError as e:
                r.warn(f"  reports-builder syntax broke after patch: {e}")
        else:
            r.log(f"  No correct-attribute filter found — leaving unchanged")
    else:
        r.log(f"  reports-builder doesn\\'t scan OUTCOMES_TABLE directly")

    # ─── 3. Deploy calibrator ───────────────────────────────────────────
    r.section("3. Deploy patched calibrator")
    src_dir = REPO_ROOT / "aws/lambdas/justhodl-calibrator/source"
    buf = io.BytesIO()
    files_added = 0
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for f in sorted(src_dir.rglob("*.py")):
            arcname = str(f.relative_to(src_dir))
            info = zipfile.ZipInfo(arcname)
            info.external_attr = 0o644 << 16
            zout.writestr(info, f.read_text())
            files_added += 1
    zbytes = buf.getvalue()
    lam.update_function_code(
        FunctionName="justhodl-calibrator", ZipFile=zbytes, Architectures=["arm64"],
    )
    lam.get_waiter("function_updated").wait(
        FunctionName="justhodl-calibrator",
        WaiterConfig={"Delay": 3, "MaxAttempts": 30},
    )
    r.ok(f"  Deployed calibrator ({len(zbytes):,}B, {files_added} files)")

    # ─── 4. Test invoke calibrator ──────────────────────────────────────
    r.section("4. Test invoke")
    time.sleep(3)
    invoke_start = time.time()
    resp = lam.invoke(
        FunctionName="justhodl-calibrator", InvocationType="RequestResponse",
    )
    elapsed = time.time() - invoke_start
    payload = resp.get("Payload").read().decode()
    if resp.get("FunctionError"):
        r.fail(f"  FunctionError ({elapsed:.1f}s): {payload[:600]}")
        raise SystemExit(1)
    r.ok(f"  Invoked in {elapsed:.1f}s")
    r.log(f"  Response: {payload[:500]}")

    r.kv(
        zip_size=len(zbytes),
        invoke_s=f"{elapsed:.1f}",
    )
    r.log("Done")
