#!/usr/bin/env python3
"""Step 218 — emergency: EventBridge points at broken new Lambda.

Step 217 cut over the EventBridge rule before the new Lambda's
test-invoke succeeded. The new Lambda has a syntax error because
my regex only matched single-line s3.put_object() calls but the
old Lambda has multi-line ones, so the duplicate insert went
in the wrong place.

This step:
  1. PRIORITY: revert EventBridge target to OLD Lambda so daily
     refresh keeps working overnight
  2. Download new Lambda's broken source for inspection
  3. Re-fetch old Lambda's source
  4. Apply a smarter dual-write patch that handles multi-line calls
  5. Validate patched source with ast.parse
  6. update-function-code on new Lambda
  7. Test-invoke new Lambda
  8. If success, optionally cut EventBridge BACK to new Lambda
"""
import io, json, time, urllib.request, zipfile, re, ast
from datetime import datetime, timezone
from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
OLD = "justhodl-khalid-metrics"
NEW = "justhodl-ka-metrics"
RULE = "justhodl-khalid-metrics-refresh"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def patch_source_v2(src):
    """Better dual-write patch. Use AST to find the whole s3.put_object
    call (which may span lines), then replace 'data/khalid-' with
    'data/ka-' in the Key string and emit a duplicate call."""
    tree = ast.parse(src)
    # Find every Call node where func is `s3.put_object`
    edits = []  # list of (start_lineno, end_lineno, original_text, ka_replacement_text)
    src_lines = src.splitlines(keepends=True)

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        # Match s3.put_object(...)
        f = node.func
        if not (isinstance(f, ast.Attribute) and f.attr == "put_object"):
            continue
        if not (isinstance(f.value, ast.Name) and f.value.id == "s3"):
            continue
        # Look for Key=... keyword arg with constant string starting with 'data/khalid-'
        khalid_key = None
        for kw in node.keywords:
            if kw.arg == "Key" and isinstance(kw.value, ast.Constant) \
               and isinstance(kw.value.value, str) and kw.value.value.startswith("data/khalid-"):
                khalid_key = kw.value.value
        if not khalid_key:
            continue

        # Capture full source span using ast lineno info
        start_line = node.lineno  # 1-indexed
        end_line = node.end_lineno
        # extract original call text
        original = "".join(src_lines[start_line - 1:end_line])
        ka_replacement = original.replace(khalid_key, khalid_key.replace("khalid-", "ka-"))
        edits.append((start_line, end_line, original, ka_replacement))

    if not edits:
        return src, 0

    # Apply edits in reverse order so line numbers don't shift
    edits.sort(key=lambda e: e[0], reverse=True)
    new_lines = list(src_lines)
    for start, end, original, ka_replacement in edits:
        # Find the indentation of the original call to match it
        indent_match = re.match(r"\s*", new_lines[start - 1])
        indent = indent_match.group(0) if indent_match else ""
        # Insert the duplicate call after the original
        # Strip leading whitespace from ka_replacement and apply same indent
        # to first line; preserve other lines as-is
        ka_lines = ka_replacement.splitlines(keepends=True)
        if ka_lines:
            ka_lines[0] = indent + ka_lines[0].lstrip()
        # Make sure original ends with a newline before duplicate
        if not new_lines[end - 1].endswith("\n"):
            new_lines[end - 1] = new_lines[end - 1] + "\n"
        # Insert the duplicate after end_line
        new_lines = new_lines[:end] + ka_lines + (["\n"] if not ka_lines[-1].endswith("\n") else []) + new_lines[end:]

    return "".join(new_lines), len(edits)


with report("phase4_emergency_fix") as r:
    r.heading("Phase 4 emergency — revert EventBridge + fix new Lambda")

    # 1. Revert EventBridge to OLD immediately
    r.section("1. Revert EventBridge target to OLD Lambda (priority — keep system working)")
    old_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{OLD}"
    try:
        targets = events.list_targets_by_rule(Rule=RULE).get("Targets", [])
        current = [t["Arn"].split(":")[-1] for t in targets]
        r.log(f"  current: {current}")
        if OLD in current:
            r.log(f"  ✅ already pointed at {OLD}")
        else:
            new_targets = [{**t, "Arn": old_arn} for t in targets]
            resp = events.put_targets(Rule=RULE, Targets=new_targets)
            failed = resp.get("FailedEntryCount", 0)
            if failed:
                r.warn(f"  ⚠ {failed} updates failed: {resp.get('FailedEntries')}")
            else:
                r.log(f"  ✅ reverted to {OLD} — daily refresh restored")
            verify = [t["Arn"].split(":")[-1] for t in events.list_targets_by_rule(Rule=RULE).get("Targets", [])]
            r.log(f"  verified: {verify}")
    except Exception as e:
        r.warn(f"  ⚠ revert: {e}")

    # 2. Download original (clean) source from OLD Lambda
    r.section("2. Download fresh OLD source for re-patching")
    old_info = lam.get_function(FunctionName=OLD)
    with urllib.request.urlopen(old_info["Code"]["Location"], timeout=30) as resp:
        old_zip = resp.read()
    with zipfile.ZipFile(io.BytesIO(old_zip)) as zf:
        src = zf.read("lambda_function.py").decode("utf-8", errors="replace")
    r.log(f"  source: {len(src.splitlines())} lines, {len(src)}B")

    # 3. Patch with v2 patcher (AST-based)
    r.section("3. Apply v2 dual-write patch (AST-based)")
    src_patched, n_edits = patch_source_v2(src)
    r.log(f"  edited {n_edits} put_object call sites")

    # 4. Validate patched source
    r.section("4. Validate patched source compiles")
    try:
        ast.parse(src_patched)
        r.log(f"  ✅ ast.parse OK ({len(src_patched.splitlines())} lines, {len(src_patched)}B)")
        valid = True
    except SyntaxError as e:
        r.warn(f"  ✗ SyntaxError: {e}")
        # Show context around error
        if e.lineno:
            ctx = src_patched.splitlines()[max(0, e.lineno - 3):e.lineno + 2]
            for i, ln in enumerate(ctx):
                r.log(f"    line {e.lineno - 2 + i}: {ln}")
        valid = False

    if valid:
        # Sanity: should now have 6 put_object calls (3 original + 3 ka)
        n_khalid = src_patched.count("'data/khalid-")
        n_ka = src_patched.count("'data/ka-")
        r.log(f"  khalid keys: {n_khalid}  ka keys: {n_ka}")

        # 5. Update new Lambda's code
        r.section(f"5. Update {NEW} with corrected source")
        new_zip_buf = io.BytesIO()
        with zipfile.ZipFile(new_zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("lambda_function.py", src_patched)
        lam.update_function_code(FunctionName=NEW, ZipFile=new_zip_buf.getvalue())
        time.sleep(3)
        lam.get_waiter("function_updated_v2").wait(FunctionName=NEW)
        r.log(f"  ✅ {NEW} updated")

        # 6. Test-invoke
        r.section(f"6. Test-invoke {NEW}")
        t0 = time.time()
        inv = lam.invoke(FunctionName=NEW, InvocationType="RequestResponse",
                         Payload=json.dumps({}))
        elapsed = time.time() - t0
        err = inv.get("FunctionError")
        payload = inv["Payload"].read().decode("utf-8", errors="replace")[:500]
        if err:
            r.warn(f"  ✗ err={err} ({elapsed:.1f}s)")
            r.warn(f"  payload: {payload}")
            invoke_ok = False
        else:
            r.log(f"  ✅ OK ({elapsed:.1f}s)")
            r.log(f"  payload: {payload}")
            invoke_ok = True

        time.sleep(8)

        # 7. Verify dual-write
        if invoke_ok:
            r.section("7. Verify dual-write")
            keys = [
                "data/ka-metrics.json", "data/ka-config.json", "data/ka-analysis.json",
                "data/khalid-metrics.json", "data/khalid-config.json", "data/khalid-analysis.json",
            ]
            now = datetime.now(timezone.utc)
            n_fresh = 0
            for k in keys:
                try:
                    obj = s3.get_object(Bucket=BUCKET, Key=k)
                    age = (now - obj["LastModified"]).total_seconds()
                    mark = "✅ FRESH" if age < 60 else "⏰ stale"
                    r.log(f"  {mark} {k:40s}  size={obj['ContentLength']:>10}B  age={age:.0f}s")
                    if age < 120: n_fresh += 1
                except ClientError as e:
                    if "NoSuchKey" in str(e):
                        r.warn(f"  ✗ MISSING {k}")
                    else:
                        r.warn(f"  ✗ {k}: {e}")
            r.log(f"\n  {n_fresh}/{len(keys)} fresh")

            # 8. If everything's good, re-cutover EventBridge
            if n_fresh >= 5:
                r.section(f"8. Re-cut EventBridge → {NEW}")
                new_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{NEW}"
                try:
                    targets = events.list_targets_by_rule(Rule=RULE).get("Targets", [])
                    new_targets = [{**t, "Arn": new_arn} for t in targets]
                    resp = events.put_targets(Rule=RULE, Targets=new_targets)
                    failed = resp.get("FailedEntryCount", 0)
                    if failed:
                        r.warn(f"  ⚠ {failed} failed: {resp.get('FailedEntries')}")
                    else:
                        r.log(f"  ✅ EventBridge → {NEW}")
                    verify = [t["Arn"].split(":")[-1] for t in events.list_targets_by_rule(Rule=RULE).get("Targets", [])]
                    r.log(f"  verified: {verify}")
                except Exception as e:
                    r.warn(f"  ⚠ {e}")
            else:
                r.warn(f"  Only {n_fresh}/6 fresh — EventBridge NOT cut over yet")
                r.warn(f"  Investigate why fewer ka_+ keys are fresh than expected")

    r.section("FINAL")
    r.log(f"  Old: {OLD}")
    r.log(f"  New: {NEW}")
    r.log(f"  Function URL: https://s6ascg5dntry5w5elqedee77na0fcljz.lambda-url.us-east-1.on.aws/")
    r.log("Done")
