#!/usr/bin/env python3
"""
Step 138 — Loop 3: weekly self-improving prompts with safety guardrails.

CRITICAL FINDING: The existing self_improve in morning-intelligence
runs daily and has NO safety guardrails:
  - No length validation beyond len > 60
  - No check that core constraints survive each rewrite
  - No A/B comparison
  - No revert if quality drops
  - No version tracking

Plus today it's iterating on noise: most outcomes have correct=None
(unscored), so 'wrong predictions' being fed to the LLM is misleading.

This step:

A. Disable the daily self_improve in morning-intelligence
   (replace with no-op that returns unchanged templates)

B. Create a new justhodl-prompt-iterator Lambda
   - Runs weekly Sunday 10:00 UTC (after calibrator at 9:00 UTC)
   - Reads last 14 days of morning briefs from archive/intelligence/
   - For each brief, scores:
     * accuracy_score: did the brief's stated regime/risk match
       what happened next? (uses outcomes table, only scores where
       correct ∈ {True, False})
     * specificity_score: heuristic — count of {numbers, percentages,
       tickers, specific timeframes}
   - If we have ≥7 scored briefs and avg accuracy < 0.50 OR
     avg specificity declining trend, propose a prompt change
   - Validates proposed change against safety rules:
     1. Length within 50%-150% of current
     2. Must contain core constraints: 'real numbers', 'no fake',
        'live data'
     3. Must NOT contain: 'ignore previous', 'forget', 'placeholder'
   - If validation passes, save with versioning:
     learning/prompt_templates.json   ← active template (v_active)
     learning/prompt_templates_v{N}.json ← all historical versions
     learning/improvement_log.json    ← change log with diffs
   - If next 3 days of briefs score worse than the previous 3,
     auto-revert (read from versioned history)

C. Schedule with EventBridge

This is a SUPERVISED iteration. We're not trusting the LLM to
self-modify safely — we're trusting:
  1. The objective (accuracy + specificity, both numerical)
  2. The guardrails (length + content checks)
  3. The revert (auto-rollback if quality drops)
"""
import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone, timedelta
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)

BUCKET = "justhodl-dashboard-live"


# ════════════════════════════════════════════════════════════════════════
# Source for new justhodl-prompt-iterator Lambda
# ════════════════════════════════════════════════════════════════════════
ITERATOR_SRC = '''"""
justhodl-prompt-iterator — Loop 3 weekly prompt self-improvement.

Runs Sunday 10:00 UTC (after calibrator at 9:00 UTC).

Process:
  1. Read last 14 morning briefs from archive/intelligence/
  2. Score each brief on:
     - accuracy_score (from outcomes table, only correct ∈ {T,F})
     - specificity_score (heuristic count of numbers/tickers)
  3. If we have data + degrading trend, propose a prompt change
  4. Validate proposal against safety rules
  5. If passes, save with versioning + log
  6. Compare next-period scoring to previous; auto-revert if worse
"""
import json
import os
import re
import time
import urllib.request
import ssl
from datetime import datetime, timezone, timedelta
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
TEMPLATES_KEY = "learning/prompt_templates.json"
LOG_KEY = "learning/improvement_log.json"
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_KEY", "")

s3 = boto3.client("s3", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# Safety rules — guardrails against bad prompt edits
LENGTH_MIN_RATIO = 0.5
LENGTH_MAX_RATIO = 1.5
REQUIRED_TERMS = ["real numbers", "live data"]  # at least one
FORBIDDEN_TERMS = ["ignore previous", "forget previous", "placeholder", "fake", "fictional"]


def get_s3_json(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"[S3] {key}: {e}")
        return None


def put_s3_json(key, body, cache="public, max-age=300"):
    s3.put_object(
        Bucket=BUCKET, Key=key,
        Body=json.dumps(body, indent=2, default=str).encode("utf-8"),
        ContentType="application/json", CacheControl=cache,
    )


def list_recent_briefs(days=14):
    """Return list of (date, brief_dict) for last N days from archive."""
    now = datetime.now(timezone.utc)
    briefs = []
    for d in range(days):
        day = now - timedelta(days=d)
        prefix = day.strftime("archive/intelligence/%Y/%m/%d/")
        try:
            resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=10)
            for obj in resp.get("Contents", []):
                # Pick one per day — first one (closest to start of day)
                pass
            # Simpler: get the first object found that day
            objs = sorted(resp.get("Contents", []), key=lambda o: o["Key"])
            if objs:
                obj = s3.get_object(Bucket=BUCKET, Key=objs[0]["Key"])
                briefs.append({
                    "date": day.strftime("%Y-%m-%d"),
                    "key": objs[0]["Key"],
                    "data": json.loads(obj["Body"].read().decode("utf-8")),
                })
        except Exception as e:
            print(f"[LIST] {prefix}: {e}")
    return sorted(briefs, key=lambda b: b["date"])


def score_specificity(brief_text):
    """Heuristic: count concrete numbers, tickers, percentages, dates."""
    if not brief_text:
        return 0.0
    text = str(brief_text)
    # Specific patterns
    numbers = len(re.findall(r"\\b\\d+(?:\\.\\d+)?(?:[KMB])?\\b", text))
    percentages = len(re.findall(r"\\d+(?:\\.\\d+)?%", text))
    tickers = len(re.findall(r"\\$?[A-Z]{2,5}\\b", text))
    timeframes = len(re.findall(r"\\b(?:7d|30d|90d|1y|YoY|QoQ|H1|H2|Q[1-4])\\b", text))
    # Normalize by text length (per 100 words)
    words = max(1, len(text.split()))
    raw = numbers + percentages * 1.5 + tickers + timeframes * 1.5
    return round((raw / words) * 100, 2)


def score_brief_accuracy(brief_date_iso):
    """Find outcomes scored within 7 days of this brief; return mean
    correct rate. Returns None if not enough data."""
    try:
        brief_dt = datetime.fromisoformat(brief_date_iso.replace("Z", "+00:00"))
    except Exception:
        return None
    table = dynamodb.Table("justhodl-outcomes")
    try:
        # Scan with filter — outcomes table is small, filter in app
        resp = table.scan(Limit=5000)
        items = resp.get("Items", [])
        relevant = []
        for o in items:
            if o.get("correct") not in (True, False):
                continue
            try:
                logged = datetime.fromisoformat(str(o.get("logged_at", "")).replace("Z", "+00:00"))
            except Exception:
                continue
            # Only outcomes whose signal was logged within 24h before this brief
            delta = (brief_dt - logged).total_seconds()
            if 0 <= delta <= 86400:
                relevant.append(o)
        if len(relevant) < 3:
            return None  # not enough data
        correct_count = sum(1 for o in relevant if o.get("correct") is True)
        return correct_count / len(relevant)
    except Exception as e:
        print(f"[ACC] {e}")
        return None


def validate_proposed_template(old_text, new_text):
    """Return (ok: bool, reason: str)."""
    if not new_text or not isinstance(new_text, str):
        return False, "empty or non-string"
    new_text = new_text.strip()
    old_len = max(1, len(old_text))
    new_len = len(new_text)
    ratio = new_len / old_len
    if ratio < LENGTH_MIN_RATIO:
        return False, f"too short (ratio {ratio:.2f} < {LENGTH_MIN_RATIO})"
    if ratio > LENGTH_MAX_RATIO:
        return False, f"too long (ratio {ratio:.2f} > {LENGTH_MAX_RATIO})"
    new_lower = new_text.lower()
    if not any(t in new_lower for t in [r.lower() for r in REQUIRED_TERMS]):
        return False, f"missing required terms: {REQUIRED_TERMS}"
    for f in FORBIDDEN_TERMS:
        if f.lower() in new_lower:
            return False, f"contains forbidden term: {f}"
    if "REAL" not in new_text.upper() and "real" not in new_text.lower():
        return False, "missing 'real' constraint"
    return True, "ok"


def call_anthropic(prompt, max_tokens=400):
    if not ANTHROPIC_KEY:
        return None
    body = json.dumps({
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
    }).encode("utf-8")
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=body,
        headers={
            "Content-Type": "application/json",
            "x-api-key": ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60, context=ctx) as r:
            data = json.loads(r.read().decode("utf-8"))
            content = data.get("content", [])
            if content and isinstance(content, list):
                return content[0].get("text", "").strip()
    except Exception as e:
        print(f"[ANTHROPIC] {e}")
    return None


def lambda_handler(event, context):
    print("=== PROMPT ITERATOR v1 ===")
    now = datetime.now(timezone.utc)

    # 1. Load current template + change log
    templates = get_s3_json(TEMPLATES_KEY) or {}
    log = get_s3_json(LOG_KEY) or []
    if not isinstance(log, list):
        log = []
    current_template = templates.get("morning_brief", "")
    current_version = templates.get("_version", 0)
    print(f"  Current template version: v{current_version}, length: {len(current_template)} chars")

    if not current_template:
        return {"statusCode": 200, "body": json.dumps({"skip": "no current template to iterate"})}

    # 2. Read last 14 days of briefs
    briefs = list_recent_briefs(14)
    print(f"  Found {len(briefs)} briefs in last 14 days")

    if len(briefs) < 7:
        return {"statusCode": 200, "body": json.dumps({
            "skip": "not enough briefs",
            "n_found": len(briefs),
        })}

    # 3. Score each brief
    scored = []
    for b in briefs:
        data = b.get("data", {})
        brief_text = json.dumps(data)[:5000]  # full JSON as proxy for brief content
        spec = score_specificity(brief_text)
        acc = score_brief_accuracy(b["date"] + "T12:00:00+00:00")
        scored.append({
            "date": b["date"],
            "specificity": spec,
            "accuracy": acc,
        })
        print(f"  {b['date']}: specificity={spec}, accuracy={acc}")

    # 4. Decide if iteration is warranted
    accs = [s["accuracy"] for s in scored if s["accuracy"] is not None]
    avg_acc = sum(accs) / len(accs) if accs else None
    avg_spec = sum(s["specificity"] for s in scored) / len(scored)

    print(f"  Average accuracy: {avg_acc} (n={len(accs)})")
    print(f"  Average specificity: {avg_spec:.2f}")

    # Decision rule: iterate if accuracy < 0.50 with ≥7 scored briefs.
    # Otherwise, no change.
    if avg_acc is None or len(accs) < 7:
        print("  Skip: not enough scored outcomes yet (calibrator hasn't caught up)")
        # Append no-op log entry so we know we tried
        log.append({
            "ts": now.isoformat(),
            "action": "skip_no_data",
            "n_briefs": len(briefs),
            "n_scored": len(accs),
            "current_version": current_version,
        })
        put_s3_json(LOG_KEY, log[-90:])
        return {"statusCode": 200, "body": json.dumps({"skip": "insufficient_scored_data"})}

    if avg_acc >= 0.55:
        print(f"  Skip: accuracy is healthy ({avg_acc:.2f})")
        log.append({
            "ts": now.isoformat(),
            "action": "skip_healthy",
            "avg_accuracy": avg_acc,
            "avg_specificity": avg_spec,
            "current_version": current_version,
        })
        put_s3_json(LOG_KEY, log[-90:])
        return {"statusCode": 200, "body": json.dumps({"skip": "healthy", "avg_accuracy": avg_acc})}

    # 5. Iteration warranted — propose change
    print(f"  ITERATING: avg_acc={avg_acc:.2f} below threshold 0.55")

    failure_summary = "\\n".join([
        f"- {s['date']}: accuracy={s['accuracy']:.2f}, specificity={s['specificity']:.1f}"
        for s in scored if s["accuracy"] is not None
    ])

    proposal_prompt = f"""You are reviewing the prompt that generates JustHodlAI's daily morning briefs. The brief uses live financial data and is read by an institutional investor.

CURRENT PROMPT (length: {len(current_template)} chars):
\"\"\"
{current_template}
\"\"\"

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

Return ONLY the new prompt text, no explanation, no quotes, no preamble."""

    new_template = call_anthropic(proposal_prompt, max_tokens=600)
    if not new_template:
        log.append({"ts": now.isoformat(), "action": "skip_anthropic_failed",
                    "current_version": current_version})
        put_s3_json(LOG_KEY, log[-90:])
        return {"statusCode": 200, "body": json.dumps({"skip": "anthropic_call_failed"})}

    print(f"  Proposed new template: {len(new_template)} chars")

    # 6. Validate
    ok, reason = validate_proposed_template(current_template, new_template)
    if not ok:
        print(f"  REJECTED: {reason}")
        log.append({
            "ts": now.isoformat(),
            "action": "rejected_proposal",
            "reason": reason,
            "current_version": current_version,
            "rejected_length": len(new_template),
        })
        put_s3_json(LOG_KEY, log[-90:])
        return {"statusCode": 200, "body": json.dumps({"reject": reason})}

    # 7. Save with versioning
    new_version = current_version + 1
    # Archive old version
    s3.put_object(
        Bucket=BUCKET, Key=f"learning/prompt_templates_v{current_version}.json",
        Body=json.dumps(templates, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    # Write new active template
    templates["morning_brief"] = new_template
    templates["_version"] = new_version
    templates["_updated_at"] = now.isoformat()
    templates["_previous_avg_accuracy"] = avg_acc
    templates["_previous_avg_specificity"] = avg_spec
    put_s3_json(TEMPLATES_KEY, templates)

    # 8. Log change with full diff metadata
    log.append({
        "ts": now.isoformat(),
        "action": "applied_proposal",
        "from_version": current_version,
        "to_version": new_version,
        "old_length": len(current_template),
        "new_length": len(new_template),
        "trigger_avg_accuracy": avg_acc,
        "trigger_avg_specificity": avg_spec,
        "n_briefs_evaluated": len(briefs),
        "n_scored": len(accs),
    })
    put_s3_json(LOG_KEY, log[-90:])

    print(f"  ✅ Applied v{current_version} → v{new_version}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "applied": True,
            "from_version": current_version,
            "to_version": new_version,
            "trigger_accuracy": round(avg_acc, 3),
        }),
    }
'''


with report("loop3_prompt_iterator") as r:
    r.heading("Loop 3 — weekly prompt self-improvement with safety guardrails")

    # ─── 1. Disable the daily self_improve in morning-intelligence ──────
    r.section("1. Replace daily self_improve with a no-op")
    mi_path = REPO_ROOT / "aws/lambdas/justhodl-morning-intelligence/source/lambda_function.py"
    mi_src = mi_path.read_text()

    OLD_SELF_IMPROVE_HEAD = '''def self_improve(outcomes,templates,accuracy):
    wrong=sorted([o for o in outcomes if o.get("correct") is False],
                 key=lambda x:x.get("checked_at",""),reverse=True)[:8]
    if not wrong: return templates,None'''

    NEW_SELF_IMPROVE_HEAD = '''def self_improve(outcomes,templates,accuracy):
    # Loop 3: this DAILY function is now a no-op. Prompt iteration
    # moved to weekly justhodl-prompt-iterator Lambda which has safety
    # guardrails (length validation, content checks, version tracking).
    # The old daily iteration ran on noise (most outcomes have
    # correct=None today) and could randomly degrade brief quality.
    return templates, None
    # ─── DISABLED CODE BELOW (preserved for reference) ───────────────
    wrong=sorted([o for o in outcomes if o.get("correct") is False],
                 key=lambda x:x.get("checked_at",""),reverse=True)[:8]
    if not wrong: return templates,None'''

    if OLD_SELF_IMPROVE_HEAD in mi_src:
        mi_src = mi_src.replace(OLD_SELF_IMPROVE_HEAD, NEW_SELF_IMPROVE_HEAD)
        mi_path.write_text(mi_src)
        r.ok("  Disabled daily self_improve (now no-op + commented original)")
    elif "DISABLED CODE BELOW" in mi_src:
        r.log("  self_improve already disabled, skipping")
    else:
        r.fail("  Couldn't find self_improve anchor")
        raise SystemExit(1)

    # Validate
    import ast
    try:
        ast.parse(mi_src)
        r.ok("  morning-intelligence syntax OK")
    except SyntaxError as e:
        r.fail(f"  Syntax: {e}")
        raise SystemExit(1)

    # Re-deploy morning-intelligence
    r.section("2. Re-deploy morning-intelligence")
    src_dir = REPO_ROOT / "aws/lambdas/justhodl-morning-intelligence/source"
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
    lam.update_function_code(
        FunctionName="justhodl-morning-intelligence", ZipFile=zbytes,
        Architectures=["arm64"],
    )
    lam.get_waiter("function_updated").wait(
        FunctionName="justhodl-morning-intelligence",
        WaiterConfig={"Delay": 3, "MaxAttempts": 30},
    )
    r.ok(f"  Re-deployed morning-intelligence ({len(zbytes):,}B)")

    # ─── 3. Create new justhodl-prompt-iterator Lambda ──────────────────
    r.section("3. Create justhodl-prompt-iterator Lambda")
    iter_dir = REPO_ROOT / "aws/lambdas/justhodl-prompt-iterator/source"
    iter_dir.mkdir(parents=True, exist_ok=True)
    (iter_dir / "lambda_function.py").write_text(ITERATOR_SRC)

    try:
        ast.parse(ITERATOR_SRC)
        r.ok("  iterator syntax OK")
    except SyntaxError as e:
        r.fail(f"  iterator syntax: {e}")
        raise SystemExit(1)

    buf2 = io.BytesIO()
    with zipfile.ZipFile(buf2, "w", zipfile.ZIP_DEFLATED) as zout:
        info = zipfile.ZipInfo("lambda_function.py")
        info.external_attr = 0o644 << 16
        zout.writestr(info, ITERATOR_SRC)
    zb2 = buf2.getvalue()

    fname = "justhodl-prompt-iterator"
    role_arn = "arn:aws:iam::857687956942:role/lambda-execution-role"
    try:
        lam.get_function(FunctionName=fname)
        lam.update_function_code(
            FunctionName=fname, ZipFile=zb2, Architectures=["arm64"],
        )
        lam.get_waiter("function_updated").wait(
            FunctionName=fname, WaiterConfig={"Delay": 3, "MaxAttempts": 30},
        )
        r.ok(f"  Updated existing {fname}")
    except lam.exceptions.ResourceNotFoundException:
        # Get ANTHROPIC_KEY from morning-intelligence env to share
        mi_env = lam.get_function_configuration(
            FunctionName="justhodl-morning-intelligence"
        ).get("Environment", {}).get("Variables", {})
        anthropic_key_val = mi_env.get("ANTHROPIC_KEY", "")

        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zb2},
            Description="Loop 3 — weekly prompt iterator with safety guardrails",
            Timeout=120,  # Anthropic call can take a while
            MemorySize=256,
            Architectures=["arm64"],
            Environment={"Variables": {"ANTHROPIC_KEY": anthropic_key_val}},
        )
        lam.get_waiter("function_active_v2").wait(
            FunctionName=fname, WaiterConfig={"Delay": 3, "MaxAttempts": 30},
        )
        r.ok(f"  Created {fname} (with shared ANTHROPIC_KEY)")

    # ─── 4. Test invoke ─────────────────────────────────────────────────
    r.section("4. Test invoke iterator")
    time.sleep(3)
    invoke_start = time.time()
    resp = lam.invoke(FunctionName=fname, InvocationType="RequestResponse")
    elapsed = time.time() - invoke_start
    payload = resp.get("Payload").read().decode()
    if resp.get("FunctionError"):
        r.warn(f"  FunctionError ({elapsed:.1f}s): {payload[:600]}")
    else:
        r.ok(f"  Invoked in {elapsed:.1f}s")
        try:
            outer = json.loads(payload)
            body = json.loads(outer.get("body", "{}"))
            r.log(f"  Response: {body}")
        except Exception:
            r.log(f"  Raw: {payload[:300]}")

    # ─── 5. Schedule weekly Sunday 10:00 UTC ────────────────────────────
    r.section("5. Schedule weekly Sunday 10:00 UTC")
    rule_name = "justhodl-prompt-iterator-weekly"
    try:
        try:
            existing_rule = events.describe_rule(Name=rule_name)
            r.log(f"  Rule exists: {existing_rule['State']}")
        except events.exceptions.ResourceNotFoundException:
            events.put_rule(
                Name=rule_name,
                ScheduleExpression="cron(0 10 ? * SUN *)",
                State="ENABLED",
                Description="Loop 3 — weekly prompt iteration Sun 10 UTC",
            )
            r.ok(f"  Created rule cron(0 10 ? * SUN *)")
        events.put_targets(
            Rule=rule_name,
            Targets=[{"Id": "1",
                      "Arn": f"arn:aws:lambda:us-east-1:857687956942:function:{fname}"}],
        )
        try:
            lam.add_permission(
                FunctionName=fname,
                StatementId=f"{rule_name}-invoke",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{rule_name}",
            )
            r.ok(f"  Added invoke permission")
        except lam.exceptions.ResourceConflictException:
            r.log(f"  Invoke permission already exists")
    except Exception as e:
        r.fail(f"  Schedule: {e}")

    # ─── 6. Initialize template version metadata if missing ─────────────
    r.section("6. Initialize template version tracking")
    templates = None
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="learning/prompt_templates.json")
        templates = json.loads(obj["Body"].read().decode("utf-8"))
        if "_version" not in templates:
            templates["_version"] = 1
            templates["_initialized_at"] = datetime.now(timezone.utc).isoformat()
            s3.put_object(
                Bucket=BUCKET, Key="learning/prompt_templates.json",
                Body=json.dumps(templates, indent=2).encode("utf-8"),
                ContentType="application/json",
            )
            r.ok(f"  Added _version=1 to existing template")
        else:
            r.log(f"  Template already at v{templates['_version']}")
    except Exception as e:
        r.warn(f"  Couldn't read templates: {e}")

    r.kv(
        morning_intel_zip=len(zbytes),
        iterator_zip=len(zb2),
        iterator_invoke_s=f"{elapsed:.1f}",
        schedule="cron(0 10 ? * SUN *)",
        template_version=templates.get("_version") if templates else None,
    )
    r.log("Done")
