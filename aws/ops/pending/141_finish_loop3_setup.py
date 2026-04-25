#!/usr/bin/env python3
"""
Step 141 — Finish Loop 3 setup.

Step 138 errored at the syntax step — the post-deploy work to:
  - verify learning/prompt_templates.json exists with proper version
  - create the EventBridge schedule cron(0 10 ? * SUN *)
... never ran. Step 140 fixed the syntax + deployed the Lambda but
left those two items.

This step finishes both:
  1. Read learning/prompt_templates.json. If missing morning_brief,
     populate from the morning-intelligence Lambda's load_templates
     defaults (the Lambda has a fallback). Add _version=1 metadata.
  2. Create EventBridge rule cron(0 10 ? * SUN *) targeting the
     prompt-iterator. Wire IAM invoke permission.
  3. Sync invoke once more to confirm the iterator now finds the
     template and returns skip_no_data (correct: needs ≥7 scored
     briefs).
"""
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)

BUCKET = "justhodl-dashboard-live"
TEMPLATES_KEY = "learning/prompt_templates.json"


with report("finish_loop3_setup") as r:
    r.heading("Finish Loop 3 — template seeding + EventBridge schedule")

    # ─── 1. Verify learning/prompt_templates.json ──────────────────────
    r.section("1. Verify/initialize learning/prompt_templates.json")

    DEFAULT_MORNING_BRIEF = (
        "You are JustHodlAI, institutional-grade autonomous financial "
        "intelligence. Generate a morning Telegram brief max 380 words "
        "using the live data provided. Requirements: 1) Use REAL numbers "
        "only - no placeholders. 2) Cite specific data points (Khalid "
        "Index, VIX, regime, BTC price, plumbing stress). 3) Identify the "
        "top risk and top opportunity for today. 4) Provide one specific "
        "action recommendation. 5) End with a confidence note. Tone: "
        "concise, professional, actionable. Format: short paragraphs, "
        "concrete numbers, no waffle."
    )

    try:
        obj = s3.get_object(Bucket=BUCKET, Key=TEMPLATES_KEY)
        existing = json.loads(obj["Body"].read().decode("utf-8"))
        r.log(f"  templates.json exists: {obj['ContentLength']}B")
        r.log(f"  current keys: {sorted(existing.keys())}")
        needs_update = False
        if "morning_brief" not in existing:
            existing["morning_brief"] = DEFAULT_MORNING_BRIEF
            needs_update = True
            r.warn(f"  Missing morning_brief — adding default")
        if "_version" not in existing:
            existing["_version"] = 1
            existing["_initialized_at"] = datetime.now(timezone.utc).isoformat()
            needs_update = True
            r.log(f"  Adding _version=1 metadata")
        if needs_update:
            s3.put_object(
                Bucket=BUCKET, Key=TEMPLATES_KEY,
                Body=json.dumps(existing, indent=2).encode("utf-8"),
                ContentType="application/json",
            )
            r.ok(f"  Updated templates.json")
        else:
            r.log(f"  Template at v{existing.get('_version')}, no changes needed")
    except s3.exceptions.NoSuchKey:
        # Create from scratch
        templates = {
            "_version": 1,
            "_initialized_at": datetime.now(timezone.utc).isoformat(),
            "_note": "Loop 3 template store. Iterator at justhodl-prompt-iterator updates this weekly with safety guardrails.",
            "morning_brief": DEFAULT_MORNING_BRIEF,
            "improvement_writer": (
                "You are a quant analyst reviewing JustHodlAI prediction "
                "failures. Produce concise actionable improvements."
            ),
        }
        s3.put_object(
            Bucket=BUCKET, Key=TEMPLATES_KEY,
            Body=json.dumps(templates, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        r.ok(f"  Created templates.json with morning_brief seed + _version=1")

    # ─── 2. Create EventBridge schedule ─────────────────────────────────
    r.section("2. Create weekly EventBridge schedule")
    rule_name = "justhodl-prompt-iterator-weekly"
    function_name = "justhodl-prompt-iterator"
    function_arn = f"arn:aws:lambda:us-east-1:857687956942:function:{function_name}"

    try:
        existing_rule = events.describe_rule(Name=rule_name)
        r.log(f"  Rule already exists: {existing_rule['State']} {existing_rule.get('ScheduleExpression')}")
    except events.exceptions.ResourceNotFoundException:
        events.put_rule(
            Name=rule_name,
            ScheduleExpression="cron(0 10 ? * SUN *)",  # Sun 10:00 UTC
            State="ENABLED",
            Description="Loop 3 — weekly prompt iteration after calibrator (which runs at 09:00 UTC Sun)",
        )
        r.ok(f"  Created rule: cron(0 10 ? * SUN *)")

    events.put_targets(
        Rule=rule_name,
        Targets=[{"Id": "1", "Arn": function_arn}],
    )
    r.ok(f"  Targeted {function_name}")

    # IAM invoke permission for EventBridge
    try:
        lam.add_permission(
            FunctionName=function_name,
            StatementId=f"{rule_name}-invoke",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{rule_name}",
        )
        r.ok(f"  Added invoke permission for EventBridge")
    except lam.exceptions.ResourceConflictException:
        r.log(f"  Invoke permission already exists")

    # ─── 3. Re-invoke to confirm iterator finds the template ────────────
    r.section("3. Re-invoke to confirm template is now findable")
    time.sleep(2)
    invoke_start = time.time()
    resp = lam.invoke(FunctionName=function_name, InvocationType="RequestResponse")
    elapsed = time.time() - invoke_start
    payload = resp.get("Payload").read().decode()
    if resp.get("FunctionError"):
        r.fail(f"  FunctionError: {payload[:600]}")
        raise SystemExit(1)
    r.ok(f"  Invoked in {elapsed:.1f}s")

    try:
        outer = json.loads(payload)
        body = json.loads(outer.get("body", "{}"))
        r.log(f"  Response body: {body}")
        skip = body.get("skip")
        if skip == "no current template to iterate":
            r.fail(f"  ✗ Still can't find template — check S3 contents")
        elif skip in ("insufficient_scored_data", "not enough briefs"):
            r.ok(f"  ✅ Template found, iterator correctly waiting for scored data")
        elif body.get("applied"):
            r.warn(f"  ⚠ Iterator APPLIED a change unexpectedly today — investigate")
        else:
            r.log(f"  Other response: {body}")
    except Exception as e:
        r.warn(f"  Couldn't parse: {e}")
        r.log(f"  Raw: {payload[:400]}")

    # ─── 4. Final sanity: list all 3 new Lambdas + their schedules ──────
    r.section("4. Loop 2/3/4 Lambda inventory (final state)")
    for fname, schedule_rule in (
        ("justhodl-pnl-tracker", "justhodl-pnl-tracker-daily"),
        ("justhodl-prompt-iterator", "justhodl-prompt-iterator-weekly"),
        ("justhodl-watchlist-debate", "justhodl-watchlist-debate-nightly"),
    ):
        try:
            cfg = lam.get_function_configuration(FunctionName=fname)
            arch = cfg.get("Architectures", ["?"])[0]
            runtime = cfg.get("Runtime")
            mem = cfg.get("MemorySize")
            timeout = cfg.get("Timeout")
            r.log(f"  {fname}")
            r.log(f"    runtime={runtime}, arch={arch}, mem={mem}MB, timeout={timeout}s")
            try:
                rule = events.describe_rule(Name=schedule_rule)
                r.log(f"    schedule: {rule['ScheduleExpression']} ({rule['State']})")
            except events.exceptions.ResourceNotFoundException:
                r.warn(f"    schedule: ✗ rule not found")
        except lam.exceptions.ResourceNotFoundException:
            r.warn(f"  {fname}: function not found")

    r.kv(
        function_name=function_name,
        invoke_s=f"{elapsed:.1f}",
        invoke_response_kind=skip if skip else ("applied" if body.get("applied") else "other"),
        schedule="cron(0 10 ? * SUN *)",
    )
    r.log("Done")
