"""Explicit reconciliation for the six reviewed brief-compiler schedules only.

The manifest supplies every setting. This allowlist limits the repair's scope;
it is not a second schedule generator. Missing classic rules are represented
as Scheduler schedules in the manifest before using this action. No classic
rule creation, broad enforcement, updates, or deletes occur here.
"""
import hashlib
import json

FUNCTION_ARN = "arn:aws:lambda:us-east-1:857687956942:function:justhodl-brief-compiler"
ROLE_ARN = "arn:aws:iam::857687956942:role/justhodl-brief-compiler-scheduler"
SPECS = {
    "brief-compiler-plumbing-6h": ("cron(20 */6 * * ? *)", "plumbing"),
    "brief-compiler-market-tape-6h": ("cron(25 */6 * * ? *)", "market_tape"),
    "brief-compiler-event-6h": ("cron(35 */6 * * ? *)", "event"),
    "brief-compiler-official-stats-daily": ("cron(40 1 * * ? *)", "official_stats"),
    "brief-compiler-positioning-daily": ("cron(50 1 * * ? *)", "positioning"),
    "brief-compiler-verdict-3h": ("cron(10 */3 * * ? *)", "verdict"),
}


def requests_from_manifest(manifest):
    if any(r.get("name") in SPECS for r in manifest.get("rules", [])):
        raise ValueError("Compiler declarations must be migrated to Scheduler first")
    rows = [s for s in manifest.get("schedules", []) if s.get("name") in SPECS]
    if len(rows) != 6 or {s["name"] for s in rows} != set(SPECS):
        raise ValueError("Exactly six unique compiler schedule declarations required")
    requests = []
    for s in rows:
        expr, mode = SPECS[s["name"]]
        targets = s.get("targets", [])
        if len(targets) != 1:
            raise ValueError("Exactly one compiler target required")
        t = targets[0]
        if (s.get("kind") != "scheduler" or s.get("group") != "default"
                or s.get("state") != "ENABLED" or s.get("expr") != expr
                or s.get("timezone") != "UTC" or s.get("flexible_time_window") != {"Mode": "OFF"}
                or t.get("arn") != FUNCTION_ARN or t.get("role_arn") != ROLE_ARN
                or t.get("path") is not None or json.loads(t.get("input", "null")) != {"mode": mode}
                or t.get("retry_policy") != {"MaximumEventAgeInSeconds": 3600, "MaximumRetryAttempts": 2}):
            raise ValueError("Compiler schedule differs from reviewed contract: " + s["name"])
        requests.append({
            "Name": s["name"], "GroupName": s["group"],
            "ScheduleExpression": s["expr"], "ScheduleExpressionTimezone": s["timezone"],
            "State": s["state"], "FlexibleTimeWindow": s["flexible_time_window"],
            "ActionAfterCompletion": "NONE",
            "Target": {"Arn": t["arn"], "RoleArn": t["role_arn"],
                       "Input": t["input"], "RetryPolicy": t["retry_policy"]},
        })
    return requests


def verify_schedule(actual, desired):
    for key in ("Name", "GroupName", "ScheduleExpression", "ScheduleExpressionTimezone",
                "State", "FlexibleTimeWindow", "ActionAfterCompletion"):
        if actual.get(key) != desired[key]:
            raise ValueError("Existing compiler schedule differs: %s.%s" % (desired["Name"], key))
    if actual.get("StartDate") or actual.get("EndDate"):
        raise ValueError("Existing compiler schedule has an unexpected date boundary")
    target = dict(actual.get("Target", {}))
    wanted = dict(desired["Target"])
    target["Input"] = json.loads(target.get("Input", "null"))
    wanted["Input"] = json.loads(wanted["Input"])
    if target != wanted:
        raise ValueError("Existing compiler target differs: " + desired["Name"])


def attach(manifest, scheduler, events):
    requests = requests_from_manifest(manifest)
    missing = []
    # Validate the entire batch before creating anything. Fail closed on an
    # unreadable rule or schedule; absence must be a ResourceNotFound response.
    for req in requests:
        try:
            events.describe_rule(Name=req["Name"])
        except events.exceptions.ResourceNotFoundException:
            pass
        else:
            raise ValueError("Classic compiler rule still exists: " + req["Name"])
        try:
            current = scheduler.get_schedule(Name=req["Name"], GroupName=req["GroupName"])
        except scheduler.exceptions.ResourceNotFoundException:
            missing.append(req)
        else:
            verify_schedule(current, req)
    created = []
    for req in missing:
        token = hashlib.sha256(json.dumps(req, sort_keys=True).encode()).hexdigest()
        scheduler.create_schedule(**req, ClientToken=token)
        created.append(req["Name"])
    verified = []
    for req in requests:
        current = scheduler.get_schedule(Name=req["Name"], GroupName=req["GroupName"])
        verify_schedule(current, req)
        verified.append({"name": req["Name"], "arn": current["Arn"],
                         "state": current["State"], "expr": current["ScheduleExpression"],
                         "input": json.loads(current["Target"]["Input"]),
                         "target": current["Target"]["Arn"]})
    return {"created": created, "verified": verified}
