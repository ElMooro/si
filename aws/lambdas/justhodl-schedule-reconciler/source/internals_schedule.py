"""Create/verify only the reviewed daily internals Scheduler declaration."""
import hashlib
import json
from brief_schedules import FUNCTION_ARN, ROLE_ARN, verify_schedule

NAME = "brief-compiler-internals-daily"
EXPR = "cron(20 22 * * ? *)"


def request_from_manifest(manifest):
    if any(x.get("name") == NAME for x in manifest.get("rules", [])):
        raise ValueError("Internals must be declared as Scheduler, not a classic rule")
    rows = [x for x in manifest.get("schedules", []) if x.get("name") == NAME]
    if len(rows) != 1:
        raise ValueError("Exactly one internals schedule declaration required")
    s = rows[0]
    ts = s.get("targets", [])
    if len(ts) != 1:
        raise ValueError("Exactly one internals target required")
    t = ts[0]
    retry = {"MaximumEventAgeInSeconds": 3600, "MaximumRetryAttempts": 2}
    if (s.get("kind") != "scheduler" or s.get("group") != "default"
            or s.get("state") != "ENABLED" or s.get("expr") != EXPR
            or s.get("timezone") != "UTC" or s.get("flexible_time_window") != {"Mode": "OFF"}
            or t.get("arn") != FUNCTION_ARN or t.get("role_arn") != ROLE_ARN
            or t.get("path") is not None or json.loads(t.get("input", "null")) != {"mode": "internals"}
            or t.get("retry_policy") != retry):
        raise ValueError("Internals declaration differs from reviewed contract")
    return {"Name": NAME, "GroupName": s["group"], "ScheduleExpression": s["expr"],
            "ScheduleExpressionTimezone": s["timezone"], "State": s["state"],
            "FlexibleTimeWindow": s["flexible_time_window"], "ActionAfterCompletion": "NONE",
            "Target": {"Arn": t["arn"], "RoleArn": t["role_arn"], "Input": t["input"], "RetryPolicy": retry}}


def attach(manifest, scheduler, events):
    req = request_from_manifest(manifest)
    try:
        events.describe_rule(Name=NAME)
    except events.exceptions.ResourceNotFoundException:
        pass
    else:
        raise ValueError("Classic internals rule exists; refusing duplicate")
    created = []
    try:
        live = scheduler.get_schedule(Name=NAME, GroupName="default")
    except scheduler.exceptions.ResourceNotFoundException:
        scheduler.create_schedule(**req, ClientToken=hashlib.sha256(json.dumps(req, sort_keys=True).encode()).hexdigest())
        created.append(NAME)
        live = scheduler.get_schedule(Name=NAME, GroupName="default")
    verify_schedule(live, req)
    return {"created": created, "verified": [{"name": NAME, "arn": live["Arn"],
            "state": live["State"], "expr": live["ScheduleExpression"],
            "input": json.loads(live["Target"]["Input"]), "target": live["Target"]["Arn"]}]}
