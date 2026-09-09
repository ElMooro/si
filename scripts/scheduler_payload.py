#!/usr/bin/env python3
"""Build a complete Scheduler replacement without dropping existing options."""
from __future__ import annotations

import copy
import json
import sys


WRITABLE = {
    "Name", "GroupName", "ScheduleExpression", "StartDate", "EndDate", "Description",
    "ScheduleExpressionTimezone", "State", "KmsKeyArn", "Target", "FlexibleTimeWindow",
    "ActionAfterCompletion",
}


def scheduler_payload(config, current, live_arn):
    spec = config["eventbridge_scheduler"]
    name = spec["schedule_name"]
    group = spec.get("group_name", "default")
    if not live_arn.endswith(":live"):
        raise ValueError("Scheduler candidate must target the stable live alias")
    if current:
        if current.get("Name") != name or current.get("GroupName", "default") != group:
            raise ValueError("Scheduler snapshot identity differs from configured schedule")
        base_arn = live_arn[:-5]
        if current.get("Target", {}).get("Arn") not in {base_arn, base_arn + ":$LATEST", live_arn}:
            raise ValueError("Refusing to overwrite an unrelated schedule target")
        result = {key: copy.deepcopy(value) for key, value in current.items() if key in WRITABLE}
    else:
        result = {"Name": name, "GroupName": group, "State": "ENABLED",
                  "FlexibleTimeWindow": {"Mode": "OFF"}, "ScheduleExpressionTimezone": "UTC",
                  "Target": {"Input": "{}", "RetryPolicy": {"MaximumRetryAttempts": 2,
                             "MaximumEventAgeInSeconds": 3600}}}
    result["ScheduleExpression"] = spec["cron"]
    result["Target"]["Arn"] = live_arn
    result["Target"]["RoleArn"] = spec["role_arn"]
    for source, target in (("timezone", "ScheduleExpressionTimezone"), ("description", "Description"),
                           ("state", "State"), ("flexible_time_window", "FlexibleTimeWindow"),
                           ("start_date", "StartDate"), ("end_date", "EndDate"),
                           ("kms_key_arn", "KmsKeyArn"), ("action_after_completion", "ActionAfterCompletion")):
        if source in spec:
            result[target] = copy.deepcopy(spec[source])
    for source, target in (("retry_policy", "RetryPolicy"), ("dead_letter_config", "DeadLetterConfig")):
        if source in spec:
            result["Target"][target] = copy.deepcopy(spec[source])
    if "input" in spec:
        value = spec["input"]
        result["Target"]["Input"] = value if isinstance(value, str) else json.dumps(value, separators=(",", ":"))
    return result


if __name__ == "__main__":
    config_path, current_path, live_arn = sys.argv[1:]
    with open(config_path) as handle:
        config = json.load(handle)
    with open(current_path) as handle:
        current = json.load(handle)
    try:
        print(json.dumps(scheduler_payload(config, current, live_arn)))
    except (ValueError, KeyError, TypeError):
        # Schedule Input can contain secrets. Never print its contents on errors.
        raise SystemExit("Invalid or unrelated Scheduler configuration; request withheld")
