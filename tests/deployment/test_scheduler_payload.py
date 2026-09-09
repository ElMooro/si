"""Scheduler's replacement API preserves the full existing execution contract."""
import copy
import runpy
from pathlib import Path

build = runpy.run_path(str(Path(__file__).resolve().parents[2] / "scripts/scheduler_payload.py"))["scheduler_payload"]
ARN = "arn:aws:lambda:us-east-1:123:function:test:live"
CONFIG = {"eventbridge_scheduler": {"schedule_name": "daily", "group_name": "research", "cron": "rate(15 minutes)", "role_arn": "role"}}
CURRENT = {"Name": "daily", "GroupName": "research", "State": "DISABLED", "ScheduleExpression": "rate(1 hour)",
           "StartDate": "2026-01-01", "EndDate": "2027-01-01", "KmsKeyArn": "kms", "ActionAfterCompletion": "DELETE",
           "FlexibleTimeWindow": {"Mode": "FLEXIBLE", "MaximumWindowInMinutes": 5}, "Description": "existing",
           "Target": {"Arn": ARN, "RoleArn": "role", "Input": '{"mode":"refresh"}', "RetryPolicy": {"MaximumRetryAttempts": 8}, "DeadLetterConfig": {"Arn": "dlq"}},
           "Arn": "read-only-schedule-arn", "CreationDate": "yesterday"}


def test_existing_schedule_keeps_all_options_and_input():
    result = build(CONFIG, CURRENT, ARN)
    expected = {key: value for key, value in CURRENT.items() if key not in {"Arn", "CreationDate"}}
    expected["ScheduleExpression"] = "rate(15 minutes)"
    assert result == expected
    assert CURRENT["ScheduleExpression"] == "rate(1 hour)"


def test_explicit_input_and_timezone_override_without_clearing_other_options():
    config = copy.deepcopy(CONFIG)
    config["eventbridge_scheduler"].update(input={"mode": "permission_refresh"}, timezone="America/New_York")
    result = build(config, CURRENT, ARN)
    assert result["Target"]["Input"] == '{"mode":"permission_refresh"}'
    assert result["ScheduleExpressionTimezone"] == "America/New_York" and result["State"] == "DISABLED"


def test_unrelated_schedule_is_not_reassigned():
    current = copy.deepcopy(CURRENT)
    current["Target"]["Arn"] = "unrelated-function"
    try: build(CONFIG, current, ARN)
    except ValueError: pass
    else: raise AssertionError("Unrelated schedule was overwritten")


def test_new_schedule_has_qualified_target_and_configured_group():
    result = build(CONFIG, {}, ARN)
    assert result["GroupName"] == "research" and result["Target"]["Arn"] == ARN
    assert result["Target"]["Input"] == "{}" and result["State"] == "ENABLED"
