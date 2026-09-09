#!/usr/bin/env python3
"""Normalize documented schedule aliases; never infer a binding from cadence."""
from __future__ import annotations

import copy
import json
import re
import sys


def expression(value):
    if not isinstance(value, str) or len(value) > 256:
        raise ValueError("schedule_expression_invalid")
    if value.startswith("cron(") and value.endswith(")") and len(value[5:-1].split()) == 6:
        return value
    if re.fullmatch(r"rate\([1-9][0-9]* (?:minute|minutes|hour|hours|day|days)\)", value):
        return value
    raise ValueError("schedule_expression_invalid")


def schedule_name(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", value):
        raise ValueError("schedule_name_invalid")
    return value


def aliases(spec, first, second):
    a, b = spec.get(first), spec.get(second)
    if a is not None and b is not None and a != b:
        raise ValueError("schedule_aliases_conflict")
    return a if a is not None else b


def normalize_config(config):
    if not isinstance(config, dict):
        raise ValueError("lambda_configuration_not_object")
    result = copy.deepcopy(config)
    scheduler = result.get("eventbridge_scheduler")
    if scheduler is not None:
        if not isinstance(scheduler, dict): raise ValueError("scheduler_schema_invalid")
        schedule_name(scheduler.get("schedule_name"))
        expression(scheduler.get("cron"))
        role = scheduler.get("role_arn")
        if not isinstance(role, str) or not re.fullmatch(r"arn:aws:iam::[0-9]{12}:role/[A-Za-z0-9+=,.@_/-]+", role):
            raise ValueError("scheduler_role_required")
    raw = result.get("schedule")
    if raw is None or raw is False:
        result.pop("schedule", None)
        return result
    if isinstance(raw, str):
        cadence = expression(raw)
        # Legacy strings express cadence but do not authorize creating, naming
        # or rewriting a binding. Keep all live state/input/targets untouched.
        result.pop("schedule")
        result["release_schedule_note"] = {"status": "CONFIG_CADENCE_ONLY", "configured_expression": cadence,
                                           "binding_action": "PRESERVE_EXISTING"}
        return result
    if not isinstance(raw, dict):
        raise ValueError("schedule_schema_invalid")
    cadence = expression(aliases(raw, "cron", "expression"))
    if raw.get("scheduler_name") is not None:
        if raw.get("rule_name") is not None or raw.get("name") is not None:
            raise ValueError("schedule_service_identity_conflict")
        name = schedule_name(raw["scheduler_name"])
        # This is AWS Scheduler, not a classic EventBridge rule. A complete
        # eventbridge_scheduler block remains independently managed; the old
        # abbreviated reference alone cannot invent role/input/state settings.
        result.pop("schedule")
        result["release_schedule_note"] = {"status": "EXISTING_SCHEDULER_REFERENCE", "schedule_name": name,
                                           "configured_expression": cadence, "binding_action": "PRESERVE_EXISTING"}
        return result
    name = schedule_name(aliases(raw, "rule_name", "name"))
    description = raw.get("description", "Scheduled run")
    if not isinstance(description, str) or len(description) > 512:
        raise ValueError("schedule_description_invalid")
    result["schedule"] = {**raw, "rule_name": name, "cron": cadence, "description": description}
    result["release_schedule_note"] = {"status": "MANAGED_CLASSIC_RULE", "rule_name": name,
                                       "configured_expression": cadence}
    return result


if __name__ == "__main__":
    path = sys.argv[1]
    try:
        with open(path) as stream:
            result = normalize_config(json.load(stream))
    except (ValueError, TypeError):
        raise SystemExit("Invalid schedule configuration; request details withheld") from None
    print(json.dumps(result, ensure_ascii=False, allow_nan=False))
