#!/usr/bin/env python3
"""Pin existing scheduled production execution before replacing $LATEST.

Runs only in the GitHub Actions runner. It changes exact unqualified targets to
the last validated :live alias, preserving schedule state, input and options.
On a first rollout the alias starts at the pre-update numbered version.
"""
from __future__ import annotations

import json
import sys


def function_identity(config):
    # Publishing a version / changing the resource policy can advance AWS's
    # revision metadata without changing the function being protected. Keep
    # every configuration field and the code hash in the comparison.
    return {key: value for key, value in config.items()
            if key not in {"ResponseMetadata", "RevisionId", "LastModified"}}


def alias_identity(alias):
    return {key: value for key, value in alias.items()
            if key not in {"ResponseMetadata", "RevisionId"}}


def checked_function(lam, function, expected, *, owned_mutation=False):
    current = lam.get_function_configuration(FunctionName=function)
    if (current.get("State") != "Active" or current.get("LastUpdateStatus") != "Successful"
            or not current.get("RevisionId") or function_identity(current) != function_identity(expected)
            or (not owned_mutation and current["RevisionId"] != expected.get("RevisionId"))):
        raise RuntimeError("Function changed during target protection")
    return current


def checked_alias(lam, function, expected, *, owned_mutation=False):
    current = lam.get_alias(FunctionName=function, Name="live")
    if (not current.get("RevisionId") or alias_identity(current) != alias_identity(expected)
            or (not owned_mutation and current["RevisionId"] != expected.get("RevisionId"))):
        raise RuntimeError("Production alias changed during target protection")
    return current


def pages(client, method, result, **kwargs):
    while True:
        response = getattr(client, method)(**kwargs)
        yield from response.get(result, [])
        token = response.get("NextToken")
        if not token:
            break
        kwargs["NextToken"] = token


def protect(lam, scheduler, events, function):
    config = lam.get_function_configuration(FunctionName=function)
    if config.get("State") != "Active" or config.get("LastUpdateStatus") != "Successful":
        raise RuntimeError("Existing function is not in a stable state")
    arn = config["FunctionArn"]
    live = arn + ":live"
    try:
        alias = lam.get_alias(FunctionName=function, Name="live")
    except lam.exceptions.ResourceNotFoundException:
        previous = lam.publish_version(FunctionName=function, RevisionId=config["RevisionId"], CodeSha256=config["CodeSha256"])
        if not str(previous.get("Version", "")).isdigit() or int(previous["Version"]) < 1:
            raise RuntimeError("Cannot protect production with an unnumbered version")
        config = checked_function(lam, function, config, owned_mutation=True)
        alias = lam.create_alias(FunctionName=function, Name="live", FunctionVersion=previous["Version"],
                                 Description="Pinned production before candidate replacement")
        # Rebase only across an operation we performed, and only after checking
        # that no code/configuration/alias routing changed in the same window.
        config = checked_function(lam, function, config, owned_mutation=True)
        alias = checked_alias(lam, function, alias, owned_mutation=True)
    result = {"function": function, "protected_version": alias["FunctionVersion"], "revision_id": config["RevisionId"],
              "schedules": [], "rules": []}
    for group in pages(scheduler, "list_schedule_groups", "ScheduleGroups"):
        for summary in pages(scheduler, "list_schedules", "Schedules", GroupName=group["Name"]):
            if summary.get("Target", {}).get("Arn") not in (arn, arn + ":$LATEST"):
                continue
            current = scheduler.get_schedule(Name=summary["Name"], GroupName=group["Name"])
            # List summaries may be stale. Never reassign a schedule that an
            # operator has moved to another target since the enumeration.
            if current.get("Target", {}).get("Arn") not in (arn, arn + ":$LATEST"):
                continue
            allowed = {"Name", "GroupName", "ScheduleExpression", "StartDate", "EndDate", "Description", "ScheduleExpressionTimezone",
                       "State", "KmsKeyArn", "Target", "FlexibleTimeWindow", "ActionAfterCompletion"}
            update = {k: v for k, v in current.items() if k in allowed}
            update["Target"] = {**current["Target"], "Arn": live}
            scheduler.update_schedule(**update)
            result["schedules"].append(summary["Name"])
    for bus in pages(events, "list_event_buses", "EventBuses"):
        for target in (arn, arn + ":$LATEST"):
            for rule in pages(events, "list_rule_names_by_target", "RuleNames", TargetArn=target, EventBusName=bus["Name"]):
                info = events.describe_rule(Name=rule, EventBusName=bus["Name"])
                sid = "AuditLive-" + __import__("hashlib").sha256(info["Arn"].encode()).hexdigest()[:32]
                try:
                    lam.add_permission(FunctionName=function, Qualifier="live", StatementId=sid, Action="lambda:InvokeFunction",
                                       Principal="events.amazonaws.com", SourceArn=info["Arn"])
                except lam.exceptions.ResourceConflictException:
                    policy = json.loads(lam.get_policy(FunctionName=function, Qualifier="live")["Policy"])
                    expected = [statement for statement in policy.get("Statement", []) if statement.get("Sid") == sid]
                    if len(expected) != 1 or expected[0].get("Effect") != "Allow" or expected[0].get("Principal") != {"Service": "events.amazonaws.com"} or expected[0].get("Action") != "lambda:InvokeFunction" or expected[0].get("Resource") != live or expected[0].get("Condition", {}).get("ArnLike", {}).get("AWS:SourceArn") != info["Arn"]:
                        raise RuntimeError("Existing alias invoke permission does not match the scheduled rule") from None
                else:
                    config = checked_function(lam, function, config, owned_mutation=True)
                    alias = checked_alias(lam, function, alias, owned_mutation=True)
                matched = []
                for row in pages(events, "list_targets_by_rule", "Targets", Rule=rule, EventBusName=bus["Name"]):
                    if row.get("Arn") in (arn, arn + ":$LATEST"):
                        matched.append({**row, "Arn": live})
                for offset in range(0, len(matched), 10):
                    response = events.put_targets(Rule=rule, EventBusName=bus["Name"], Targets=matched[offset:offset + 10])
                    if response.get("FailedEntryCount"):
                        raise RuntimeError("Classic schedule alias migration failed")
                if matched:
                    result["rules"].append(f'{bus["Name"]}/{rule}')
    # Abort before code staging if a concurrent edit changed either pin.
    checked_alias(lam, function, alias)
    latest = checked_function(lam, function, config)
    # The next update-function-code uses this exact, current revision as CAS.
    result["revision_id"] = latest["RevisionId"]
    return result


if __name__ == "__main__":
    import boto3
    function, region = sys.argv[1:3]
    print(json.dumps({"phase": "protecting_production", "function": function}), file=sys.stderr, flush=True)
    print(json.dumps(protect(*(boto3.client(service, region_name=region) for service in ("lambda", "scheduler", "events")), function), sort_keys=True))
