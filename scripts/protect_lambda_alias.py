#!/usr/bin/env python3
"""Pin existing scheduled production execution before replacing $LATEST.

Runs only in the GitHub Actions runner. It changes exact unqualified targets to
the last validated :live alias, preserving schedule state, input and options.
On a first rollout the alias starts at the pre-update numbered version.
"""
from __future__ import annotations

import json
import sys


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
        alias = lam.create_alias(FunctionName=function, Name="live", FunctionVersion=previous["Version"],
                                 Description="Pinned production before candidate replacement")
    result = {"function": function, "protected_version": alias["FunctionVersion"], "schedules": [], "rules": []}
    for group in pages(scheduler, "list_schedule_groups", "ScheduleGroups"):
        for summary in pages(scheduler, "list_schedules", "Schedules", GroupName=group["Name"]):
            if summary.get("Target", {}).get("Arn") not in (arn, arn + ":$LATEST"):
                continue
            current = scheduler.get_schedule(Name=summary["Name"], GroupName=group["Name"])
            allowed = {"Name", "GroupName", "ScheduleExpression", "StartDate", "EndDate", "Description", "ScheduleExpressionTimezone",
                       "State", "KmsKeyArn", "Target", "FlexibleTimeWindow", "ActionAfterCompletion"}
            update = {k: v for k, v in current.items() if k in allowed}
            update["Target"] = {**current["Target"], "Arn": live}
            scheduler.update_schedule(**update)
            result["schedules"].append(summary["Name"])
    for target in (arn, arn + ":$LATEST"):
        for rule in pages(events, "list_rule_names_by_target", "RuleNames", TargetArn=target):
            info = events.describe_rule(Name=rule)
            sid = "AuditLive-" + __import__("hashlib").sha256(info["Arn"].encode()).hexdigest()[:32]
            try:
                lam.add_permission(FunctionName=function, Qualifier="live", StatementId=sid, Action="lambda:InvokeFunction",
                                   Principal="events.amazonaws.com", SourceArn=info["Arn"])
            except lam.exceptions.ResourceConflictException:
                pass
            matched = []
            for row in pages(events, "list_targets_by_rule", "Targets", Rule=rule):
                if row.get("Arn") in (arn, arn + ":$LATEST"):
                    matched.append({**row, "Arn": live})
            if matched:
                response = events.put_targets(Rule=rule, Targets=matched)
                if response.get("FailedEntryCount"):
                    raise RuntimeError("Classic schedule alias migration failed")
                result["rules"].append(rule)
    return result


if __name__ == "__main__":
    import boto3
    function, region = sys.argv[1:3]
    print(json.dumps(protect(*(boto3.client(service, region_name=region) for service in ("lambda", "scheduler", "events")), function), sort_keys=True))
