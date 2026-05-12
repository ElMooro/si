#!/usr/bin/env python3
"""Step 460b — Run boto3 events directly from workflow.

The github-actions-justhodl IAM user (which the workflow assumes) has
events:PutRule permission because deploy-lambdas creates rules during
Lambda deployment. The lambda-execution-role used by temp Lambdas does
not. So we skip the temp Lambda pattern here and just call boto3 directly.
"""
import json
import os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/460b_schedule_upgrade_direct.json"

NEW_CRON = "cron(15 0/6 * * ? *)"  # every 6h at :15 (00:15, 06:15, 12:15, 18:15 UTC)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    events = boto3.client("events", region_name="us-east-1")

    rule_name = "justhodl-sentiment-daily"

    # Read current state
    try:
        cur = events.describe_rule(Name=rule_name)
        out["before"] = {
            "name": cur.get("Name"),
            "schedule": cur.get("ScheduleExpression"),
            "state": cur.get("State"),
            "description": cur.get("Description"),
        }
    except Exception as e:
        out["before_err"] = str(e)[:300]
        with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)
        return

    # Update schedule
    try:
        events.put_rule(
            Name=rule_name,
            ScheduleExpression=NEW_CRON,
            State="ENABLED",
            Description="JustHodl AI News Sentiment v2 — every 6h at :15 UTC (FMP backend, fresh signal 4x/day)",
        )
        out["update_ok"] = True
    except Exception as e:
        out["update_err"] = str(e)[:500]
        with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)
        return

    # Read after
    try:
        after = events.describe_rule(Name=rule_name)
        out["after"] = {
            "name": after.get("Name"),
            "schedule": after.get("ScheduleExpression"),
            "state": after.get("State"),
            "description": after.get("Description"),
        }
    except Exception as e:
        out["after_err"] = str(e)[:200]

    # Confirm targets still attached
    try:
        targets = events.list_targets_by_rule(Rule=rule_name)
        out["targets"] = [{"id": t.get("Id"), "arn": t.get("Arn")}
                          for t in targets.get("Targets", [])]
    except Exception as e:
        out["targets_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
