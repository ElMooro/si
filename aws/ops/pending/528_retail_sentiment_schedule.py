#!/usr/bin/env python3
"""528 — Add EventBridge schedule to justhodl-retail-sentiment (currently unscheduled)."""
import json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/528_retail_sentiment_schedule.json"
NAME = "justhodl-retail-sentiment"
SCHEDULE = "cron(0,30 * ? * * *)"  # every 30 min around the clock

lam = boto3.client("lambda", region_name="us-east-1")
eb = boto3.client("events", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    RULE = f"{NAME}-30min"
    try:
        eb.put_rule(Name=RULE, ScheduleExpression=SCHEDULE, State="ENABLED",
                     Description="Retail sentiment ApeWisdom+StockTwits refresh every 30 min")
        arn = lam.get_function(FunctionName=NAME)["Configuration"]["FunctionArn"]
        eb.put_targets(Rule=RULE, Targets=[{"Id": "1", "Arn": arn}])
        try:
            lam.add_permission(FunctionName=NAME, StatementId=f"{NAME}-eb-permit",
                Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                SourceArn=eb.describe_rule(Name=RULE)["Arn"])
        except lam.exceptions.ResourceConflictException: pass
        out["schedule"] = SCHEDULE
        out["rule_state"] = "ENABLED"
    except Exception as e:
        out["err"] = str(e)[:300]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
