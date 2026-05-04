"""Ensure justhodl-wave-signal-logger has a schedule + summarize Wave 3 status."""
import boto3
from ops_report import report

events = boto3.client("events", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
ddb = boto3.client("dynamodb", region_name="us-east-1")

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-wave-signal-logger"


def main():
    with report("schedule_logger_and_status") as r:
        r.heading("Existing schedules touching wave-signal-logger")
        rules = events.list_rule_names_by_target(TargetArn=lam.get_function(FunctionName=LAMBDA_NAME)["Configuration"]["FunctionArn"])
        existing = rules.get("RuleNames", [])
        r.log(f"  rules already wired: {existing}")

        if existing:
            for rn in existing:
                d = events.describe_rule(Name=rn)
                r.ok(f"  ✓ {rn:50s} {d.get('ScheduleExpression', '?'):20s} state={d.get('State')}")
        else:
            r.heading("Wiring rate(6 hours) schedule")
            rule_name = "justhodl-wave-signal-logger-6h"
            events.put_rule(
                Name=rule_name,
                ScheduleExpression="rate(6 hours)",
                State="ENABLED",
                Description="Log Wave 1+2 signals to DDB so Loop 1 scores them",
            )
            fn_arn = lam.get_function(FunctionName=LAMBDA_NAME)["Configuration"]["FunctionArn"]
            events.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": fn_arn}])
            try:
                lam.add_permission(
                    FunctionName=LAMBDA_NAME,
                    StatementId=f"{rule_name}-invoke",
                    Action="lambda:InvokeFunction",
                    Principal="events.amazonaws.com",
                    SourceArn=f"arn:aws:events:{REGION}:857687956942:rule/{rule_name}",
                )
            except lam.exceptions.ResourceConflictException:
                pass
            r.ok(f"  ✓ wired {rule_name}")

        # Show fresh DDB stats
        r.heading("DDB justhodl-signals — verify v2 items now present")
        from datetime import datetime, timezone, timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=15)).isoformat()
        d = boto3.resource("dynamodb", region_name=REGION).Table("justhodl-signals")
        from boto3.dynamodb.conditions import Attr
        resp = d.scan(
            FilterExpression=Attr("logged_at").gte(cutoff),
            Limit=200,
        )
        items = resp.get("Items", [])
        from collections import Counter
        by_src = Counter(i.get("source", "?") for i in items)
        by_type_v2 = Counter(i.get("signal_type") for i in items if i.get("source") == "wave-signal-logger-v2")
        r.log(f"  total in last 15 min: {len(items)}")
        r.log(f"  by source: {dict(by_src)}")
        r.log(f"  by signal_type (v2):")
        for t, n in by_type_v2.most_common():
            r.log(f"    {t:30s} n={n}")


if __name__ == "__main__":
    main()
