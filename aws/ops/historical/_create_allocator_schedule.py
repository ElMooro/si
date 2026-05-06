"""Wire 6h EventBridge schedule for justhodl-allocator + verify data."""
import json
import time
import boto3
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-allocator"
RULE_NAME = "justhodl-allocator-6h"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    with report("create_allocator_schedule") as r:
        r.heading("Wire 6h schedule for justhodl-allocator")

        events.put_rule(
            Name=RULE_NAME,
            ScheduleExpression="rate(6 hours)",
            State="ENABLED",
            Description="Refresh cross-asset allocator every 6 hours",
        )
        fn_arn = lam.get_function(FunctionName=LAMBDA_NAME)["Configuration"]["FunctionArn"]
        events.put_targets(Rule=RULE_NAME, Targets=[{"Id": "1", "Arn": fn_arn}])
        try:
            lam.add_permission(
                FunctionName=LAMBDA_NAME,
                StatementId=f"{RULE_NAME}-invoke",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:857687956942:rule/{RULE_NAME}",
            )
        except lam.exceptions.ResourceConflictException:
            pass
        r.ok("  ✓ EventBridge rule wired")

        # Verify data/allocator.json contents
        r.heading("Allocator data shape")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/allocator.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  top-level keys: {list(d.keys())[:15]}")
            r.log(f"  generated_at: {d.get('generated_at')}")
            for top_key in ["regime", "regime_label", "regime_score", "summary", "weights", "allocations", "evidence"]:
                if top_key in d:
                    val = d[top_key]
                    if isinstance(val, dict):
                        r.log(f"  {top_key}: keys={list(val.keys())[:10]}")
                    elif isinstance(val, list):
                        r.log(f"  {top_key}: list[{len(val)}], first={str(val[0])[:120] if val else '—'}")
                    else:
                        r.log(f"  {top_key}: {str(val)[:200]}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
