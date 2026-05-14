#!/usr/bin/env python3
"""528 — Read full retail-sentiment sidecar + add EventBridge schedule."""
import json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/528_retail_sentiment_full.json"
NAME = "justhodl-retail-sentiment"
SCHEDULE = "cron(0,30 * ? * * *)"
lam = boto3.client("lambda", region_name="us-east-1")
eb = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Read full sidecar
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/retail-sentiment.json")
        p = json.loads(obj["Body"].read())
        out["sidecar_top_keys"] = list(p.keys())
        # Sample each top-level value
        sample = {}
        for k, v in p.items():
            if isinstance(v, list):
                sample[k] = {"_type": "list", "_len": len(v), "_head": v[:3] if v else []}
            elif isinstance(v, dict):
                sample[k] = {"_type": "dict", "_keys": list(v.keys())[:10], "_head_value": dict(list(v.items())[:3])}
            else:
                sample[k] = v
        out["sidecar_sample"] = sample
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    # Schedule
    RULE = f"{NAME}-30min"
    try:
        eb.put_rule(Name=RULE, ScheduleExpression=SCHEDULE, State="ENABLED",
                     Description="Retail sentiment refresh every 30 min")
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
        out["schedule_err"] = str(e)[:300]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
