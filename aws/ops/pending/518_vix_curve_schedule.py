#!/usr/bin/env python3
"""518 — Read full vix-curve sidecar contents + ADD EventBridge schedule (currently missing)."""
import json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/518_vix_curve_schedule.json"
NAME = "justhodl-vix-curve"
SCHEDULE = "cron(0,30 13-21 ? * MON-FRI *)"  # every 30 min during US market hours UTC

lam = boto3.client("lambda", region_name="us-east-1")
eb = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # ─── Read full sidecar so we can build the page from real fields ───
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/vix-curve.json")
        p = json.loads(obj["Body"].read())
        out["full_sidecar"] = p
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    # ─── Read history file structure ───
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/vix-curve-history.json")
        h = json.loads(obj["Body"].read())
        out["history_meta"] = {
            "n_days": h.get("n_days"),
            "first_date": h.get("first_date"),
            "last_date": h.get("last_date"),
            "series_keys": list((h.get("series") or {}).keys()),
            "spreads_keys": list((h.get("spreads") or {}).keys()),
            "sample_series_lengths": {k: len(v) if isinstance(v, list) else 0
                                         for k, v in (h.get("series") or {}).items()},
        }
    except Exception as e:
        out["history_err"] = str(e)[:300]

    # ─── Add EventBridge schedule ───
    RULE = f"{NAME}-30min"
    try:
        eb.put_rule(Name=RULE, ScheduleExpression=SCHEDULE,
                     State="ENABLED",
                     Description="VIX term structure refresh every 30min during US market hours")
        arn = lam.get_function(FunctionName=NAME)["Configuration"]["FunctionArn"]
        eb.put_targets(Rule=RULE, Targets=[{"Id": "1", "Arn": arn}])
        try:
            lam.add_permission(
                FunctionName=NAME, StatementId=f"{NAME}-eb-permit",
                Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                SourceArn=eb.describe_rule(Name=RULE)["Arn"],
            )
        except lam.exceptions.ResourceConflictException: pass
        out["schedule"] = SCHEDULE
        out["rule_state"] = "ENABLED"
    except Exception as e:
        out["schedule_err"] = str(e)[:300]

    # ─── Bump memory if needed ───
    try:
        lam.update_function_configuration(FunctionName=NAME, MemorySize=512, Timeout=120)
        out["mem_updated"] = "512MB/120s"
    except Exception as e:
        out["mem_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
