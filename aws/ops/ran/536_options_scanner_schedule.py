#!/usr/bin/env python3
"""536 — Add EventBridge schedule to options-flow-scanner (BUILD 13 activation)."""
import json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/536_options_scanner_schedule.json"
NAME = "justhodl-options-flow-scanner"
SCHEDULE = "cron(0 14,18 ? * MON-FRI *)"  # 2x daily during market hrs

lam = boto3.client("lambda", region_name="us-east-1")
eb = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    RULE = f"{NAME}-2xdaily"
    try:
        eb.put_rule(Name=RULE, ScheduleExpression=SCHEDULE, State="ENABLED",
                     Description="Options flow scanner 2x daily on market days")
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

    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/options-flow.json")
        body = obj["Body"].read()
        p = json.loads(body)
        s_ = p.get("summary") or {}
        st = p.get("stats") or {}
        out["sidecar"] = {
            "size_kb": round(len(body)/1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "schema_version": p.get("schema_version"),
            "method": p.get("method"),
            "stats": st,
            "n_tier_a_names": len(s_.get("tier_a") or []),
            "tier_a_top_15": (s_.get("tier_a") or [])[:15],
            "top_5_overall": [
                {"sym": x.get("symbol"), "score": x.get("score"), "tier": x.get("tier"),
                  "flags": x.get("flags"), "spot": x.get("spot"),
                  "cpr_change_pct": x.get("cpr_change_pct"),
                  "call_vol_surge": x.get("call_vol_surge"),
                  "short_pct_change": x.get("short_pct_change")}
                for x in (s_.get("top_25_overall") or [])[:5]
            ],
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
