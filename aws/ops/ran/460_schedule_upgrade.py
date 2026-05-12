#!/usr/bin/env python3
"""Step 460 — Upgrade news-sentiment EventBridge from daily to every 6h.

Current: cron(15 6 * * ? *)  → daily 6:15 UTC
New:     cron(15 */6 * * ? *) → every 6 hours at :15 (00:15, 06:15, 12:15, 18:15)

Cost: 4 runs/day × ~$0.05 = $0.20/day = $6/month for fresh signal.
Worth it given the screener integration delivers real bullish/bearish signals
that traders want fresh, not 24h stale.
"""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/460_schedule_upgrade.json"
NAME = "justhodl-tmp-460"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json
import boto3
events = boto3.client("events", region_name="us-east-1")

OLD_CRON = "cron(15 6 * * ? *)"
NEW_CRON = "cron(15 0/6 * * ? *)"  # AWS EventBridge syntax: 0/6 = every 6h starting at 0

def lambda_handler(event, context):
    out = {}
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
        out["before_err"] = str(e)[:200]
        return {"statusCode": 500, "body": json.dumps(out, default=str)}

    # Update schedule
    try:
        events.put_rule(
            Name=rule_name,
            ScheduleExpression=NEW_CRON,
            State="ENABLED",
            Description="JustHodl AI News Sentiment — every 6h at :15 (00:15, 06:15, 12:15, 18:15 UTC)")
        out["update_ok"] = True
    except Exception as e:
        out["update_err"] = str(e)[:300]
        return {"statusCode": 500, "body": json.dumps(out, default=str)}

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

    # List targets to confirm Lambda still wired
    try:
        targets = events.list_targets_by_rule(Rule=rule_name)
        out["targets"] = [{
            "id": t.get("Id"),
            "arn": t.get("Arn"),
        } for t in targets.get("Targets", [])]
    except Exception as e:
        out["targets_err"] = str(e)[:200]

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=256, Timeout=60, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    _time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed and parsed["body"] else parsed
    except Exception:
        out["raw"] = body[:10000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
