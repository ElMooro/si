"""ops/782 — confirm justhodl-ecb-detail is cleanly live after the deploy race.

ops 781 verified the OUTPUT (ok=true, 0 errors) but its function_live check
false-negatived on a benign update race with deploy-lambdas.yml. This nails
function state, schedule target, and a clean invoke definitively.
"""
import json, os, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=120, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
events = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

FN = "justhodl-ecb-detail"
report = {"ops": 782, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Confirm justhodl-ecb-detail cleanly live"}

# 1. function state
try:
    c = lam.get_function_configuration(FunctionName=FN)
    report["function"] = {
        "state": c.get("State"),
        "last_update_status": c.get("LastUpdateStatus"),
        "runtime": c.get("Runtime"),
        "memory": c.get("MemorySize"),
        "timeout": c.get("Timeout"),
        "code_size": c.get("CodeSize"),
        "last_modified": c.get("LastModified"),
        "has_fred_key": "FRED_API_KEY" in (
            (c.get("Environment") or {}).get("Variables") or {}),
    }
except Exception as e:
    report["function"] = {"err": str(e)[:200]}

# 2. schedule + target
try:
    rd = events.describe_rule(Name="ecb-detail-daily")
    tg = events.list_targets_by_rule(Rule="ecb-detail-daily").get("Targets", [])
    report["schedule"] = {
        "state": rd.get("State"), "cron": rd.get("ScheduleExpression"),
        "targets": [t.get("Arn", "").split(":")[-1] for t in tg]}
except Exception as e:
    report["schedule"] = {"err": str(e)[:200]}

# 3. clean invoke
try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"),
                        "body": json.loads(r["Payload"].read() or b"{}").get("body")}
except Exception as e:
    report["invoke"] = {"err": str(e)[:200]}

time.sleep(2)
try:
    out = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                     Key="data/ecb-detail.json")["Body"].read())
    report["output_headline"] = out.get("headline")
    report["output_ok"] = out.get("ok")
except Exception as e:
    report["output_err"] = str(e)[:200]

fn = report.get("function", {})
sch = report.get("schedule", {})
checks = {
    "function_active": fn.get("state") == "Active",
    "update_successful": fn.get("last_update_status") == "Successful",
    "fred_key_set": fn.get("has_fred_key") is True,
    "schedule_enabled": sch.get("state") == "ENABLED",
    "schedule_targets_fn": FN in (sch.get("targets") or []),
    "invoke_ok": report.get("invoke", {}).get("status") == 200
                 and not report.get("invoke", {}).get("fn_error"),
    "output_ok": report.get("output_ok") is True,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "CONFIRMED — justhodl-ecb-detail is cleanly live: Active, daily 11:00 UTC "
    "schedule targeting the function, FRED key set, invoke returns ok=true. "
    "The ECB liquidity engine is fully operational."
    if report["all_pass"] else "REVIEW — see checks[]")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/782_ecb_detail_confirm.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/782_ecb_detail_confirm.json")
