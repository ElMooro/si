"""ops/768 — verify justhodl-activity-nowcast (roadmap item #3).

Confirms the Real-Time Activity Nowcast deployed, runs clean on real FRED
data, and produces a valid activity index + regime + divergence block.
"""
import json, os
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
events = boto3.client("events", region_name="us-east-1", config=cfg)

report = {"ops": 768, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Real-Time Activity Nowcast deploy verify (roadmap #3)"}

try:
    fc = lam.get_function_configuration(FunctionName="justhodl-activity-nowcast")
    report["function"] = {"exists": True, "runtime": fc.get("Runtime"),
                          "last_modified": fc.get("LastModified"),
                          "timeout": fc.get("Timeout"), "memory": fc.get("MemorySize")}
except Exception as e:
    report["function"] = {"exists": False, "err": str(e)[:200]}

try:
    r = events.describe_rule(Name="activity-nowcast-daily")
    report["schedule"] = {"exists": True, "state": r.get("State"),
                          "cron": r.get("ScheduleExpression")}
except Exception as e:
    report["schedule"] = {"exists": False, "err": str(e)[:160]}

try:
    r = lam.invoke(FunctionName="justhodl-activity-nowcast",
                   InvocationType="RequestResponse", Payload=b"{}")
    payload = json.loads(r["Payload"].read() or b"{}")
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"),
                        "body": payload.get("body")}
except Exception as e:
    report["invoke"] = {"err": str(e)[:240]}

an = {}
try:
    an = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                  Key="data/activity-nowcast.json")["Body"].read())
except Exception as e:
    report["read_err"] = str(e)[:200]

comps = an.get("components", []) or []
report["output"] = {
    "ok": an.get("ok"), "activity_index": an.get("activity_index"),
    "activity_z": an.get("activity_z"), "regime": an.get("regime"),
    "momentum": an.get("momentum"), "headline": an.get("headline"),
    "n_ok": an.get("n_ok"), "n_series": an.get("n_series"),
    "errors": an.get("errors"),
}
report["divergence"] = an.get("divergence")
report["components"] = [
    {"series": c.get("series"), "name": c.get("name"),
     "latest": c.get("latest"), "latest_date": c.get("latest_date"),
     "contribution": c.get("contribution"),
     "signal_label": c.get("signal_label")} for c in comps]

idx = an.get("activity_index")
checks = {
    "function_deployed": report["function"].get("exists") is True,
    "schedule_set": report["schedule"].get("exists") is True,
    "invoke_ok": report.get("invoke", {}).get("status") == 200
                 and not report.get("invoke", {}).get("fn_error"),
    "output_ok": an.get("ok") is True,
    "index_in_range": isinstance(idx, (int, float)) and 0 <= idx <= 100,
    "has_regime": an.get("regime") in ("ACCELERATING", "EXPANDING", "STEADY",
                                       "SLOWING", "CONTRACTING"),
    "real_fred_data": len(comps) >= 4
                      and all(c.get("latest") is not None for c in comps),
    "divergence_present": isinstance(an.get("divergence"), dict),
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "ACTIVITY NOWCAST LIVE — deployed, scheduled daily, computing a real-time "
    "0-100 activity index on real high-frequency FRED data with a divergence "
    "flag vs the monthly composite. Roadmap #3 complete."
    if report["all_pass"] else "REVIEW — see checks[]/output/components")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/768_activity_nowcast_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/768_activity_nowcast_verify.json")
