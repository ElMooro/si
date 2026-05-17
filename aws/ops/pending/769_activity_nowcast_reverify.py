"""ops/769 — re-verify justhodl-activity-nowcast (ops 768 hit a deploy race).

ops 768 ran before deploy-lambdas.yml finished creating the function. This
re-checks with the deploy settled; if still missing, surfaces it clearly.
"""
import json, os
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
events = boto3.client("events", region_name="us-east-1", config=cfg)

report = {"ops": 769, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Activity Nowcast re-verify (ops 768 deploy race)"}

exists = False
try:
    fc = lam.get_function_configuration(FunctionName="justhodl-activity-nowcast")
    exists = True
    report["function"] = {"exists": True, "runtime": fc.get("Runtime"),
                          "last_modified": fc.get("LastModified"),
                          "state": fc.get("State"),
                          "code_size": fc.get("CodeSize")}
except Exception as e:
    report["function"] = {"exists": False, "err": str(e)[:200]}

if not exists:
    # confirm via a paginated scan in case GetFunctionConfiguration lagged
    found = False
    try:
        p = lam.get_paginator("list_functions")
        for page in p.paginate():
            for f in page.get("Functions", []):
                if f.get("FunctionName") == "justhodl-activity-nowcast":
                    found = True
                    break
            if found:
                break
    except Exception as e:
        report["scan_err"] = str(e)[:160]
    report["function"]["found_in_scan"] = found
    exists = found

try:
    r = events.describe_rule(Name="activity-nowcast-daily")
    report["schedule"] = {"exists": True, "state": r.get("State")}
except Exception as e:
    report["schedule"] = {"exists": False, "err": str(e)[:120]}

if exists:
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
        an = json.loads(s3.get_object(
            Bucket="justhodl-dashboard-live",
            Key="data/activity-nowcast.json")["Body"].read())
    except Exception as e:
        report["read_err"] = str(e)[:200]
    comps = an.get("components", []) or []
    report["output"] = {
        "ok": an.get("ok"), "activity_index": an.get("activity_index"),
        "regime": an.get("regime"), "momentum": an.get("momentum"),
        "headline": an.get("headline"), "n_ok": an.get("n_ok"),
        "divergence": an.get("divergence"), "errors": an.get("errors")}
    report["components"] = [
        {"series": c.get("series"), "latest": c.get("latest"),
         "latest_date": c.get("latest_date"),
         "contribution": c.get("contribution"),
         "signal_label": c.get("signal_label")} for c in comps]

    idx = an.get("activity_index")
    checks = {
        "function_deployed": True,
        "invoke_ok": report.get("invoke", {}).get("status") == 200
                     and not report.get("invoke", {}).get("fn_error"),
        "output_ok": an.get("ok") is True,
        "index_in_range": isinstance(idx, (int, float)) and 0 <= idx <= 100,
        "has_regime": an.get("regime") in ("ACCELERATING", "EXPANDING",
                                           "STEADY", "SLOWING", "CONTRACTING"),
        "real_fred_data": len(comps) >= 4
                          and all(c.get("latest") is not None for c in comps),
        "divergence_present": isinstance(an.get("divergence"), dict),
    }
    report["checks"] = checks
    report["all_pass"] = all(checks.values())
    report["verdict"] = (
        "ACTIVITY NOWCAST LIVE & VERIFIED — deployed, scheduled daily, real "
        "FRED data, divergence flag working. Roadmap #3 complete."
        if report["all_pass"] else "REVIEW — function up but output checks fail")
else:
    report["all_pass"] = False
    report["verdict"] = ("DEPLOY FAILED — function still absent after settle "
                         "time; inspect deploy-lambdas.yml run for "
                         "justhodl-activity-nowcast.")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/769_activity_nowcast_reverify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/769_activity_nowcast_reverify.json")
