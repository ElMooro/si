"""ops/770 — force-deploy justhodl-activity-nowcast directly via boto3.

deploy-lambdas.yml left the EventBridge rule but no function. This script
deploys the function deterministically from the repo source, captures any
AWS creation error verbatim, wires the schedule, invokes, and verifies.
"""
import json, os, io, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
events = boto3.client("events", region_name="us-east-1", config=cfg)

FN = "justhodl-activity-nowcast"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACC, REGION = "857687956942", "us-east-1"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
RULE = "activity-nowcast-daily"

report = {"ops": 770, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Force-deploy activity-nowcast via boto3 (roadmap #3)"}

# 1. build zip from repo source
try:
    with open(SRC, "r", encoding="utf-8") as f:
        code = f.read()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", code)
    zip_bytes = buf.getvalue()
    report["zip_bytes"] = len(zip_bytes)
except Exception as e:
    report["fatal"] = f"zip build failed: {e}"
    print(json.dumps(report, indent=2))
    os.makedirs("aws/ops/reports", exist_ok=True)
    json.dump(report, open("aws/ops/reports/770_activity_nowcast_force_deploy.json", "w"), indent=2)
    raise SystemExit(0)

# 2. create or update the function
exists = False
try:
    lam.get_function_configuration(FunctionName=FN)
    exists = True
except lam.exceptions.ResourceNotFoundException:
    exists = False
except Exception as e:
    report["precheck_err"] = str(e)[:200]

try:
    if exists:
        lam.update_function_code(FunctionName=FN, ZipFile=zip_bytes)
        report["deploy_action"] = "updated existing function"
    else:
        r = lam.create_function(
            FunctionName=FN, Runtime="python3.12", Role=ROLE,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zip_bytes}, Timeout=60, MemorySize=256,
            Description=("Real-Time Activity Nowcast — high-frequency FRED "
                         "basket into a 0-100 activity index with divergence "
                         "flag vs the monthly composite."),
            Architectures=["x86_64"])
        report["deploy_action"] = "created new function"
        report["create_arn"] = r.get("FunctionArn")
except Exception as e:
    report["deploy_error"] = f"{type(e).__name__}: {str(e)[:300]}"

# 3. wait for active
state = None
for _ in range(30):
    try:
        c = lam.get_function_configuration(FunctionName=FN)
        state = c.get("State")
        lu = c.get("LastUpdateStatus")
        if state == "Active" and lu in ("Successful", None):
            break
    except Exception as e:
        state = f"poll-err:{str(e)[:80]}"
    time.sleep(3)
report["function_state"] = state

# 4. wire EventBridge schedule
if state == "Active":
    try:
        events.put_rule(Name=RULE, ScheduleExpression="cron(30 12 * * ? *)",
                        State="ENABLED",
                        Description="Real-Time Activity Nowcast — daily 12:30 UTC")
        try:
            lam.add_permission(
                FunctionName=FN, StatementId=f"EventBridge-{RULE}",
                Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:{ACC}:rule/{RULE}")
        except lam.exceptions.ResourceConflictException:
            pass
        events.put_targets(Rule=RULE, Targets=[
            {"Id": "target1",
             "Arn": f"arn:aws:lambda:{REGION}:{ACC}:function:{FN}"}])
        report["schedule_wired"] = True
    except Exception as e:
        report["schedule_wired"] = False
        report["schedule_err"] = str(e)[:200]

# 5. invoke + verify
if state == "Active":
    try:
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                       Payload=b"{}")
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
    report["output"] = {"ok": an.get("ok"),
                        "activity_index": an.get("activity_index"),
                        "regime": an.get("regime"), "momentum": an.get("momentum"),
                        "headline": an.get("headline"), "n_ok": an.get("n_ok"),
                        "errors": an.get("errors")}
    report["divergence"] = an.get("divergence")
    report["components"] = [
        {"series": c.get("series"), "latest": c.get("latest"),
         "latest_date": c.get("latest_date"),
         "contribution": c.get("contribution"),
         "signal_label": c.get("signal_label")} for c in comps]

    idx = an.get("activity_index")
    checks = {
        "function_active": state == "Active",
        "schedule_wired": report.get("schedule_wired") is True,
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
        "ACTIVITY NOWCAST LIVE & VERIFIED — function deployed via boto3, "
        "scheduled daily, real FRED data, divergence flag working. "
        "Roadmap #3 complete."
        if report["all_pass"] else "REVIEW — function up, output checks failed")
else:
    report["all_pass"] = False
    report["verdict"] = ("DEPLOY STILL FAILING — see deploy_error / "
                         "function_state.")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/770_activity_nowcast_force_deploy.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/770_activity_nowcast_force_deploy.json")
