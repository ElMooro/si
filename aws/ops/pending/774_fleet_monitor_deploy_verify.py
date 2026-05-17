"""ops/774 — force-deploy + verify justhodl-fleet-monitor.

Deploys via boto3 (create or update), inherits ANTHROPIC_API_KEY from
justhodl-ai-chat so the credit probe works, wires the 3h schedule, invokes,
and validates the full-fleet sweep.
"""
import json, os, io, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
events = boto3.client("events", region_name="us-east-1", config=cfg)

FN = "justhodl-fleet-monitor"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACC, REGION = "857687956942", "us-east-1"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
RULE = "fleet-monitor-3h"

report = {"ops": 774, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Force-deploy + verify fleet-monitor (full-fleet observability)"}

# inherit ANTHROPIC_API_KEY from justhodl-ai-chat
env_vars = {}
try:
    src_env = lam.get_function_configuration(
        FunctionName="justhodl-ai-chat").get("Environment", {}).get("Variables", {})
    for k in ("ANTHROPIC_API_KEY", "ANTHROPIC_KEY", "CLAUDE_API_KEY"):
        if src_env.get(k):
            env_vars["ANTHROPIC_API_KEY"] = src_env[k]
            report["anthropic_key_inherited"] = f"from ai-chat ({k})"
            break
except Exception as e:
    report["inherit_err"] = str(e)[:160]
if "ANTHROPIC_API_KEY" not in env_vars:
    report["anthropic_key_inherited"] = "NOT FOUND — credit probe will be 'unknown'"

# build zip
try:
    code = open(SRC, encoding="utf-8").read()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", code)
    zip_bytes = buf.getvalue()
    report["zip_bytes"] = len(zip_bytes)
except Exception as e:
    report["fatal"] = f"zip failed: {e}"
    json.dump(report, open("aws/ops/reports/774_fleet_monitor_deploy_verify.json", "w"), indent=2)
    raise SystemExit(0)

# create or update
exists = False
try:
    lam.get_function_configuration(FunctionName=FN)
    exists = True
except lam.exceptions.ResourceNotFoundException:
    exists = False
except Exception as e:
    report["precheck_err"] = str(e)[:160]

try:
    if exists:
        lam.update_function_code(FunctionName=FN, ZipFile=zip_bytes)
        report["deploy_action"] = "updated existing"
    else:
        lam.create_function(
            FunctionName=FN, Runtime="python3.12", Role=ROLE,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zip_bytes}, Timeout=180, MemorySize=256,
            Description="Full-Fleet Observability — auto-discovering system monitor.",
            Environment={"Variables": env_vars}, Architectures=["x86_64"])
        report["deploy_action"] = "created new"
except Exception as e:
    report["deploy_error"] = f"{type(e).__name__}: {str(e)[:300]}"

# wait active
state = None
for _ in range(35):
    try:
        c = lam.get_function_configuration(FunctionName=FN)
        state = c.get("State")
        if state == "Active" and c.get("LastUpdateStatus") in ("Successful", None):
            break
    except Exception as e:
        state = f"poll-err:{str(e)[:70]}"
    time.sleep(3)
report["function_state"] = state

# ensure env + timeout
if state == "Active":
    try:
        lam.update_function_configuration(
            FunctionName=FN, Timeout=180,
            Environment={"Variables": env_vars})
        for _ in range(20):
            if lam.get_function_configuration(
                    FunctionName=FN).get("LastUpdateStatus") == "Successful":
                break
            time.sleep(2)
    except Exception as e:
        report["config_err"] = str(e)[:160]

# wire schedule
if state == "Active":
    try:
        events.put_rule(Name=RULE, ScheduleExpression="cron(20 */3 * * ? *)",
                        State="ENABLED",
                        Description="Full-Fleet Observability — every 3 hours")
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

# invoke + verify
fl = {}
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
    try:
        fl = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                        Key="_health/fleet.json")["Body"].read())
    except Exception as e:
        report["read_err"] = str(e)[:200]

report["system_status"] = fl.get("system_status")
report["summary"] = fl.get("summary")
report["dependencies"] = fl.get("dependencies")
do = fl.get("data_outputs") or {}
report["data_sweep"] = {"available": do.get("available"), "total": do.get("total"),
                        "green": do.get("green"), "n_yellow": do.get("n_yellow"),
                        "n_red": do.get("n_red"), "n_degraded": do.get("n_degraded")}
report["data_red_sample"] = (do.get("red") or [])[:6]
report["data_degraded_sample"] = (do.get("degraded") or [])[:6]
report["compute"] = fl.get("compute")

deps = fl.get("dependencies") or []
checks = {
    "function_active": state == "Active",
    "schedule_wired": report.get("schedule_wired") is True,
    "invoke_ok": report.get("invoke", {}).get("status") == 200
                 and not report.get("invoke", {}).get("fn_error"),
    "data_sweep_ran": do.get("available") is True and (do.get("total") or 0) >= 50,
    "dependency_probes_ran": len(deps) >= 5,
    "anthropic_probed": any(p.get("name") == "Anthropic API"
                            and p.get("status") != "unknown" for p in deps),
    "system_status_set": fl.get("system_status") in ("green", "yellow", "red"),
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "FLEET MONITOR LIVE & VERIFIED — auto-discovering observability deployed, "
    "scheduled every 3h. Swept all data outputs + probed Anthropic and every "
    f"data-provider key in one pass. System status: {fl.get('system_status')}."
    if report["all_pass"] else "REVIEW — see checks[]/dependencies/data_sweep")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/774_fleet_monitor_deploy_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/774_fleet_monitor_deploy_verify.json")
