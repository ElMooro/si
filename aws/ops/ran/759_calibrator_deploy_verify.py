"""ops/759 — deploy + verify the Opportunity Engine learning loop.

The justhodl-opportunity-calibrator code is in the repo and sound, but the
function never landed on AWS (ops 758: "still not found"). This script
deploys it directly via boto3 — create function + weekly EventBridge rule
+ invoke permission + target — then invokes it and verifies the loop:
the calibrator should run, sit DORMANT (only 1-2 snapshots so far),
write data/opportunity-calibration.json, and keep SSM on the baseline
40/30/20/10 prior.
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
ssm = boto3.client("ssm", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
sts = boto3.client("sts", region_name="us-east-1", config=cfg)

report = {"ops": 759, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "deploy + verify opportunity-calibrator (learning loop)"}

FN = "justhodl-opportunity-calibrator"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CFG = f"aws/lambdas/{FN}/config.json"
acct = sts.get_caller_identity()["Account"]

# ── build deployment zip (Python zipfile, never the zip cmd) ──
conf = json.load(open(CFG))
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    zi = zipfile.ZipInfo("lambda_function.py")
    zi.external_attr = 0o644 << 16
    z.writestr(zi, open(SRC, "r", encoding="utf-8").read())
zip_bytes = buf.getvalue()
report["zip_bytes"] = len(zip_bytes)

# ── create or update the function ──
def wait_active(name, tries=30):
    for _ in range(tries):
        g = lam.get_function(FunctionName=name)["Configuration"]
        if g.get("State") == "Active" and g.get("LastUpdateStatus") != "InProgress":
            return True
        time.sleep(2)
    return False

exists = True
try:
    lam.get_function(FunctionName=FN)
except lam.exceptions.ResourceNotFoundException:
    exists = False

try:
    if exists:
        lam.update_function_code(FunctionName=FN, ZipFile=zip_bytes)
        wait_active(FN)
        lam.update_function_configuration(
            FunctionName=FN, Runtime=conf["runtime"], Handler=conf["handler"],
            Timeout=conf["timeout"], MemorySize=conf["memory"],
            Description=conf["description"])
        report["deploy"] = {"action": "updated"}
    else:
        lam.create_function(
            FunctionName=FN, Runtime=conf["runtime"], Role=conf["role"],
            Handler=conf["handler"], Code={"ZipFile": zip_bytes},
            Timeout=conf["timeout"], MemorySize=conf["memory"],
            Architectures=conf.get("architectures", ["x86_64"]),
            Description=conf["description"], Publish=True)
        report["deploy"] = {"action": "created"}
    wait_active(FN)
except Exception as e:
    report["deploy"] = {"err": str(e)[:300]}

fn_arn = f"arn:aws:lambda:us-east-1:{acct}:function:{FN}"

# ── weekly EventBridge schedule ──
sched = conf.get("schedule", {})
try:
    rule_arn = ev.put_rule(Name=sched["rule_name"],
                           ScheduleExpression=sched["cron"], State="ENABLED",
                           Description=sched.get("description", ""))["RuleArn"]
    try:
        lam.add_permission(FunctionName=FN, StatementId="evb-calibrator-weekly",
                           Action="lambda:InvokeFunction",
                           Principal="events.amazonaws.com", SourceArn=rule_arn)
    except lam.exceptions.ResourceConflictException:
        pass
    ev.put_targets(Rule=sched["rule_name"],
                   Targets=[{"Id": "1", "Arn": fn_arn}])
    report["schedule"] = {"rule": sched["rule_name"], "cron": sched["cron"],
                          "wired": True}
except Exception as e:
    report["schedule"] = {"err": str(e)[:240]}

# ── invoke it ──
try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"),
                        "body": (r["Payload"].read().decode()[:320]
                                 if r.get("Payload") else "")}
except Exception as e:
    report["invoke"] = {"err": str(e)[:240]}

# ── verify outputs ──
calib = None
try:
    calib = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                       Key="data/opportunity-calibration.json")["Body"].read())
    report["calibration"] = {
        "status": calib.get("status"),
        "headline": calib.get("headline"),
        "n_snapshots": calib.get("n_snapshots"),
        "oldest_snapshot_age_days": calib.get("oldest_snapshot_age_days"),
        "n_matured_windows": calib.get("n_matured_windows"),
        "factor_weights": calib.get("factor_weights"),
        "avg_information_coefficient": calib.get("avg_information_coefficient"),
        "weights_written_to_ssm": calib.get("weights_written_to_ssm"),
    }
except Exception as e:
    report["calibration"] = {"err": str(e)[:240]}

try:
    w = json.loads(ssm.get_parameter(Name="/justhodl/opportunity/weights")
                   ["Parameter"]["Value"])
    report["ssm_weights"] = w
except Exception as e:
    report["ssm_weights"] = {"err": str(e)[:200]}

checks = {
    "calibrator_deployed": "err" not in report.get("deploy", {}),
    "schedule_rule_wired": report.get("schedule", {}).get("wired") is True,
    "calibrator_runs_ok": report.get("invoke", {}).get("status") == 200
                          and not report.get("invoke", {}).get("fn_error"),
    "calibration_report_written": bool(calib)
        and calib.get("status") in ("insufficient_data", "calibrated"),
    "dormant_or_calibrated_safely": bool(calib) and (
        calib.get("status") == "calibrated"
        or (calib.get("status") == "insufficient_data"
            and calib.get("factor_weights") == {"value": 0.40, "quality": 0.30,
                                                "growth": 0.20, "momentum": 0.10})),
    "ssm_weights_valid": isinstance(report.get("ssm_weights"), dict)
        and abs(sum(report["ssm_weights"].get(k, 0) for k in
                ("value", "quality", "growth", "momentum")) - 1.0) < 0.02,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "LEARNING LOOP LIVE — opportunity-calibrator deployed, weekly schedule "
    "wired, running safely on the baseline prior (DORMANT until ~33 days of "
    "snapshots mature, then it calibrates factor weights from realised "
    "Information Coefficients). The loop is fully armed."
    if report["all_pass"]
    else "REVIEW — see checks[]")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/759_calibrator_deploy_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/759_calibrator_deploy_verify.json")
