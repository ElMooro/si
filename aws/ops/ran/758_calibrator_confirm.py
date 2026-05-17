"""ops/758 — confirm justhodl-opportunity-calibrator deployed and the
learning loop is fully closed (calibrator -> SSM -> engine)."""
import json, os
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=230, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
ssm = boto3.client("ssm", region_name="us-east-1", config=cfg)

report = {"ops": 758, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "calibrator deploy confirm"}
FACTORS = ["value", "quality", "growth", "momentum"]

try:
    r = lam.invoke(FunctionName="justhodl-opportunity-calibrator",
                   InvocationType="RequestResponse", Payload=b"{}")
    report["calibrator_invoke"] = {
        "status": r.get("StatusCode"), "fn_error": r.get("FunctionError"),
        "body": r["Payload"].read().decode()[:400] if r.get("Payload") else ""}
except lam.exceptions.ResourceNotFoundException:
    report["calibrator_invoke"] = {"err": "still not found"}
except Exception as e:
    report["calibrator_invoke"] = {"err": str(e)[:200]}

calib = None
try:
    calib = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                       Key="data/opportunity-calibration.json")["Body"].read())
    report["calibration"] = {
        "schema_version": calib.get("schema_version"),
        "status": calib.get("status"),
        "n_snapshots": calib.get("n_snapshots"),
        "n_matured_windows": calib.get("n_matured_windows"),
        "oldest_snapshot_age_days": calib.get("oldest_snapshot_age_days"),
        "factor_weights": calib.get("factor_weights"),
        "avg_ic": calib.get("avg_information_coefficient"),
        "headline": calib.get("headline"),
        "weights_written_to_ssm": calib.get("weights_written_to_ssm")}
except Exception as e:
    report["calibration"] = {"err": str(e)[:200]}

ssm_w = None
try:
    ssm_w = json.loads(ssm.get_parameter(
        Name="/justhodl/opportunity/weights")["Parameter"]["Value"])
    report["ssm_weights"] = ssm_w
except Exception as e:
    report["ssm_weights"] = {"err": str(e)[:200]}

checks = {
    "calibrator_runs_ok": report["calibrator_invoke"].get("status") == 200
        and not report["calibrator_invoke"].get("fn_error"),
    "calibration_report_ok": bool(calib) and calib.get("schema_version") == "1.0"
        and bool(calib.get("status")),
    "ssm_weights_present": isinstance(ssm_w, dict)
        and all(k in ssm_w for k in FACTORS),
    "calibrator_wrote_ssm": bool(calib) and calib.get("weights_written_to_ssm") is True,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
status = (calib or {}).get("status", "?")
report["verdict"] = (
    f"LEARNING LOOP CLOSED — calibrator deployed and running (status: "
    f"{status}), weights written to SSM, engine read-path proven (ops 757). "
    "The loop auto-activates once ~33 days of snapshots have matured; until "
    "then it safely runs the baseline prior."
    if report["all_pass"]
    else "REVIEW — see checks[]")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/758_calibrator_confirm.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/758_calibrator_confirm.json")
