"""ops/757 — re-verify the learning loop (calibrator deploy lagged in 756).

Rigorous SSM read-path proof: write a distinctive sentinel weight to SSM,
invoke the engine, confirm the engine's output carries the sentinel —
then restore the real weights and re-run the engine so the live page is
left correct.
"""
import json, os
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=230, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
ssm = boto3.client("ssm", region_name="us-east-1", config=cfg)

report = {"ops": 757, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "learning loop re-verify + SSM read-path proof"}
FACTORS = ["value", "quality", "growth", "momentum"]
PARAM = "/justhodl/opportunity/weights"
BASELINE = {"value": 0.40, "quality": 0.30, "growth": 0.20, "momentum": 0.10}
SENTINEL = {"value": 0.25, "quality": 0.25, "growth": 0.25, "momentum": 0.25}


def invoke(fn):
    try:
        r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse",
                       Payload=b"{}")
        return {"status": r.get("StatusCode"), "fn_error": r.get("FunctionError")}
    except lam.exceptions.ResourceNotFoundException:
        return {"err": "Lambda not found"}
    except Exception as e:
        return {"err": str(e)[:200]}


def engine_weights():
    try:
        opp = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                         Key="data/opportunities.json")["Body"].read())
        return opp.get("factor_weights")
    except Exception as e:
        return {"err": str(e)[:160]}


# 1 ─ calibrator now deployed?
report["calibrator_invoke"] = invoke("justhodl-opportunity-calibrator")

calib = None
try:
    calib = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                        Key="data/opportunity-calibration.json")["Body"].read())
    report["calibration"] = {"status": calib.get("status"),
                             "n_matured_windows": calib.get("n_matured_windows"),
                             "factor_weights": calib.get("factor_weights"),
                             "headline": calib.get("headline"),
                             "ssm": calib.get("weights_written_to_ssm")}
except Exception as e:
    report["calibration"] = {"err": str(e)[:200]}

try:
    raw = ssm.get_parameter(Name=PARAM)["Parameter"]["Value"]
    report["ssm_after_calibrator"] = json.loads(raw)
except Exception as e:
    report["ssm_after_calibrator"] = {"err": str(e)[:200]}

# 2 ─ SSM read-path proof: inject sentinel → engine must echo it
sentinel_proven = False
try:
    ssm.put_parameter(Name=PARAM, Type="String", Overwrite=True,
                      Value=json.dumps(SENTINEL))
    report["engine_invoke_sentinel"] = invoke("justhodl-opportunity-engine")
    ew = engine_weights()
    report["engine_weights_with_sentinel"] = ew
    sentinel_proven = (isinstance(ew, dict)
                       and all(abs(ew.get(k, 0) - 0.25) < 0.001 for k in FACTORS))
except Exception as e:
    report["sentinel_test_err"] = str(e)[:200]

# 3 ─ restore real weights (whatever the calibrator decided; else baseline)
restore = BASELINE
if calib and isinstance(calib.get("factor_weights"), dict):
    restore = calib["factor_weights"]
restored_ok = False
try:
    ssm.put_parameter(Name=PARAM, Type="String", Overwrite=True,
                      Value=json.dumps(restore))
    report["engine_invoke_restore"] = invoke("justhodl-opportunity-engine")
    ew = engine_weights()
    report["engine_weights_restored"] = ew
    restored_ok = (isinstance(ew, dict)
                   and all(abs(ew.get(k, 0) - restore.get(k, 0)) < 0.001
                           for k in FACTORS))
except Exception as e:
    report["restore_err"] = str(e)[:200]

checks = {
    "calibrator_runs_ok": report["calibrator_invoke"].get("status") == 200
        and not report["calibrator_invoke"].get("fn_error"),
    "calibration_report_ok": bool(calib) and bool(calib.get("status")),
    "ssm_weights_present": isinstance(report.get("ssm_after_calibrator"), dict)
        and all(k in report.get("ssm_after_calibrator", {}) for k in FACTORS),
    "engine_reads_ssm_proven": sentinel_proven,
    "real_weights_restored": restored_ok,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "LEARNING LOOP FULLY VERIFIED — calibrator runs, writes weights to SSM, "
    "and the engine provably reads them (sentinel echoed in output). Real "
    f"weights restored ({calib.get('status') if calib else '?'}). The loop "
    "auto-activates once ~33 days of snapshots mature."
    if report["all_pass"]
    else "REVIEW — see checks[]")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/757_learning_loop_reverify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/757_learning_loop_reverify.json")
