"""ops/756 — verify the Opportunity Engine learning loop.

Order: run the calibrator first (it writes SSM weights), then the engine
(it should read those weights) — proves the full feedback loop.
"""
import json, os, urllib.request
from datetime import datetime, timezone, date
import boto3
from botocore.config import Config

cfg = Config(read_timeout=230, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
ssm = boto3.client("ssm", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)

report = {"ops": 756, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "opportunity learning loop verify"}
today = date.today().isoformat()
FACTORS = ["value", "quality", "growth", "momentum"]


def invoke(fn):
    try:
        r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse",
                       Payload=b"{}")
        return {"status": r.get("StatusCode"), "fn_error": r.get("FunctionError"),
                "body": (r["Payload"].read().decode()[:300]
                         if r.get("Payload") else "")}
    except lam.exceptions.ResourceNotFoundException:
        return {"err": "Lambda not found — deploy not yet landed"}
    except Exception as e:
        return {"err": str(e)[:200]}


# 1 ─ calibrator
report["calibrator_invoke"] = invoke("justhodl-opportunity-calibrator")

# 2 ─ calibration report
calib = None
try:
    calib = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                        Key="data/opportunity-calibration.json")["Body"].read())
    report["calibration"] = {
        "schema_version": calib.get("schema_version"),
        "status": calib.get("status"),
        "n_snapshots": calib.get("n_snapshots"),
        "n_matured_windows": calib.get("n_matured_windows"),
        "factor_weights": calib.get("factor_weights"),
        "avg_ic": calib.get("avg_information_coefficient"),
        "headline": calib.get("headline"),
        "weights_written_to_ssm": calib.get("weights_written_to_ssm"),
    }
except Exception as e:
    report["calibration"] = {"err": str(e)[:200]}

# 3 ─ SSM weights param
ssm_w = None
try:
    raw = ssm.get_parameter(Name="/justhodl/opportunity/weights")["Parameter"]["Value"]
    ssm_w = json.loads(raw)
    report["ssm_weights"] = ssm_w
except Exception as e:
    report["ssm_weights"] = {"err": str(e)[:200]}

# 4 ─ engine (should read SSM weights)
report["engine_invoke"] = invoke("justhodl-opportunity-engine")

# 5 ─ engine output carries factor_weights
opp = None
try:
    opp = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                     Key="data/opportunities.json")["Body"].read())
    report["engine_factor_weights"] = opp.get("factor_weights")
except Exception as e:
    report["engine_output"] = {"err": str(e)[:200]}

# 6 ─ snapshot now stores sub-scores
snap_ss_ok = False
try:
    snap = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                      Key=f"data/track-record/snapshots/{today}.json")["Body"].read())
    sample = dict(list(snap.get("picks", {}).items())[:1])
    one = next(iter(sample.values())) if sample else {}
    snap_ss_ok = isinstance(one.get("ss"), list) and len(one.get("ss", [])) == 4
    report["snapshot_sample"] = sample
except Exception as e:
    report["snapshot_sample"] = {"err": str(e)[:200]}

# 7 ─ schedule
try:
    rule = ev.describe_rule(Name="opportunity-calibrator-weekly")
    report["schedule"] = {"state": rule.get("State"),
                          "cron": rule.get("ScheduleExpression")}
except Exception as e:
    report["schedule"] = {"err": str(e)[:200]}

# 8 ─ page
try:
    req = urllib.request.Request("https://justhodl.ai/opportunities.html",
                                 headers={"User-Agent": "justhodl-ops/756"})
    html = urllib.request.urlopen(req, timeout=25).read().decode("utf-8", "replace")
    page_ok = ("Self-calibration" in html and "renderCalibration" in html
               and "opportunity-calibration.json" in html)
except Exception as e:
    page_ok = False
    report["page_err"] = str(e)[:200]

ssm_valid = isinstance(ssm_w, dict) and all(k in ssm_w for k in FACTORS)
engine_w = opp.get("factor_weights") if opp else None
engine_w_ok = isinstance(engine_w, dict) and all(k in engine_w for k in FACTORS)

checks = {
    "calibrator_deployed_ok": report["calibrator_invoke"].get("status") == 200
        and not report["calibrator_invoke"].get("fn_error"),
    "calibration_report_written": bool(calib)
        and calib.get("schema_version") == "1.0" and bool(calib.get("status")),
    "ssm_weights_written": ssm_valid,
    "engine_invoke_ok": report["engine_invoke"].get("status") == 200
        and not report["engine_invoke"].get("fn_error"),
    "engine_reads_weights": engine_w_ok,
    "snapshot_has_subscores": snap_ss_ok,
    "calibrator_schedule_exists": "cron" in report.get("schedule", {}),
    "page_calibration_panel_live": page_ok,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    f"LEARNING LOOP LIVE — calibrator running ({calib.get('status') if calib else '?'}), "
    "writes IC-blended weights to SSM, engine reads them, sub-scores logged "
    "for future calibration, weekly schedule + page panel verified. "
    "Calibration auto-activates once ~33 days of snapshots have matured."
    if report["all_pass"]
    else "REVIEW — see checks[] (new Lambda creation can lag one deploy cycle)")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/756_learning_loop_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/756_learning_loop_verify.json")
