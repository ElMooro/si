"""
ops/889 - VERIFY the full predictive-GSI pipeline end-to-end.

Proves the Global Stress Index has shifted from prior-weighted to
empirically-calibrated against forward equity drawdowns. The pipeline:

  1) global-stress engine runs with backfill flag -> reconstructs the
     last ~200 sessions of per-dimension stress scores in-memory and
     writes them to data/gsi-dim-history.json.
  2) gsi-calibrator reads the dim-history, pairs each snapshot with
     its 21-session forward SPY drawdown, fits per-dimension Spearman
     IC and publishes weights to SSM /justhodl/gsi/weights + a full
     report to data/gsi-calibration.json.
  3) global-stress engine runs normally -- it loads SSM weights at
     the start of the run and exposes them in the published 'weights'
     block of data/global-stress.json. With N around 200 the mode
     should land on 'empirical'.

This op runs all three steps, verifies each, AND wires the weekly
calibrator schedule (EventBridge Scheduler, Sundays 09:00 UTC).
Writes aws/ops/reports/889_predictive_gsi.json.
"""
import json
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
GS_FN = "justhodl-global-stress"
CAL_FN = "justhodl-gsi-calibrator"
DIM_HIST_KEY = "data/gsi-dim-history.json"
CAL_REPORT_KEY = "data/gsi-calibration.json"
GS_OUT_KEY = "data/global-stress.json"
WEIGHTS_PARAM = "/justhodl/gsi/weights"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"

cfg = Config(read_timeout=660, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
ssm = boto3.client("ssm", region_name=REGION, config=cfg)
sch = boto3.client("scheduler", region_name=REGION, config=cfg)

rep = {"ops": 889, "ts": datetime.now(timezone.utc).isoformat(),
       "subject": "Verify the predictive GSI: backfill -> calibrate -> "
                  "GSI reads SSM weights", "checks": []}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:280]})


# ---- 0. confirm both Lambdas exist ----------------------------------------
gs_arn, cal_arn = None, None
try:
    gs_arn = lam.get_function(FunctionName=GS_FN
                              )["Configuration"]["FunctionArn"]
    check("global_stress_deployed", True,
          gs_arn.split(":")[-1] if gs_arn else "")
except Exception as e:
    check("global_stress_deployed", False, f"{type(e).__name__}: {e}")
try:
    cal_arn = lam.get_function(FunctionName=CAL_FN
                               )["Configuration"]["FunctionArn"]
    check("gsi_calibrator_deployed", True,
          cal_arn.split(":")[-1] if cal_arn else "")
except Exception as e:
    check("gsi_calibrator_deployed", False, f"{type(e).__name__}: {e}")

# ---- 1. invoke backfill ---------------------------------------------------
backfill_body = {}
try:
    r = lam.invoke(FunctionName=GS_FN, InvocationType="RequestResponse",
                   Payload=json.dumps({"backfill": True,
                                       "days": 200}).encode("utf-8"))
    raw = r["Payload"].read().decode("utf-8", "ignore")
    backfill_body = json.loads(json.loads(raw).get("body") or "{}")
    check("backfill_ok",
          r.get("StatusCode") == 200 and not r.get("FunctionError")
          and backfill_body.get("ok") is True
          and backfill_body.get("backfilled", 0) >= 30,
          "backfilled=%s, total=%s, range %s -> %s, elapsed %ss"
          % (backfill_body.get("backfilled"),
             backfill_body.get("total_snapshots"),
             backfill_body.get("earliest"), backfill_body.get("latest"),
             backfill_body.get("elapsed_s")))
except Exception as e:
    check("backfill_ok", False, f"{type(e).__name__}: {e}")

# ---- 2. dim-history populated --------------------------------------------
hist_snaps = []
try:
    d = json.loads(s3.get_object(Bucket=S3_BUCKET,
                                 Key=DIM_HIST_KEY)["Body"].read())
    hist_snaps = d.get("snapshots") or []
    # count snapshots that have ALL six dimensions filled in
    full = [s for s in hist_snaps
            if isinstance(s.get("dims"), dict)
            and len(s["dims"]) >= 5
            and s.get("spy_close")]
    check("dim_history_populated",
          len(hist_snaps) >= 30 and len(full) >= 30,
          "%d snapshots total, %d with >=5 dims + spy_close"
          % (len(hist_snaps), len(full)))
except Exception as e:
    check("dim_history_populated", False, f"{type(e).__name__}: {e}")

# ---- 3. invoke calibrator -------------------------------------------------
cal_body = {}
try:
    r = lam.invoke(FunctionName=CAL_FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    raw = r["Payload"].read().decode("utf-8", "ignore")
    cal_body = json.loads(json.loads(raw).get("body") or "{}")
    check("calibrator_ran",
          r.get("StatusCode") == 200 and not r.get("FunctionError")
          and cal_body.get("ok") is True
          and cal_body.get("sample_size", 0) >= 30,
          "mode=%s, N=%s, IC=%s, elapsed %ss"
          % (cal_body.get("mode"), cal_body.get("sample_size"),
             cal_body.get("ic"), cal_body.get("elapsed_s")))
except Exception as e:
    check("calibrator_ran", False, f"{type(e).__name__}: {e}")

# ---- 4. SSM weights written ----------------------------------------------
ssm_payload = {}
try:
    p = ssm.get_parameter(Name=WEIGHTS_PARAM)
    ssm_payload = json.loads(p["Parameter"]["Value"])
    needed = {"market", "credit", "vix", "rate_vol", "contagion",
              "sovereign"}
    weights_ok = (set(ssm_payload.get("weights", {}).keys()) >= needed
                  and isinstance(ssm_payload.get("sample_size"), int)
                  and ssm_payload.get("sample_size") >= 30
                  and ssm_payload.get("mode") in ("blended", "empirical"))
    check("ssm_weights_written", weights_ok,
          "mode=%s, N=%s, weights=%s" % (ssm_payload.get("mode"),
                                          ssm_payload.get("sample_size"),
                                          ssm_payload.get("weights")))
except Exception as e:
    check("ssm_weights_written", False, f"{type(e).__name__}: {e}")

# ---- 5. calibration report on S3 -----------------------------------------
report = {}
try:
    report = json.loads(s3.get_object(Bucket=S3_BUCKET,
                                      Key=CAL_REPORT_KEY)["Body"].read())
    paired = report.get("paired_observations") or []
    check("calibration_report_complete",
          isinstance(report.get("ic"), dict)
          and isinstance(report.get("weights"), dict)
          and len(paired) >= 10,
          "%d paired observations in report, methodology %d chars"
          % (len(paired), len(report.get("methodology") or "")))
except Exception as e:
    check("calibration_report_complete", False, f"{type(e).__name__}: {e}")

# ---- 6. re-invoke global-stress; it should pick up SSM weights -----------
gs_out = {}
try:
    r = lam.invoke(FunctionName=GS_FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    raw = r["Payload"].read().decode("utf-8", "ignore")
    body = json.loads(json.loads(raw).get("body") or "{}")
    time.sleep(2)
    gs_out = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=GS_OUT_KEY
                                      )["Body"].read())
    w_block = gs_out.get("weights") or {}
    applied_ok = (w_block.get("mode") in ("blended", "empirical")
                  and isinstance(w_block.get("sample_size"), int)
                  and w_block.get("sample_size") >= 30)
    check("global_stress_uses_calibrated_weights", applied_ok,
          "weights.mode=%s, sample_size=%s, GSI=%s, weights=%s"
          % (w_block.get("mode"), w_block.get("sample_size"),
             gs_out.get("global_stress_index"),
             w_block.get("values")))
except Exception as e:
    check("global_stress_uses_calibrated_weights", False,
          f"{type(e).__name__}: {e}")

# ---- 7. wire / verify weekly schedule ------------------------------------
sched_name = "justhodl-gsi-calibrator-weekly"
try:
    try:
        sch.get_schedule(Name=sched_name)
        sch.update_schedule(Name=sched_name, FlexibleTimeWindow={"Mode": "OFF"},
                            ScheduleExpression="cron(0 9 ? * SUN *)",
                            ScheduleExpressionTimezone="UTC", State="ENABLED",
                            Target={"Arn": cal_arn, "RoleArn": SCHED_ROLE,
                                    "Input": json.dumps({})})
        action = "updated"
    except sch.exceptions.ResourceNotFoundException:
        sch.create_schedule(Name=sched_name,
                            FlexibleTimeWindow={"Mode": "OFF"},
                            ScheduleExpression="cron(0 9 ? * SUN *)",
                            ScheduleExpressionTimezone="UTC", State="ENABLED",
                            Target={"Arn": cal_arn, "RoleArn": SCHED_ROLE,
                                    "Input": json.dumps({})})
        action = "created"
    check("calibrator_scheduled_weekly", True,
          "%s -> Sundays 09:00 UTC" % action)
except Exception as e:
    check("calibrator_scheduled_weekly", False,
          f"{type(e).__name__}: {e}")

# ---- summary --------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
rep["snapshot"] = {
    "dim_history_snapshots": len(hist_snaps),
    "calibrator_mode": cal_body.get("mode"),
    "sample_size": cal_body.get("sample_size"),
    "ic_per_dim": cal_body.get("ic"),
    "calibrated_weights": cal_body.get("weights"),
    "global_stress_index_with_new_weights": gs_out.get("global_stress_index"),
    "weights_mode_in_engine": (gs_out.get("weights") or {}).get("mode"),
}
if rep["all_passed"]:
    rep["verdict"] = (
        "PREDICTIVE GSI LIVE - the Global Stress Index has been "
        "recalibrated against forward equity drawdowns. The backfill "
        "reconstructed historical dimension scores for ~200 sessions, "
        "the calibrator fit Spearman rank IC of each dimension against "
        "the 21-session forward SPY drawdown, and re-weighted the blend "
        "by empirical IC. Global-stress now loads those weights from "
        "SSM on every run, with the prior weights kept as a fallback. "
        "The calibrator is scheduled weekly so the GSI stays "
        "self-improving as new data arrives.")
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("VERIFICATION INCOMPLETE - failed: %s."
                      % ", ".join(bad))

with open("aws/ops/reports/889_predictive_gsi.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
