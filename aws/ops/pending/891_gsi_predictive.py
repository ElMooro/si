"""
ops/891 - DEPLOY + RUN the GSI calibrator end-to-end, and verify that
empirical (IC-derived) weights actually flow through to the live Global
Stress Index.

The pipeline (already wired in code by prior pushes):

    justhodl-global-stress engine ---writes--->  data/gsi-dim-history.json
                                                       |
                                                       v
                                  justhodl-gsi-calibrator
                                  (fits Spearman IC vs 21-session forward
                                   SPY drawdown, derives shrunk weights)
                                                       |
                                                       v
                                  SSM /justhodl/gsi/weights
                                                       |
                                                       v
    justhodl-global-stress engine ---reads SSM--->  empirical blend

This op:
  1. Deploys both Lambdas (creates the calibrator if missing).
  2. Backfills ~500 trading days of per-dimension history via the
     engine's built-in backfill handler -- so the calibrator has N >>
     MIN_N_FULL of observations to fit on immediately.
  3. Invokes the calibrator synchronously.
  4. Reads SSM /justhodl/gsi/weights and the calibration report.
  5. Re-invokes global-stress (no backfill flag) and confirms it now
     reports weights_mode == "empirical" (or "blended") and the IC-
     derived weights are reflected on the live index.
  6. Wires the calibrator's weekly Sun 09:00 UTC schedule via
     EventBridge Scheduler.

Writes aws/ops/reports/891_gsi_predictive.json.
"""
import io
import json
import time
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
SSM_KEY = "/justhodl/gsi/weights"
REPORT_KEY = "data/gsi-calibration.json"
HIST_KEY = "data/gsi-dim-history.json"

CALI_FN = "justhodl-gsi-calibrator"
CALI_SRC = f"aws/lambdas/{CALI_FN}/source/lambda_function.py"
CALI_CFG = f"aws/lambdas/{CALI_FN}/config.json"

GS_FN = "justhodl-global-stress"
GS_SRC = f"aws/lambdas/{GS_FN}/source/lambda_function.py"

DIMS = ["market", "credit", "vix", "rates", "contagion", "sovereign"]

cfg = Config(read_timeout=300, connect_timeout=20,
             retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
ssm = boto3.client("ssm", region_name=REGION, config=cfg)
sch = boto3.client("scheduler", region_name=REGION, config=cfg)

rep = {"ops": 891, "ts": datetime.now(timezone.utc).isoformat(),
       "subject": "Deploy + run the GSI calibrator; verify empirical "
                  "weights flow through to the live index",
       "checks": []}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:280]})


def zip_source(path):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", open(path, encoding="utf-8").read())
    return buf.getvalue()


def wait_active(fn, secs=120):
    for _ in range(secs // 3):
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if (c.get("LastUpdateStatus") == "Successful"
                    and c.get("State") == "Active"):
                return True
        except Exception:
            return False
        time.sleep(3)
    return False


# ---- 1. deploy both Lambdas -----------------------------------------------
# calibrator: create if missing, update if present
try:
    cfg_json = json.load(open(CALI_CFG, encoding="utf-8"))
    code = zip_source(CALI_SRC)
    try:
        lam.get_function(FunctionName=CALI_FN)
        lam.update_function_code(FunctionName=CALI_FN, ZipFile=code)
        if wait_active(CALI_FN):
            lam.update_function_configuration(
                FunctionName=CALI_FN,
                Runtime=cfg_json["runtime"],
                Handler=cfg_json["handler"],
                MemorySize=cfg_json["memory"],
                Timeout=cfg_json["timeout"],
                Description=cfg_json["description"][:255],
                Environment={"Variables": cfg_json.get("environment", {})})
        check("calibrator_updated", wait_active(CALI_FN), "")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            lam.create_function(
                FunctionName=CALI_FN,
                Runtime=cfg_json["runtime"],
                Role=cfg_json.get("role", ROLE_ARN),
                Handler=cfg_json["handler"],
                Code={"ZipFile": code},
                Description=cfg_json["description"][:255],
                MemorySize=cfg_json["memory"],
                Timeout=cfg_json["timeout"],
                Architectures=cfg_json.get("architectures",
                                          ["x86_64"]),
                Environment={"Variables": cfg_json.get("environment", {})})
            check("calibrator_created", wait_active(CALI_FN), "fresh create")
        else:
            raise
except Exception as e:
    check("calibrator_deploy", False, f"{type(e).__name__}: {e}")

# global-stress: just refresh
try:
    lam.update_function_code(FunctionName=GS_FN, ZipFile=zip_source(GS_SRC))
    check("global_stress_refreshed", wait_active(GS_FN), "from source")
except Exception as e:
    check("global_stress_refreshed", False, f"{type(e).__name__}: {e}")

# ---- 2. backfill ~500 trading days of per-dimension history ---------------
try:
    r = lam.invoke(FunctionName=GS_FN, InvocationType="RequestResponse",
                   Payload=json.dumps({"backfill": True,
                                       "days": 500}).encode())
    body = json.loads(r["Payload"].read().decode("utf-8", "ignore"))
    inner = json.loads(body.get("body") or "{}")
    check("backfill_ran",
          r.get("StatusCode") == 200 and not r.get("FunctionError")
          and inner.get("ok"),
          ("backfilled %s rows, total %s, range %s -> %s"
           % (inner.get("backfilled"), inner.get("total_snapshots"),
              inner.get("earliest"), inner.get("latest"))))
except Exception as e:
    check("backfill_ran", False, f"{type(e).__name__}: {e}")

time.sleep(2)
# confirm the history file has plenty of snapshots
hist = {}
try:
    hist = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=HIST_KEY
                                    )["Body"].read())
    n = len(hist.get("snapshots") or [])
    check("history_populated", n >= 60,
          "data/gsi-dim-history.json has %d snapshots (need >= 60 for "
          "the calibrator to reach 'empirical' mode)" % n)
except Exception as e:
    check("history_populated", False, f"{type(e).__name__}: {e}")

# ---- 3. invoke the calibrator synchronously -------------------------------
cal_body = {}
try:
    r = lam.invoke(FunctionName=CALI_FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    cal_body = json.loads(r["Payload"].read().decode("utf-8", "ignore"))
    inner = json.loads(cal_body.get("body") or "{}") if isinstance(
        cal_body, dict) and "body" in cal_body else cal_body
    check("calibrator_invoked_ok",
          r.get("StatusCode") == 200 and not r.get("FunctionError"),
          "mode=%s sample_size=%s elapsed=%ss"
          % (inner.get("mode"), inner.get("sample_size"),
             inner.get("elapsed_s")))
except Exception as e:
    check("calibrator_invoked_ok", False, f"{type(e).__name__}: {e}")

# ---- 4. SSM weights + calibration report ---------------------------------
ssm_payload = None
try:
    p = ssm.get_parameter(Name=SSM_KEY)
    ssm_payload = json.loads(p["Parameter"]["Value"])
    has_all = all(k in ssm_payload and isinstance(ssm_payload[k],
                                                  (int, float))
                  for k in DIMS)
    ws = sum(ssm_payload[k] for k in DIMS) if has_all else 0
    check("ssm_weights_written",
          has_all and abs(ws - 1.0) < 0.02,
          "SSM weights: " + ", ".join(
              "%s=%.3f" % (k, ssm_payload.get(k) or 0.0)
              for k in DIMS) + " (sum=%.3f)" % ws)
except Exception as e:
    check("ssm_weights_written", False, f"{type(e).__name__}: {e}")

cal_report = {}
try:
    cal_report = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=REPORT_KEY
                                          )["Body"].read())
    ic = cal_report.get("ic_by_dim") or cal_report.get("ic_per_dim") or {}
    mode = cal_report.get("mode")
    n = cal_report.get("sample_size")
    check("calibration_report_published",
          isinstance(ic, dict) and len(ic) >= 1
          and mode in ("priors", "blended", "empirical", "insufficient",
                       "no_history"),
          "mode=%s N=%s; IC: %s" % (mode, n,
                                    ", ".join("%s=%s" % (k, v)
                                              for k, v in ic.items())))
except Exception as e:
    check("calibration_report_published", False, f"{type(e).__name__}: {e}")

# ---- 5. re-invoke global-stress; confirm empirical weights flow through ---
try:
    r = lam.invoke(FunctionName=GS_FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    body = json.loads(r["Payload"].read().decode("utf-8", "ignore"))
    inv = json.loads(body.get("body") or "{}")
    check("live_invoke_after_calibration",
          r.get("StatusCode") == 200 and not r.get("FunctionError")
          and inv.get("ok"),
          "GSI=%s weights_mode=%s sample=%s"
          % (inv.get("global_stress_index"), inv.get("weights_mode"),
             inv.get("weights_sample_size")))
except Exception as e:
    check("live_invoke_after_calibration", False, f"{type(e).__name__}: {e}")

time.sleep(2)
gs_out = {}
try:
    gs_out = json.loads(s3.get_object(Bucket=S3_BUCKET,
                                      Key="data/global-stress.json"
                                      )["Body"].read())
    weights_block = gs_out.get("weights") or {}
    mode = weights_block.get("mode")
    vals = weights_block.get("values") or {}
    ssm_match = (ssm_payload is not None and isinstance(vals, dict)
                 and all(k in vals and abs(vals[k]
                                           - (ssm_payload.get(k) or 0)) < 0.01
                         for k in DIMS))
    check("gsi_uses_empirical_weights",
          mode in ("empirical", "blended") and ssm_match,
          "live mode=%s; weights match SSM=%s; values=%s"
          % (mode, ssm_match,
             ", ".join("%s=%.3f" % (k, vals.get(k) or 0.0)
                       for k in DIMS)))
except Exception as e:
    check("gsi_uses_empirical_weights", False, f"{type(e).__name__}: {e}")

# ---- 6. weekly schedule for the calibrator -------------------------------
SCHED_NAME = "justhodl-gsi-calibrator-weekly"
try:
    target = {"Arn": "arn:aws:lambda:us-east-1:857687956942:function:"
                     + CALI_FN,
              "RoleArn": SCHED_ROLE}
    common = dict(Name=SCHED_NAME,
                  ScheduleExpression="cron(0 9 ? * SUN *)",
                  ScheduleExpressionTimezone="UTC",
                  FlexibleTimeWindow={"Mode": "OFF"},
                  State="ENABLED",
                  Target=target,
                  Description="GSI calibrator weekly recalibration - "
                              "Sundays 09:00 UTC")
    try:
        sch.get_schedule(Name=SCHED_NAME)
        sch.update_schedule(**common)
        action = "updated"
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            sch.create_schedule(**common)
            action = "created"
        else:
            raise
    # add Lambda invoke permission for the scheduler role if missing
    try:
        lam.add_permission(
            FunctionName=CALI_FN, StatementId="sched-weekly",
            Action="lambda:InvokeFunction",
            Principal="scheduler.amazonaws.com")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") != "ResourceConflictException":
            raise
    check("weekly_schedule_wired", True,
          "%s schedule %s, cron(0 9 ? * SUN *) UTC"
          % (action, SCHED_NAME))
except Exception as e:
    check("weekly_schedule_wired", False, f"{type(e).__name__}: {e}")

# ---- summary --------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
rep["snapshot"] = {
    "ssm_weights": ssm_payload or {},
    "calibration_mode": (cal_report or {}).get("mode"),
    "calibration_n": (cal_report or {}).get("sample_size"),
    "ic_by_dim": ((cal_report or {}).get("ic_by_dim")
                  or (cal_report or {}).get("ic_per_dim") or {}),
    "live_weights": (gs_out.get("weights") or {}).get("values"),
    "live_mode": (gs_out.get("weights") or {}).get("mode"),
    "live_gsi": gs_out.get("global_stress_index"),
}
if rep["all_passed"]:
    rep["verdict"] = (
        "PREDICTIVE GSI LIVE - the Global Stress Index now blends its "
        "six dimensions on empirical Spearman IC against 21-session "
        "forward SPY drawdown, derived from the backfilled per-dimension "
        "history. The calibrator runs weekly Sunday 09:00 UTC and shrinks "
        "toward the priors so noisy IC estimates can't kill any "
        "dimension; the engine reads the empirical weights from SSM at "
        "every run and falls back to priors if absent. The GSI now reflects "
        "what actually leads equity drawdowns, not the priors I chose.")
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("VERIFICATION INCOMPLETE - failed: %s."
                      % ", ".join(bad))

with open("aws/ops/reports/891_gsi_predictive.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
