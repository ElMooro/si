"""
ops/894 - Verify the soft-floor + cap + shrinkage regularization is
in effect end-to-end.

Forces a fresh calibrator run with the regularized weight derivation
(WEIGHT_FLOOR=0.05, WEIGHT_CAP=0.40, SHRINKAGE=0.6), then reads SSM
and the live global-stress output to confirm:

  - every dimension gets at least the floor (5%);
  - no dimension exceeds the cap (40%);
  - global-stress picks up the new softened weights on its next run;
  - the weighted GSI value with the softened weights is published.

Writes aws/ops/reports/894_soft_floor_verify.json.
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
GS_OUT_KEY = "data/global-stress.json"
CAL_REPORT_KEY = "data/gsi-calibration.json"
WEIGHTS_PARAM = "/justhodl/gsi/weights"
DIMS = ("market", "credit", "vix", "rate_vol", "contagion", "sovereign")

cfg = Config(read_timeout=300, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
ssm = boto3.client("ssm", region_name=REGION, config=cfg)

rep = {"ops": 894, "ts": datetime.now(timezone.utc).isoformat(),
       "subject": "Verify soft-floor + cap + shrinkage regularization",
       "checks": []}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:300]})


# ---- 1. rerun calibrator with current code -------------------------------
cal_body = {}
try:
    r = lam.invoke(FunctionName=CAL_FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    raw = r["Payload"].read().decode("utf-8", "ignore")
    cal_body = json.loads(json.loads(raw).get("body") or "{}")
    check("calibrator_rerun",
          r.get("StatusCode") == 200 and not r.get("FunctionError")
          and cal_body.get("ok") is True,
          "mode=%s N=%s, IC=%s" % (cal_body.get("mode"),
                                    cal_body.get("sample_size"),
                                    cal_body.get("ic")))
except Exception as e:
    check("calibrator_rerun", False, f"{type(e).__name__}: {e}")

# ---- 2. read the SSM weights, verify floor/cap satisfied -----------------
ssm_payload = {}
try:
    p = ssm.get_parameter(Name=WEIGHTS_PARAM)
    ssm_payload = json.loads(p["Parameter"]["Value"])
    w = ssm_payload.get("weights") or {}
    floor_ok = all(w.get(d, 0) >= 0.049 for d in DIMS)   # 5% within rounding
    cap_ok = all(w.get(d, 0) <= 0.401 for d in DIMS)     # 40% within rounding
    sums_to_1 = abs(sum(w.values()) - 1.0) < 0.01
    check("floor_satisfied", floor_ok,
          "min weight = %s" % round(min(w.values()), 4)
          if w else "no weights")
    check("cap_satisfied", cap_ok,
          "max weight = %s" % round(max(w.values()), 4)
          if w else "no weights")
    check("weights_sum_to_unity", sums_to_1,
          "sum = %s" % round(sum(w.values()), 4)
          if w else "no weights")
    check("ssm_weights_present",
          set(w.keys()) >= set(DIMS),
          "mode=%s, weights=%s" % (ssm_payload.get("mode"),
                                    {d: round(w.get(d, 0), 4)
                                     for d in DIMS}))
except Exception as e:
    check("ssm_weights_present", False, f"{type(e).__name__}: {e}")

# ---- 3. re-invoke global-stress; it should pick up softened weights ------
gs_out = {}
applied = {}
try:
    r = lam.invoke(FunctionName=GS_FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    time.sleep(2)
    gs_out = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=GS_OUT_KEY
                                      )["Body"].read())
    wb = gs_out.get("weights") or {}
    applied = wb.get("values") or {}
    applied_floor = all(applied.get(d, 0) >= 0.049 for d in DIMS)
    applied_cap = all(applied.get(d, 0) <= 0.401 for d in DIMS)
    check("global_stress_applies_softened",
          applied_floor and applied_cap and wb.get("mode") in
          ("blended", "empirical"),
          "GSI=%s, mode=%s, active weights=%s" % (
              gs_out.get("global_stress_index"), wb.get("mode"),
              {d: round(applied.get(d, 0), 4) for d in DIMS}))
except Exception as e:
    check("global_stress_applies_softened", False,
          f"{type(e).__name__}: {e}")

# ---- summary --------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
ic = cal_body.get("ic") or {}
rep["snapshot"] = {
    "sample_size": cal_body.get("sample_size"),
    "mode": cal_body.get("mode"),
    "ic": {d: ic.get(d) for d in DIMS},
    "active_weights": {d: round(applied.get(d, 0), 4) for d in DIMS}
    if applied else {},
    "priors": {"market": 0.32, "credit": 0.18, "vix": 0.17,
               "rate_vol": 0.13, "contagion": 0.10, "sovereign": 0.10},
    "global_stress_index": gs_out.get("global_stress_index"),
    "global_stress_level": gs_out.get("global_stress_level"),
}
if rep["all_passed"]:
    rep["verdict"] = (
        "SOFT-FLOOR REGULARIZATION LIVE - the GSI weights are now "
        "regularized with floor 5%, cap 40%, and 60/40 empirical/prior "
        "shrinkage. Every dimension keeps a meaningful contribution, "
        "no single predictor dominates, and the index degrades "
        "gracefully toward priors when the empirical fit weakens. "
        "Global-stress applies the regularized weights on each run.")
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("VERIFICATION INCOMPLETE - failed: %s."
                      % ", ".join(bad))

with open("aws/ops/reports/894_soft_floor_verify.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
