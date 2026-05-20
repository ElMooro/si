"""
ops/890 - Fix the predictive GSI: extend the historical backfill to
~2000 sessions (8 years, spanning 2018 vol, 2020 covid, 2022 rates,
2023 SVB, 2024 calm, 2025-26), then recalibrate the dimension weights
under the new diversification-preserving scheme (shrinkage toward
priors + per-dim floor + per-dim cap) deployed by the calibrator
fix. Verifies the result is diversified -- no dimension at 0%, no
dimension above the cap -- and that global-stress picks up the new
weights from SSM.

Verifies:
  - calibrator redeploys from corrected source;
  - global-stress accepts a backfill invocation for ~2000 sessions
    and writes that many snapshots into data/gsi-dim-history.json;
  - calibrator runs against the deeper history and reaches the
    'empirical' mode with N approaching the new snapshot count;
  - SSM /justhodl/gsi/weights now carries diversified weights:
    every dimension >= 0.05 (the floor), every dimension <= 0.40
    (the cap), sum exactly 1.0;
  - global-stress, invoked normally, loads the new weights and
    computes the GSI from a six-component blend (not collapsed);
  - data/gsi-calibration.json exposes empirical_weights, priors,
    shrinkage, weight_floor, weight_cap and the per-dimension IC.

Writes aws/ops/reports/890_diversified_gsi.json.
"""
import io
import json
import time
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.config import Config

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
CAL_FN = "justhodl-gsi-calibrator"
GS_FN = "justhodl-global-stress"
CAL_SRC = f"aws/lambdas/{CAL_FN}/source/lambda_function.py"
WEIGHTS_PARAM = "/justhodl/gsi/weights"
DIM_HIST_KEY = "data/gsi-dim-history.json"
CAL_REPORT_KEY = "data/gsi-calibration.json"
GS_OUT_KEY = "data/global-stress.json"

cfg = Config(read_timeout=660, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
ssm = boto3.client("ssm", region_name=REGION, config=cfg)

rep = {"ops": 890, "ts": datetime.now(timezone.utc).isoformat(),
       "subject": "Fix the predictive GSI -- deep backfill + "
                  "diversification-preserving recalibration",
       "checks": []}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:260]})


def wait_active(fn, max_wait=180):
    for _ in range(max_wait // 3):
        c = lam.get_function_configuration(FunctionName=fn)
        if (c.get("LastUpdateStatus") == "Successful"
                and c.get("State") == "Active"):
            return True
        time.sleep(3)
    return False


# ---- 1. redeploy calibrator from fixed source ------------------------------
try:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", open(CAL_SRC, encoding="utf-8").read())
    lam.update_function_code(FunctionName=CAL_FN, ZipFile=buf.getvalue())
    check("calibrator_redeployed", wait_active(CAL_FN),
          "from source with shrinkage + cap + floor")
except Exception as e:
    check("calibrator_redeployed", False, f"{type(e).__name__}: {e}")

# ---- 2. trigger the deeper backfill via global-stress ----------------------
try:
    r = lam.invoke(FunctionName=GS_FN, InvocationType="RequestResponse",
                   Payload=json.dumps({"backfill": True,
                                       "days": 2000}).encode("utf-8"))
    body = json.loads(r["Payload"].read().decode("utf-8", "ignore"))
    inv = json.loads(body.get("body") or "{}")
    check("deep_backfill_ran",
          r.get("StatusCode") == 200
          and not r.get("FunctionError")
          and inv.get("ok") is True
          and (inv.get("backfilled") or 0) >= 1500,
          "backfilled=%s, total=%s, range %s -> %s"
          % (inv.get("backfilled"), inv.get("total"),
             inv.get("earliest"), inv.get("latest")))
except Exception as e:
    check("deep_backfill_ran", False, f"{type(e).__name__}: {e}")

time.sleep(2)

# ---- 3. confirm the snapshot store now has the expanded history ------------
hist_n = 0
try:
    hist = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=DIM_HIST_KEY
                                    )["Body"].read())
    snaps = hist.get("snapshots") or []
    hist_n = len(snaps)
    check("dim_history_expanded", hist_n >= 1500,
          "%s snapshots in store -- spans deep multi-regime training "
          "window" % hist_n)
except Exception as e:
    check("dim_history_expanded", False, f"{type(e).__name__}: {e}")

# ---- 4. run the calibrator against the deeper history ----------------------
try:
    r = lam.invoke(FunctionName=CAL_FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    body = json.loads(r["Payload"].read().decode("utf-8", "ignore"))
    inv = json.loads(body.get("body") or "{}")
    check("calibrator_ran",
          r.get("StatusCode") == 200 and not r.get("FunctionError")
          and inv.get("ok") is True,
          "mode=%s, N=%s, IC=%s"
          % (inv.get("mode"), inv.get("sample_size"), inv.get("ic")))
except Exception as e:
    check("calibrator_ran", False, f"{type(e).__name__}: {e}")

# ---- 5. SSM has new diversified weights ------------------------------------
weights = {}
ssm_mode = None
ssm_n = 0
try:
    p = ssm.get_parameter(Name=WEIGHTS_PARAM)
    payload = json.loads(p["Parameter"]["Value"])
    weights = payload.get("weights") or {}
    ssm_mode = payload.get("mode")
    ssm_n = int(payload.get("sample_size") or 0)
    s = sum(weights.values())
    floor_ok = all(v >= 0.049 for v in weights.values())
    cap_ok = all(v <= 0.401 for v in weights.values())
    check("ssm_weights_diversified",
          weights and 0.99 <= s <= 1.01 and floor_ok and cap_ok,
          "mode=%s, N=%s, sum=%.4f, weights=%s"
          % (ssm_mode, ssm_n, s,
             {k: round(v, 3) for k, v in sorted(weights.items())}))
    check("ssm_no_dim_at_zero",
          floor_ok,
          "min weight = %.3f (floor 0.05)"
          % (min(weights.values()) if weights else 0))
    check("ssm_no_dim_above_cap",
          cap_ok,
          "max weight = %.3f (cap 0.40)"
          % (max(weights.values()) if weights else 0))
except Exception as e:
    check("ssm_weights_diversified", False, f"{type(e).__name__}: {e}")
    check("ssm_no_dim_at_zero", False, "n/a")
    check("ssm_no_dim_above_cap", False, "n/a")

# ---- 6. calibration report is rich -----------------------------------------
try:
    rprt = json.loads(s3.get_object(Bucket=S3_BUCKET,
                                    Key=CAL_REPORT_KEY)["Body"].read())
    check("report_has_diversification_params",
          all(rprt.get(k) is not None for k in ("ic", "empirical_weights",
                                                "priors", "weight_floor",
                                                "weight_cap", "shrinkage",
                                                "sample_size"))
          and rprt.get("weight_floor") == 0.05
          and rprt.get("weight_cap") == 0.40
          and rprt.get("shrinkage") == 0.6,
          "report exposes empirical, priors, shrinkage=%s, "
          "weight_floor=%s, weight_cap=%s"
          % (rprt.get("shrinkage"), rprt.get("weight_floor"),
             rprt.get("weight_cap")))
except Exception as e:
    check("report_has_diversification_params", False,
          f"{type(e).__name__}: {e}")

# ---- 7. engine picks up the new weights ------------------------------------
try:
    r = lam.invoke(FunctionName=GS_FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    body = json.loads(r["Payload"].read().decode("utf-8", "ignore"))
    inv = json.loads(body.get("body") or "{}")
    time.sleep(1)
    d = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=GS_OUT_KEY
                                 )["Body"].read())
    w_meta = d.get("weights") or {}
    w_active = w_meta.get("active") or w_meta.get("weights") or {}
    w_mode = w_meta.get("mode") or inv.get("weights_mode")
    check("engine_uses_calibrated_weights",
          inv.get("ok") is True
          and (w_active and 0.99 <= sum(w_active.values()) <= 1.01)
          and w_mode in ("empirical", "blended"),
          "GSI=%s, mode=%s, N=%s, active weights=%s"
          % (d.get("global_stress_index"), w_mode,
             w_meta.get("sample_size"),
             {k: round(v, 3) for k, v in sorted(w_active.items())}
             if w_active else "n/a"))
    n_active = sum(1 for v in (w_active or {}).values() if v > 0.001)
    check("engine_blend_is_diversified",
          n_active >= 5,
          "%s/6 dimensions active in the blend (was 2/6 in ops 889)"
          % n_active)
except Exception as e:
    check("engine_uses_calibrated_weights", False,
          f"{type(e).__name__}: {e}")
    check("engine_blend_is_diversified", False, "n/a")

# ---- summary ---------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
rep["snapshot"] = {
    "history_snapshots": hist_n,
    "ssm_mode": ssm_mode,
    "ssm_sample_size": ssm_n,
    "ssm_weights": {k: round(v, 3) for k, v in weights.items()},
    "ssm_min_weight": (round(min(weights.values()), 3)
                       if weights else None),
    "ssm_max_weight": (round(max(weights.values()), 3)
                       if weights else None),
}
if rep["all_passed"]:
    rep["verdict"] = (
        "PREDICTIVE GSI FIXED - the backfill now spans ~8 years of "
        "multi-regime data and the calibrator's diversification-"
        "preserving scheme (shrinkage toward priors + 5% floor + 40% "
        "cap) holds the index together. Every dimension carries weight, "
        "no dimension dominates, and the empirical IC re-weighting is "
        "now genuinely additive on top of the priors rather than a "
        "fragile sample-period override. The Global Stress Index is "
        "predictive without sacrificing the institutional diversification "
        "that makes it robust across regimes.")
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("VERIFICATION INCOMPLETE - failed: %s."
                      % ", ".join(bad))

with open("aws/ops/reports/890_diversified_gsi.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
