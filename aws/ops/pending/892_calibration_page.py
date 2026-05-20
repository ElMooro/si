"""
ops/892 - VERIFY the GSI calibration dashboard page is live and the
calibration data it reads is well-formed.

Re-runs the calibrator to make sure data/gsi-calibration.json is
fresh, then fetches the live page and confirms:

  - data/gsi-calibration.json carries weights, IC per dim, sample
    size, mode, calibrated_at, paired_observations, methodology;
  - the page renders with the calibration shell (hero, IC table,
    weights bar chart, scatter grid, methodology note);
  - the page is wired into the directory and the landing page.

Writes aws/ops/reports/892_calibration_page.json.
"""
import json
import time
import urllib.request
from datetime import datetime, timezone

import boto3
from botocore.config import Config

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
CAL_FN = "justhodl-gsi-calibrator"
REPORT_KEY = "data/gsi-calibration.json"
PAGE_URL = "https://justhodl.ai/gsi-calibration.html"
DIRECTORY_URL = "https://justhodl.ai/directory.html"
INDEX_URL = "https://justhodl.ai/"

cfg = Config(read_timeout=300, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {"ops": 892, "ts": datetime.now(timezone.utc).isoformat(),
       "subject": "Verify the GSI calibration dashboard page",
       "checks": []}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})


def fetch(url):
    req = urllib.request.Request(
        url + ("&" if "?" in url else "?")
        + "cb=" + datetime.now().strftime("%H%M%S"),
        headers={"User-Agent": "justhodl-ops"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.status, resp.read().decode("utf-8", "ignore")


# ---- 1. re-run the calibrator so the report is fresh ----------------------
try:
    r = lam.invoke(FunctionName=CAL_FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    body = json.loads(r["Payload"].read().decode("utf-8", "ignore"))
    inv = json.loads(body.get("body") or "{}")
    check("calibrator_rerun_ok",
          r.get("StatusCode") == 200 and not r.get("FunctionError")
          and inv.get("ok") is True,
          "mode=%s, N=%s" % (inv.get("mode"), inv.get("sample_size")))
except Exception as e:
    check("calibrator_rerun_ok", False, f"{type(e).__name__}: {e}")

time.sleep(2)

# ---- 2. calibration JSON well-formed -------------------------------------
report = {}
try:
    report = json.loads(s3.get_object(Bucket=S3_BUCKET,
                                      Key=REPORT_KEY)["Body"].read())
    needed_keys = {"weights", "empirical_weights", "priors", "ic",
                   "n_by_dim", "sample_size", "mode", "calibrated_at",
                   "paired_observations", "methodology"}
    missing = needed_keys - set(report.keys())
    n_paired = len(report.get("paired_observations") or [])
    check("calibration_json_well_formed",
          not missing and n_paired >= 10,
          "all expected keys present, %d paired obs, mode=%s, N=%s"
          % (n_paired, report.get("mode"), report.get("sample_size"))
          if not missing else "missing keys: " + ", ".join(sorted(missing)))
except Exception as e:
    check("calibration_json_well_formed", False,
          f"{type(e).__name__}: {e}")

# ---- 3. paired-observations shape ----------------------------------------
paired = report.get("paired_observations") or []
sample = paired[0] if paired else {}
shape_ok = (isinstance(sample.get("date"), str)
            and isinstance(sample.get("drawdown_21d_pct"), (int, float))
            and isinstance(sample.get("dims"), dict)
            and any(isinstance(v, (int, float))
                    for v in sample["dims"].values()))
check("paired_obs_shape_correct", shape_ok,
      ("first row: date=%s, drawdown=%.2f%%, %d dims"
       % (sample.get("date"), sample.get("drawdown_21d_pct"),
          len(sample.get("dims") or {})))
      if shape_ok else "first paired-obs row: %s" % str(sample)[:160])

# ---- 4. the page renders the calibration shell ---------------------------
try:
    st, page = fetch(PAGE_URL)
    markers = ["GSI Calibration", "renderHero", "Information Coefficient",
               "Prior vs Active Weights", "Paired Observations",
               "Methodology", "gsi-calibration.json"]
    missing = [m for m in markers if m not in page]
    check("page_renders_calibration_shell",
          st == 200 and not missing,
          ("HTTP %s, all %d shell markers present" % (st, len(markers)))
          if st == 200 and not missing
          else "HTTP %s, missing: %s" % (st, ", ".join(missing)))
except Exception as e:
    check("page_renders_calibration_shell", False,
          f"{type(e).__name__}: {e}")

# ---- 5. directory + landing page link to it ------------------------------
try:
    st, dirp = fetch(DIRECTORY_URL)
    check("directory_links_to_calibration",
          st == 200 and "gsi-calibration.html" in dirp,
          "directory.html %s, link %s"
          % (st, "present" if "gsi-calibration.html" in dirp
             else "MISSING"))
except Exception as e:
    check("directory_links_to_calibration", False,
          f"{type(e).__name__}: {e}")

try:
    st, idx = fetch(INDEX_URL)
    check("landing_links_to_calibration",
          st == 200 and "gsi-calibration.html" in idx
          and "GSI CALIBRATION" in idx,
          "index %s, GSI CALIBRATION card %s"
          % (st, "rendered" if "GSI CALIBRATION" in idx
             else "MISSING (pages may still be publishing)"))
except Exception as e:
    check("landing_links_to_calibration", False,
          f"{type(e).__name__}: {e}")

# ---- summary --------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
rep["snapshot"] = {
    "mode": report.get("mode"),
    "sample_size": report.get("sample_size"),
    "n_paired_observations": len(report.get("paired_observations") or []),
    "active_weights": report.get("weights"),
    "ic_per_dim": report.get("ic"),
    "calibrated_at": report.get("calibrated_at"),
}
if rep["all_passed"]:
    rep["verdict"] = (
        "GSI CALIBRATION DASHBOARD LIVE - the empirical reweighting of "
        "the Global Stress Index is no longer a black box. The page "
        "renders the per-dimension IC against 21-session forward SPY "
        "drawdown, the prior vs active weight bars, and the actual "
        "paired observations as scatter plots with OLS fit lines. "
        "Linked from the landing page (RISK, HEDGING & VOLATILITY) and "
        "the directory.")
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("VERIFICATION INCOMPLETE - failed: %s."
                      % ", ".join(bad))

with open("aws/ops/reports/892_calibration_page.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
