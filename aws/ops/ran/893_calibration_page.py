"""
ops/893 - VERIFY the GSI Calibration dashboard page is live and the
JSON it fetches contains everything the page needs.

Confirms:
  - GitHub Pages serves https://justhodl.ai/gsi-calibration.html (200);
  - the page contains the markers it should render (hero, IC table,
    prior-vs-active weight chart, paired-observations scatters,
    methodology);
  - data/gsi-calibration.json (the JSON the page fetches) has every
    field the page reads -- mode, sample_size, ic, n_by_dim, weights,
    priors, empirical_weights, paired_observations, methodology;
  - the page is linked from index.html and directory.html.

Writes aws/ops/reports/893_calibration_page.json.
"""
import json
import urllib.request
from datetime import datetime, timezone

import boto3
from botocore.config import Config

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
PAGE_URL = "https://justhodl.ai/gsi-calibration.html"
INDEX_URL = "https://justhodl.ai/"
DIR_URL = "https://justhodl.ai/directory.html"
CAL_JSON_KEY = "data/gsi-calibration.json"

cfg = Config(read_timeout=60, connect_timeout=20, retries={"max_attempts": 2})
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {"ops": 893, "ts": datetime.now(timezone.utc).isoformat(),
       "subject": "Verify the GSI Calibration dashboard page is live",
       "checks": []}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})


def fetch(url):
    req = urllib.request.Request(
        url + ("?cb=" + datetime.now().strftime("%H%M%S")
               if "?" not in url else "&cb=" + datetime.now().strftime("%H%M%S")),
        headers={"User-Agent": "justhodl-ops"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.status, r.read().decode("utf-8", "ignore")


# ---- 1. page renders ------------------------------------------------------
try:
    st, page = fetch(PAGE_URL)
    markers = ["GSI Calibration", "Information Coefficient",
               "Prior vs Active", "Paired Observations", "Methodology",
               "gsi-calibration.json"]
    missing = [m for m in markers if m not in page]
    check("page_serves",
          st == 200 and not missing,
          "HTTP %s, %d/%d markers present%s" % (
              st, len(markers) - len(missing), len(markers),
              "" if not missing else " (missing: " + ", ".join(missing) + ")"))
except Exception as e:
    check("page_serves", False, f"{type(e).__name__}: {e}")

# ---- 2. JSON well-formed (matches what the page reads) -------------------
try:
    d = json.loads(s3.get_object(Bucket=S3_BUCKET,
                                 Key=CAL_JSON_KEY)["Body"].read())
    needed_top = ("mode", "sample_size", "forward_days", "ic_floor",
                  "snapshots_total", "calibrated_at",
                  "earliest_snapshot", "latest_snapshot", "ic",
                  "n_by_dim", "weights", "priors", "empirical_weights",
                  "paired_observations", "methodology")
    missing_top = [k for k in needed_top if k not in d]
    needed_dims = {"market", "credit", "vix", "rate_vol", "contagion",
                   "sovereign"}
    ic_keys_ok = set((d.get("ic") or {}).keys()) >= needed_dims
    weight_keys_ok = set((d.get("weights") or {}).keys()) >= needed_dims
    paired = d.get("paired_observations") or []
    paired_ok = (len(paired) >= 10
                 and all("dims" in p and "drawdown_21d_pct" in p
                         and "date" in p
                         for p in paired[:5]))
    check("calibration_json_complete",
          not missing_top and ic_keys_ok and weight_keys_ok and paired_ok,
          "mode=%s, N=%s, %d paired obs, ic dims %s, weights dims %s, "
          "missing top fields: %s" % (
              d.get("mode"), d.get("sample_size"), len(paired),
              "ok" if ic_keys_ok else "MISSING",
              "ok" if weight_keys_ok else "MISSING",
              missing_top or "none"))
except Exception as e:
    check("calibration_json_complete", False, f"{type(e).__name__}: {e}")

# ---- 3. linked from index + directory ------------------------------------
try:
    _, idx = fetch(INDEX_URL)
    check("linked_from_index", "gsi-calibration.html" in idx
          or "/calibration/" in idx,
          "calibration link %s in /" % ("present" if (
              "gsi-calibration.html" in idx
              or "/calibration/" in idx) else "MISSING"))
except Exception as e:
    check("linked_from_index", False, f"{type(e).__name__}: {e}")

try:
    _, di = fetch(DIR_URL)
    check("linked_from_directory", "gsi-calibration.html" in di,
          "GSI Calibration link %s in /directory.html"
          % ("present" if "gsi-calibration.html" in di else "MISSING"))
except Exception as e:
    check("linked_from_directory", False, f"{type(e).__name__}: {e}")

# ---- summary --------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
if rep["all_passed"]:
    rep["verdict"] = (
        "GSI CALIBRATION PAGE LIVE - https://justhodl.ai/gsi-"
        "calibration.html renders the full calibrator report: the "
        "active mode, the per-dimension Spearman IC table, the prior-"
        "vs-active weight comparison chart, the paired-observations "
        "scatter grid (dimension score vs forward SPY drawdown) and "
        "the methodology. Linked from the landing page and the "
        "directory. The transparency loop is closed: the engine reads "
        "weights from SSM, the calibrator publishes both the SSM "
        "weights and the explanatory JSON, and the page renders why "
        "the GSI is weighted the way it is.")
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("VERIFICATION INCOMPLETE - failed: %s."
                      % ", ".join(bad))

with open("aws/ops/reports/893_calibration_page.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
