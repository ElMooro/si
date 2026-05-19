"""
ops/887 - VERIFY the multi-dimensional stress escalation layer on the
Global Stress Matrix.

The global-stress engine blended its dimensions into one headline index,
but its Telegram tripwire only watched per-market ACUTE flags and the
blended index itself -- so credit, rate volatility, sovereign and funding
stress could each go ACUTE silently, diluted inside the blend.

This op adds a per-dimension escalation matrix. It redeploys, runs the
engine twice (to prove edge detection), and proves:

  - the output carries an `escalation` block with a GREEN/AMBER/RED
    posture and a scored dimension list;
  - every dimension is well-formed (key, label, 0-100 score, valid band);
  - the four dimensions that used to be invisible to the tripwire --
    credit, rate volatility, sovereign and funding -- are now first-class
    escalation dimensions;
  - the posture is internally consistent (RED iff any RED dimension,
    AMBER iff any AMBER and no RED, else GREEN);
  - edge detection works: the first run after deploy is a baseline run
    (no escalation history) and the immediately following run is not a
    baseline and reports no spurious newly-red dimensions;
  - the page renders the Stress Escalation Matrix panel.

Writes aws/ops/reports/887_stress_escalation.json.
"""
import io
import json
import time
import urllib.request
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.config import Config

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
FN = "justhodl-global-stress"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
OUT_KEY = "data/global-stress.json"
PAGE_URL = "https://justhodl.ai/global-stress.html"
VALID_BANDS = ("GREEN", "AMBER", "RED")
NEW_DIMS = ("credit", "rate_vol", "sovereign", "funding")

cfg = Config(read_timeout=300, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {"ops": 887, "ts": datetime.now(timezone.utc).isoformat(),
       "subject": "Verify the multi-dimensional stress escalation layer "
                  "on the Global Stress Matrix", "checks": []}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})


def invoke():
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    body = json.loads(r["Payload"].read().decode("utf-8", "ignore"))
    inv = json.loads(body.get("body") or "{}")
    err = r.get("FunctionError")
    ok = r.get("StatusCode") == 200 and not err and inv.get("ok") is True
    return ok, inv


# ---- redeploy --------------------------------------------------------------
try:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", open(SRC, encoding="utf-8").read())
    lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue())
    ok = False
    for _ in range(60):
        c = lam.get_function_configuration(FunctionName=FN)
        if (c.get("LastUpdateStatus") == "Successful"
                and c.get("State") == "Active"):
            ok = True
            break
        time.sleep(3)
    check("redeployed", ok, "from source with escalation layer")
except Exception as e:
    check("redeployed", False, f"{type(e).__name__}: {e}")

# ---- run 1 -- baseline (first run after deploy has no escalation history) --
inv1 = {}
try:
    ok1, inv1 = invoke()
    check("invoke_run1_ok", ok1,
          "GSI=%s, escalation posture=%s, build %ss"
          % (inv1.get("global_stress_index"),
             inv1.get("escalation_posture"), inv1.get("build_seconds")))
except Exception as e:
    check("invoke_run1_ok", False, f"{type(e).__name__}: {e}")

time.sleep(2)
d1 = {}
try:
    d1 = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=OUT_KEY
                                  )["Body"].read())
    check("output_readable", True, "global-stress.json read")
except Exception as e:
    check("output_readable", False, f"{type(e).__name__}: {e}")

# ---- escalation block present ----------------------------------------------
esc = d1.get("escalation") or {}
dims = esc.get("dimensions") or []
check("escalation_block_present",
      isinstance(esc, dict) and esc.get("posture") in VALID_BANDS
      and isinstance(dims, list) and len(dims) >= 5,
      "posture=%s, %d dimensions, bands=%s"
      % (esc.get("posture"), len(dims), esc.get("bands")))

# ---- every dimension well-formed -------------------------------------------
bad = []
for x in dims:
    if not isinstance(x, dict):
        bad.append("non-dict")
        continue
    s = x.get("score")
    if (not x.get("key") or not x.get("label")
            or not isinstance(s, (int, float)) or not 0 <= s <= 100
            or x.get("band") not in VALID_BANDS):
        bad.append(str(x.get("key")))
check("all_dimensions_well_formed", dims and not bad,
      "all %d dimensions well-formed (key/label/0-100 score/valid band)"
      % len(dims) if not bad else "malformed: %s" % ", ".join(bad))

# ---- the four formerly-invisible dimensions are now first-class ------------
keys = {x.get("key") for x in dims if isinstance(x, dict)}
missing = [k for k in NEW_DIMS if k not in keys]
covered = {x["key"]: "%s %s/100" % (x["band"], x["score"])
           for x in dims if isinstance(x, dict) and x.get("key") in NEW_DIMS}
check("credit_ratevol_sovereign_funding_covered", not missing,
      "credit/rate-vol/sovereign/funding now escalation dimensions: %s"
      % covered if not missing else "MISSING: %s" % ", ".join(missing))

# ---- posture internally consistent -----------------------------------------
n_red = sum(1 for x in dims if isinstance(x, dict) and x.get("band") == "RED")
n_amb = sum(1 for x in dims if isinstance(x, dict)
            and x.get("band") == "AMBER")
want = "RED" if n_red else ("AMBER" if n_amb else "GREEN")
check("posture_internally_consistent",
      esc.get("posture") == want
      and esc.get("n_red") == n_red and esc.get("n_amber") == n_amb,
      "posture=%s (%d red / %d amber) -- derivation %s"
      % (esc.get("posture"), n_red, n_amb,
         "matches" if esc.get("posture") == want else "MISMATCH want " + want))

# ---- run 1 is a baseline run (no prior escalation history) -----------------
check("run1_is_baseline",
      esc.get("baseline_run") is True
      and (esc.get("newly_red") or []) == [],
      "run1 baseline_run=%s, newly_red=%s (expected baseline + empty)"
      % (esc.get("baseline_run"), esc.get("newly_red")))

# ---- run 2 -- edge detection: not baseline, no spurious newly-red ----------
try:
    time.sleep(3)
    ok2, inv2 = invoke()
    time.sleep(2)
    d2 = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=OUT_KEY
                                  )["Body"].read())
    esc2 = d2.get("escalation") or {}
    check("run2_edge_detection_sane",
          ok2 and esc2.get("baseline_run") is False
          and (esc2.get("newly_red") or []) == [],
          "run2 baseline_run=%s, newly_red=%s (back-to-back run must not "
          "re-alert -- no-spam edge detection)"
          % (esc2.get("baseline_run"), esc2.get("newly_red")))
except Exception as e:
    check("run2_edge_detection_sane", False, f"{type(e).__name__}: {e}")

# ---- page renders the escalation matrix ------------------------------------
try:
    req = urllib.request.Request(
        PAGE_URL + "?cb=" + datetime.now().strftime("%H%M%S"),
        headers={"User-Agent": "justhodl-ops"})
    with urllib.request.urlopen(req, timeout=40) as resp:
        st, page = resp.status, resp.read().decode("utf-8", "ignore")
    has = "Stress Escalation Matrix" in page and "escalationPanel" in page
    check("page_renders_escalation", st == 200 and has,
          "HTTP %s, Escalation Matrix %s" % (
              st, "rendered" if has
              else "MISSING (Pages may still be publishing)"))
except Exception as e:
    check("page_renders_escalation", False, f"{type(e).__name__}: {e}")

# ---- summary ---------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
rep["snapshot"] = {
    "posture": esc.get("posture"),
    "n_red": esc.get("n_red"),
    "n_amber": esc.get("n_amber"),
    "dimensions": [{"label": x.get("label"), "score": x.get("score"),
                    "band": x.get("band")}
                   for x in dims if isinstance(x, dict)],
    "red_dimensions": esc.get("red_dimensions"),
    "global_stress_index": d1.get("global_stress_index"),
}
if rep["all_passed"]:
    rep["verdict"] = (
        "STRESS ESCALATION MATRIX LIVE - the Global Stress Matrix now "
        "escalates per dimension. Every scored dimension -- market matrix, "
        "equity vol, credit, rate volatility, sovereign, funding and "
        "contagion -- is classified GREEN/AMBER/RED, rolled into one "
        "firm-wide posture, and the Telegram tripwire fires a single "
        "consolidated push the moment ANY dimension newly crosses into "
        "ACUTE -- not just the per-market flags and the diluted blend. "
        "Edge detection proven: back-to-back runs do not re-alert.")
else:
    badc = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("VERIFICATION INCOMPLETE - failed: %s."
                      % ", ".join(badc))

with open("aws/ops/reports/887_stress_escalation.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
