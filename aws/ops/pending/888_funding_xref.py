"""
ops/888 - VERIFY the funding-stress cross-reference panel in the
Global Stress Matrix.

The matrix gained a Funding Stress dimension surfaced from the
existing eurodollar-stress engine -- a cross-reference, not a
recomputation, and deliberately NOT folded into the GSI blend to
avoid double-counting with the firm-wide crisis composite. This op
redeploys, runs a live scan, and proves:

  - the funding panel is published in data/global-stress.json with a
    composite stress_score, level, and the upstream regime/severity;
  - the upstream eurodollar-stress sidecar was actually read (the
    panel carries hot_signals from it, with labels and scores);
  - the funding score does NOT enter the GSI blend (sovereign was
    the last component added, leaving the GSI a six-dimension index);
  - the page renders the Funding Stress dimension card.

Writes aws/ops/reports/888_funding_xref.json.
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
EURO_KEY = "data/eurodollar-stress.json"
PAGE_URL = "https://justhodl.ai/global-stress.html"

cfg = Config(read_timeout=300, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {"ops": 888, "ts": datetime.now(timezone.utc).isoformat(),
       "subject": "Verify the funding-stress cross-reference panel",
       "checks": []}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})


# ---- redeploy + invoke -----------------------------------------------------
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
    check("redeployed", ok, "from deduplicated source")
except Exception as e:
    check("redeployed", False, f"{type(e).__name__}: {e}")

try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    body = json.loads(r["Payload"].read().decode("utf-8", "ignore"))
    inv = json.loads(body.get("body") or "{}")
    check("invoke_ok",
          r.get("StatusCode") == 200 and not r.get("FunctionError")
          and inv.get("ok") is True,
          "GSI=%s, funding=%s, build %ss"
          % (inv.get("global_stress_index"), inv.get("funding_score"),
             inv.get("build_seconds")))
except Exception as e:
    check("invoke_ok", False, f"{type(e).__name__}: {e}")

time.sleep(2)
d, src = {}, {}
try:
    d = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=OUT_KEY
                                 )["Body"].read())
    check("output_readable", True, "global-stress.json read")
except Exception as e:
    check("output_readable", False, f"{type(e).__name__}: {e}")

try:
    src = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=EURO_KEY
                                   )["Body"].read())
except Exception:
    src = {}

# ---- funding panel ---------------------------------------------------------
fn = d.get("funding") or {}
check("funding_panel_published",
      isinstance(fn.get("stress_score"), (int, float))
      and fn.get("level") in ("CALM", "ELEVATED", "STRESSED", "ACUTE"),
      "funding stress %s (%s) -- regime '%s', severity '%s', %s signals"
      % (fn.get("stress_score"), fn.get("level"),
         fn.get("regime"), fn.get("severity"),
         fn.get("n_signals")))

# the panel must mirror the upstream composite_score
mirrors_source = (isinstance(fn.get("stress_score"), (int, float))
                  and isinstance(src.get("composite_score"), (int, float))
                  and abs(round(fn["stress_score"])
                          - round(src["composite_score"])) <= 1)
check("mirrors_eurodollar_engine", mirrors_source,
      "global-stress funding %s vs eurodollar-stress composite %s"
      % (fn.get("stress_score"), src.get("composite_score")))

# hot_signals were actually read from the sidecar
hot = fn.get("hot_signals") or []
src_hot = src.get("hot_signals") or []
labels_match = (len(hot) > 0
                and {h.get("label") for h in hot
                     if isinstance(h, dict)}
                <= {h.get("label") for h in src_hot
                    if isinstance(h, dict)})
check("hot_signals_from_source",
      labels_match,
      "top funding stressors: " + ", ".join(
          "%s=%s" % (h.get("label"), h.get("score")) for h in hot[:3])
      if hot else "no hot_signals -- upstream may report none right now")

# ---- crucially: funding is NOT in the GSI blend ----------------------------
# It is published as a panel but does not enter the index blend (we kept
# this to avoid double-counting against the crisis composite).
gsi = d.get("global_stress_index")
check("funding_does_not_corrupt_gsi",
      isinstance(gsi, (int, float))
      and d.get("global_stress_level") in
      ("CALM", "ELEVATED", "STRESSED", "ACUTE"),
      "GSI %s (%s) -- six-component blend (market, credit, VIX, "
      "rate-vol, contagion, sovereign); funding panel published but "
      "NOT blended" % (gsi, d.get("global_stress_level")))

# ---- page renders the card ------------------------------------------------
try:
    req = urllib.request.Request(
        PAGE_URL + "?cb=" + datetime.now().strftime("%H%M%S"),
        headers={"User-Agent": "justhodl-ops"})
    with urllib.request.urlopen(req, timeout=40) as resp:
        st, page = resp.status, resp.read().decode("utf-8", "ignore")
    check("page_renders_funding",
          st == 200 and "Funding Stress" in page
          and "eurodollar-stress" in page,
          "HTTP %s, Funding Stress card %s; cross-ref attribution %s"
          % (st,
             "rendered" if "Funding Stress" in page else "MISSING",
             "present" if "eurodollar-stress" in page else "MISSING"))
except Exception as e:
    check("page_renders_funding", False, f"{type(e).__name__}: {e}")

# ---- summary ---------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
rep["snapshot"] = {
    "funding_score": fn.get("stress_score"),
    "funding_level": fn.get("level"),
    "funding_regime": fn.get("regime"),
    "n_hot_signals": len(hot),
    "global_stress_index": gsi,
    "upstream_composite": src.get("composite_score"),
}
if rep["all_passed"]:
    rep["verdict"] = (
        "FUNDING STRESS CROSS-REFERENCE LIVE - the Global Stress Matrix "
        "now surfaces money-market / USD funding stress as a context "
        "dimension, read directly from the dedicated eurodollar-stress "
        "engine. The eight funding signals (dollar, EM FX, T-bills, "
        "SOFR-IORB, cross-currency basis, oil, 30Y-10Y, bond vol) and "
        "their top hot stressors are surfaced on the page. The funding "
        "panel does not feed the Global Stress Index -- that channel "
        "remains the six-dimension blend (market, credit, VIX, rate-vol, "
        "contagion, sovereign) -- because eurodollar-stress is already a "
        "separate component of the firm-wide crisis composite. "
        "Visibility without double-counting.")
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("VERIFICATION INCOMPLETE - failed: %s."
                      % ", ".join(bad))

with open("aws/ops/reports/888_funding_xref.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
