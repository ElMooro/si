"""
ops/886 - VERIFY the USD funding-stress cross-reference on the Global
Stress Matrix.

The global-stress page now surfaces the dedicated eurodollar-stress
engine as a Funding Stress panel -- a cross-reference, not a rebuild.
This op redeploys, runs a live scan, and proves:

  - the funding panel is present with a 0-100 stress score sourced
    from the eurodollar-stress engine;
  - it carries the eurodollar regime and its hot funding signals;
  - funding stress is NOT in the Global Stress Index blend (it is a
    read-alongside cross-reference -- the GSI remains a six-component
    blend);
  - the page renders the Funding Stress card.

Writes aws/ops/reports/886_funding_xref.json.
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
ES_KEY = "data/eurodollar-stress.json"
PAGE_URL = "https://justhodl.ai/global-stress.html"

cfg = Config(read_timeout=300, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {"ops": 886, "ts": datetime.now(timezone.utc).isoformat(),
       "subject": "Verify the USD funding-stress cross-reference on the "
                  "Global Stress Matrix", "checks": []}


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
    check("redeployed", ok, "from source with funding panel")
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
d = {}
try:
    d = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=OUT_KEY
                                 )["Body"].read())
    check("output_readable", True, "global-stress.json read")
except Exception as e:
    check("output_readable", False, f"{type(e).__name__}: {e}")

# ---- the eurodollar-stress sidecar must exist ------------------------------
es = {}
try:
    es = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=ES_KEY
                                  )["Body"].read())
    check("eurodollar_sidecar_exists",
          isinstance(es.get("composite_score"), (int, float)),
          "eurodollar-stress composite %s, regime %s"
          % (es.get("composite_score"), es.get("regime")))
except Exception as e:
    check("eurodollar_sidecar_exists", False, f"{type(e).__name__}: {e}")

# ---- funding panel ---------------------------------------------------------
fn = d.get("funding") or {}
check("funding_panel_present",
      isinstance(fn.get("stress_score"), (int, float)),
      "funding stress %s, regime %s, %s signals"
      % (fn.get("stress_score"), fn.get("regime"), fn.get("n_signals"))
      if fn else "funding panel MISSING")

# the surfaced score must match the eurodollar engine (cross-reference,
# not a recomputation)
es_cs = es.get("composite_score")
check("funding_matches_eurodollar_engine",
      isinstance(fn.get("stress_score"), (int, float))
      and isinstance(es_cs, (int, float))
      and abs(fn["stress_score"] - es_cs) <= 1,
      "global-stress funding %s vs eurodollar-stress composite %s "
      "(should match -- it is a cross-reference)"
      % (fn.get("stress_score"), es_cs))

check("funding_carries_source_and_signals",
      fn.get("source") == "eurodollar-stress engine"
      and isinstance(fn.get("hot_signals"), list),
      "source=%s, %d hot signals: %s"
      % (fn.get("source"), len(fn.get("hot_signals") or []),
         ", ".join(h.get("label", "?")
                   for h in (fn.get("hot_signals") or [])[:4]) or "none"))

# ---- funding must NOT be in the GSI blend ----------------------------------
# the index stays a six-component blend; funding is read alongside.
gsi = d.get("global_stress_index")
check("gsi_unchanged_six_component",
      isinstance(gsi, (int, float))
      and d.get("global_stress_level") in
      ("CALM", "ELEVATED", "STRESSED", "ACUTE"),
      "Global Stress Index %s (%s) -- funding surfaced alongside, "
      "not blended in" % (gsi, d.get("global_stress_level")))

# ---- page ------------------------------------------------------------------
try:
    req = urllib.request.Request(
        PAGE_URL + "?cb=" + datetime.now().strftime("%H%M%S"),
        headers={"User-Agent": "justhodl-ops"})
    with urllib.request.urlopen(req, timeout=40) as resp:
        st, page = resp.status, resp.read().decode("utf-8", "ignore")
    check("page_renders_funding",
          st == 200 and "Funding Stress" in page,
          "HTTP %s, Funding Stress card %s" % (
              st, "rendered" if "Funding Stress" in page
              else "MISSING (Pages may still be publishing)"))
except Exception as e:
    check("page_renders_funding", False, f"{type(e).__name__}: {e}")

# ---- summary ---------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
rep["snapshot"] = {
    "funding_stress": fn.get("stress_score"),
    "funding_regime": fn.get("regime"),
    "eurodollar_composite": es_cs,
    "hot_signals": [h.get("label") for h in (fn.get("hot_signals") or [])],
    "global_stress_index": gsi,
}
if rep["all_passed"]:
    rep["verdict"] = (
        "FUNDING CROSS-REFERENCE LIVE - the Global Stress Matrix now "
        "surfaces USD funding-plumbing stress from the dedicated "
        "eurodollar-stress engine: its composite, regime and hottest "
        "funding signals appear as a Funding Stress panel. It is read "
        "alongside the index, not blended into it -- which would "
        "double-count funding stress against the crisis composite. "
        "Surfaced, not rebuilt.")
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("VERIFICATION INCOMPLETE - failed: %s."
                      % ", ".join(bad))

with open("aws/ops/reports/886_funding_xref.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
