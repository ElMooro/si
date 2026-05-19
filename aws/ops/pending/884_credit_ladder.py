"""
ops/884 - VERIFY the ICE BofA credit-spread ladder in the Global
Stress Matrix.

The credit dimension was expanded from 2 series (HY + IG) to the full
ICE BofA OAS ladder: US HY Master, the BB / B / CCC & Lower junk
rating ladder, US Investment-Grade, and EM Corporate / EM High-Yield
Corporate -- with a derived CCC-vs-BB dispersion. This op redeploys,
runs a live scan, and proves:

  - all seven ICE BofA OAS indices scored, each with oas_pct,
    percentile and a 0-100 stress score;
  - the CCC & Lower (worst junk) and both EM series are present;
  - the CCC-vs-BB tier dispersion and worst-tier readouts are wired;
  - the credit composite still feeds the Global Stress Index;
  - the page renders the Credit Spread Ladder section.

Writes aws/ops/reports/884_credit_ladder.json.
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

cfg = Config(read_timeout=300, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {"ops": 884, "ts": datetime.now(timezone.utc).isoformat(),
       "subject": "Verify the ICE BofA credit-spread ladder in the Global "
                  "Stress Matrix", "checks": []}


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
    check("redeployed", ok, "from expanded source")
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
          "GSI=%s, credit composite=%s, build %ss"
          % (inv.get("global_stress_index"), inv.get("credit_composite"),
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

# ---- the seven ICE BofA OAS indices ----------------------------------------
cr = d.get("credit_spreads") or {}
spreads = cr.get("spreads") or []
by_key = {s.get("key"): s for s in spreads}
want = {"hy", "hy_bb", "hy_b", "hy_ccc", "ig", "em_corp", "em_hy"}
check("seven_oas_indices",
      want.issubset(set(by_key.keys())),
      "%d indices: %s" % (len(spreads),
                          ", ".join("%s %s%%" % (k, by_key[k].get("oas_pct"))
                                    for k in by_key)))

# every spread row well-formed
bad = [s.get("key") for s in spreads
       if not (isinstance(s.get("oas_pct"), (int, float))
               and isinstance(s.get("stress_score"), (int, float))
               and s.get("level") in ("CALM", "ELEVATED", "STRESSED",
                                       "ACUTE")
               and isinstance(s.get("series"), list)
               and len(s["series"]) > 20)]
check("all_spread_rows_well_formed", not bad,
      "all %d rows well-formed" % len(spreads) if not bad
      else "malformed: " + ", ".join(map(str, bad)))

# the worst junk + EM specifically present
ccc = by_key.get("hy_ccc")
check("ccc_worst_junk_present",
      isinstance(ccc, dict) and ccc.get("oas_pct") is not None,
      "CCC & Lower OAS %s%% -- stress %s (%s)"
      % (ccc.get("oas_pct"), ccc.get("stress_score"), ccc.get("level"))
      if ccc else "CCC & Lower MISSING")
check("em_credit_present",
      "em_corp" in by_key and "em_hy" in by_key,
      "EM Corporate %s%%, EM HY Corporate %s%%"
      % ((by_key.get("em_corp") or {}).get("oas_pct"),
         (by_key.get("em_hy") or {}).get("oas_pct")))

# ---- derived metrics -------------------------------------------------------
disp = cr.get("tier_dispersion") or {}
check("ccc_bb_dispersion_wired",
      isinstance(disp.get("ccc_minus_bb_pct"), (int, float))
      and disp.get("level") in ("CALM", "ELEVATED", "STRESSED", "ACUTE"),
      "CCC-BB dispersion %s%% -- %s (stress %s)"
      % (disp.get("ccc_minus_bb_pct"), disp.get("level"),
         disp.get("stress_score")))
wt = cr.get("worst_tier") or {}
check("worst_tier_wired",
      bool(wt.get("name")) and wt.get("oas_pct") is not None,
      "worst tier: %s at %s%% (%s)"
      % (wt.get("name"), wt.get("oas_pct"), wt.get("level")))

# ---- credit still feeds the GSI -------------------------------------------
check("credit_feeds_gsi",
      isinstance(cr.get("composite_score"), (int, float))
      and isinstance(d.get("global_stress_index"), (int, float)),
      "credit composite %s -> Global Stress Index %s (%s)"
      % (cr.get("composite_score"), d.get("global_stress_index"),
         d.get("global_stress_level")))

# ---- page ------------------------------------------------------------------
try:
    req = urllib.request.Request(
        PAGE_URL + "?cb=" + datetime.now().strftime("%H%M%S"),
        headers={"User-Agent": "justhodl-ops"})
    with urllib.request.urlopen(req, timeout=40) as resp:
        st, page = resp.status, resp.read().decode("utf-8", "ignore")
    check("page_renders_credit_ladder",
          st == 200 and "Credit Spread Ladder" in page
          and "creditRow" in page,
          "HTTP %s, Credit Spread Ladder %s" % (
              st, "rendered" if "Credit Spread Ladder" in page
              else "MISSING (Pages may still be publishing)"))
except Exception as e:
    check("page_renders_credit_ladder", False, f"{type(e).__name__}: {e}")

# ---- summary ---------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
rep["snapshot"] = {
    "oas_indices": len(spreads),
    "credit_composite": cr.get("composite_score"),
    "ccc_oas": (ccc or {}).get("oas_pct"),
    "ccc_bb_dispersion": disp.get("ccc_minus_bb_pct"),
    "worst_tier": wt.get("name"),
    "global_stress_index": d.get("global_stress_index"),
}
if rep["all_passed"]:
    rep["verdict"] = (
        "ICE BofA CREDIT LADDER LIVE - the Global Stress Matrix now reads "
        "credit across seven option-adjusted spread indices: the US "
        "high-yield BB/B/CCC rating ladder, investment grade, and EM "
        "corporate / EM high-yield. The CCC-vs-BB dispersion flags "
        "distress concentrating in the weakest junk, and the credit "
        "composite feeds the Global Stress Index. Credit stress is now "
        "measured the way a credit desk measures it.")
else:
    bad_c = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("VERIFICATION INCOMPLETE - failed: %s."
                      % ", ".join(bad_c))

with open("aws/ops/reports/884_credit_ladder.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
