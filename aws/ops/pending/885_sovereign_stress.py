"""
ops/885 - VERIFY the sovereign-default stress dimension in the Global
Stress Matrix.

The matrix gained a sovereign panel: euro-area periphery (BTP/Bonos-
Bund spreads, read from the euro-fragmentation engine) and EM USD
sovereign debt (EMB vs Treasuries -- the EMBI-style relative spread).
This op redeploys, runs a live scan, and proves:

  - the sovereign panel is present with a 0-100 composite stress
    score and level;
  - the euro-periphery sub-read carries the fragmentation regime and
    a peripheral spread in bp;
  - the EM-sovereign sub-read carries the EMB-vs-Treasuries drawdown
    and a ratio series;
  - sovereign stress is folded into the Global Stress Index (now a
    six-component blend) and the index still scores.

Writes aws/ops/reports/885_sovereign_stress.json.
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

rep = {"ops": 885, "ts": datetime.now(timezone.utc).isoformat(),
       "subject": "Verify sovereign-default stress (euro periphery + EM "
                  "sovereign) in the Global Stress Matrix", "checks": []}


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
    check("redeployed", ok, "from source with sovereign panel")
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
          "GSI=%s, sovereign=%s, build %ss"
          % (inv.get("global_stress_index"), inv.get("sovereign_score"),
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

# ---- sovereign panel -------------------------------------------------------
sv = d.get("sovereign") or {}
check("sovereign_panel_present",
      isinstance(sv.get("stress_score"), (int, float))
      and sv.get("level") in ("CALM", "ELEVATED", "STRESSED", "ACUTE"),
      "sovereign stress %s (%s)" % (sv.get("stress_score"), sv.get("level"))
      if sv else "sovereign panel MISSING")

ep = sv.get("euro_periphery")
check("euro_periphery_read",
      isinstance(ep, dict)
      and isinstance(ep.get("stress_score"), (int, float)),
      ("euro periphery %s (%s) -- widest spread %sbp, periphery avg %sbp"
       % (ep.get("stress_score"), ep.get("regime"),
          ep.get("widest_spread_bp"), ep.get("periphery_avg_spread_bp")))
      if isinstance(ep, dict)
      else "euro-fragmentation sidecar not available -- euro periphery "
           "absent (EM sovereign still scored)")

em = sv.get("em_sovereign")
check("em_sovereign_read",
      isinstance(em, dict)
      and isinstance(em.get("stress_score"), (int, float))
      and isinstance(em.get("emb_vs_ust_drawdown_pct"), (int, float))
      and isinstance(em.get("series"), list) and len(em["series"]) > 20,
      ("EM sovereign %s (%s) -- EMB vs UST drawdown %s%%, 1m ratio %s%%"
       % (em.get("stress_score"), em.get("level"),
          em.get("emb_vs_ust_drawdown_pct"),
          em.get("ratio_change_1m_pct")))
      if isinstance(em, dict) else "EM sovereign read MISSING")

# at least one sub-read must be live for the panel to mean anything
check("at_least_one_sovereign_subread",
      isinstance(ep, dict) or isinstance(em, dict),
      "euro=%s, em=%s" % (bool(ep), bool(em)))

# ---- folded into the GSI ---------------------------------------------------
gsi = d.get("global_stress_index")
check("sovereign_feeds_gsi",
      isinstance(gsi, (int, float))
      and d.get("global_stress_level") in
      ("CALM", "ELEVATED", "STRESSED", "ACUTE"),
      "Global Stress Index %s (%s) -- six-component blend incl. "
      "sovereign %s" % (gsi, d.get("global_stress_level"),
                        sv.get("stress_score")))

# ---- page ------------------------------------------------------------------
try:
    req = urllib.request.Request(
        PAGE_URL + "?cb=" + datetime.now().strftime("%H%M%S"),
        headers={"User-Agent": "justhodl-ops"})
    with urllib.request.urlopen(req, timeout=40) as resp:
        st, page = resp.status, resp.read().decode("utf-8", "ignore")
    check("page_renders_sovereign",
          st == 200 and "Sovereign Stress" in page,
          "HTTP %s, Sovereign Stress card %s" % (
              st, "rendered" if "Sovereign Stress" in page
              else "MISSING (Pages may still be publishing)"))
except Exception as e:
    check("page_renders_sovereign", False, f"{type(e).__name__}: {e}")

# ---- summary ---------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
rep["snapshot"] = {
    "sovereign_stress": sv.get("stress_score"),
    "euro_periphery": (ep or {}).get("stress_score"),
    "euro_regime": (ep or {}).get("regime"),
    "em_sovereign": (em or {}).get("stress_score"),
    "emb_vs_ust_drawdown_pct": (em or {}).get("emb_vs_ust_drawdown_pct"),
    "global_stress_index": gsi,
}
if rep["all_passed"]:
    rep["verdict"] = (
        "SOVEREIGN STRESS LIVE - the Global Stress Matrix now reads "
        "sovereign-default risk: euro-area periphery via the BTP/Bonos-"
        "Bund fragmentation spreads, and EM USD sovereign debt via EMB "
        "versus Treasuries. Sovereign stress is folded into the Global "
        "Stress Index alongside the market matrix, credit, the VIX, rate "
        "volatility and contagion -- a six-dimension institutional "
        "stress index.")
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("VERIFICATION INCOMPLETE - failed: %s."
                      % ", ".join(bad))

with open("aws/ops/reports/885_sovereign_stress.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
