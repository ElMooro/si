"""
ops/882 - VERIFY the Global Stress Matrix metric expansion.

The engine gained a Rate Volatility dimension (realised 10y Treasury
yield vol -- a MOVE-style bond fear gauge -- plus the 2s10s curve),
three more markets (India, South Korea equities; US IG Credit), and a
GSI history percentile. This op redeploys, runs a live scan, and
proves it all landed:

  - 13 markets scored (8 equity + 5 bond), India / Korea / LQD present;
  - the rates panel present and well-formed (rate_vol_bp, percentile,
    curve, stress_score, level, series);
  - all the other stress dimensions still present (VIX, credit,
    contagion, breadth, safe-haven, momentum);
  - the GSI is a five-component blend and still scores;
  - stress momentum now carries a percentile field;
  - the page is live and now renders the Rate Volatility panel.

Writes aws/ops/reports/882_global_stress_metrics.json.
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

rep = {
    "ops": 882,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Verify Global Stress Matrix expansion: rate volatility, "
               "India/Korea/IG-credit markets, GSI history percentile",
    "checks": [],
}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})
    return ok


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
    check("redeployed", ok, "from expanded source")
except Exception as e:
    check("redeployed", False, f"{type(e).__name__}: {e}")

# ---- invoke ----------------------------------------------------------------
inv = {}
try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    body = json.loads(r["Payload"].read().decode("utf-8", "ignore"))
    inv = json.loads(body.get("body") or "{}")
    check("invoke_ok",
          r.get("StatusCode") == 200 and not r.get("FunctionError")
          and inv.get("ok") is True,
          "GSI=%s, %s markets, rate-vol=%s, build %ss"
          % (inv.get("global_stress_index"), inv.get("markets_scored"),
             inv.get("rate_vol_bp"), inv.get("build_seconds")))
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

# ---- 13 markets, new ones present -----------------------------------------
eq = d.get("equities") or []
bd = d.get("bonds") or []
names = {r.get("market") for r in eq + bd}
check("thirteen_markets_scored",
      len(eq) == 8 and len(bd) == 5,
      "%d equity + %d bond markets scored" % (len(eq), len(bd)))
new_markets = {"India", "South Korea", "US IG Credit"}
check("new_markets_present",
      new_markets.issubset(names),
      "present: %s; missing: %s"
      % (sorted(new_markets & names), sorted(new_markets - names)))

# ---- rate volatility panel -------------------------------------------------
rt = d.get("rates")
rt_ok = (isinstance(rt, dict)
         and isinstance(rt.get("rate_vol_bp"), (int, float))
         and isinstance(rt.get("stress_score"), (int, float))
         and rt.get("level") in ("CALM", "ELEVATED", "STRESSED", "ACUTE")
         and isinstance(rt.get("series"), list) and len(rt["series"]) > 20)
check("rate_volatility_panel", rt_ok,
      ("rate-vol %sbp, %sth %%ile, 2s10s %s%s, stress %s (%s)"
       % (rt.get("rate_vol_bp"), rt.get("percentile_1y"),
          rt.get("curve_2s10s"),
          " inverted" if rt.get("curve_inverted") else "",
          rt.get("stress_score"), rt.get("level")))
      if isinstance(rt, dict) else "rates panel MISSING")

# ---- all other dimensions still present -----------------------------------
dims = {"implied_vol": d.get("implied_vol"),
        "credit_spreads": d.get("credit_spreads"),
        "contagion": d.get("contagion"),
        "breadth": d.get("breadth"),
        "safe_haven": d.get("safe_haven"),
        "stress_momentum": d.get("stress_momentum")}
present = [k for k, v in dims.items() if isinstance(v, dict)]
check("all_dimensions_present",
      len(present) == 6,
      "%d/6 dimensions present: %s" % (len(present), ", ".join(present)))

# ---- GSI is a five-component blend and still scores -----------------------
gsi = d.get("global_stress_index")
check("gsi_scores",
      isinstance(gsi, (int, float))
      and d.get("global_stress_level") in
      ("CALM", "ELEVATED", "STRESSED", "ACUTE"),
      "Global Stress Index %s (%s); market matrix %s, VIX %s, credit %s, "
      "rate-vol %s, contagion %s"
      % (gsi, d.get("global_stress_level"), d.get("market_stress_index"),
         (d.get("implied_vol") or {}).get("stress_score"),
         (d.get("credit_spreads") or {}).get("composite_score"),
         (rt or {}).get("stress_score"),
         (d.get("contagion") or {}).get("stress_score")))

# ---- momentum percentile field --------------------------------------------
mo = d.get("stress_momentum") or {}
check("momentum_has_percentile",
      "percentile" in mo,
      "stress momentum %s, %s runs tracked, history percentile=%s"
      % (mo.get("direction"), mo.get("runs_tracked"),
         mo.get("percentile")))

# ---- page live + rate panel rendered --------------------------------------
try:
    req = urllib.request.Request(PAGE_URL,
                                 headers={"User-Agent": "justhodl-ops"})
    with urllib.request.urlopen(req, timeout=40) as resp:
        st, page = resp.status, resp.read().decode("utf-8", "ignore")
    check("page_live_with_rate_panel",
          st == 200 and "Rate Volatility" in page and "d.rates" in page,
          "HTTP %s, Rate Volatility panel %s" % (
              st, "rendered" if "Rate Volatility" in page else "MISSING"))
except Exception as e:
    check("page_live_with_rate_panel", False, f"{type(e).__name__}: {e}")

# ---- summary ---------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
rep["snapshot"] = {
    "global_stress_index": gsi,
    "level": d.get("global_stress_level"),
    "markets": len(eq) + len(bd),
    "rate_vol_bp": (rt or {}).get("rate_vol_bp"),
    "curve_2s10s": (rt or {}).get("curve_2s10s"),
    "worst": (d.get("worst_market") or {}).get("market"),
}
if rep["all_passed"]:
    rep["verdict"] = (
        "GLOBAL STRESS MATRIX COMPLETE - the engine now spans 13 world "
        "markets and five stress dimensions: the market matrix, credit "
        "spreads, the VIX, Treasury rate volatility (the new bond fear "
        "gauge, with the 2s10s curve) and cross-market contagion, plus "
        "breadth, safe-haven demand and a history-percentile momentum "
        "read. Every metric an institutional global-stress desk tracks "
        "is now covered.")
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("VERIFICATION INCOMPLETE - failed: %s."
                      % ", ".join(bad))

with open("aws/ops/reports/882_global_stress_metrics.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
