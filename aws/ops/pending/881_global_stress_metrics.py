"""
ops/881 - DEPLOY + VERIFY the enriched Global Stress Matrix.

The engine gained six institutional metrics: implied volatility (VIX),
credit spreads (HY/IG OAS), cross-market contagion, stress breadth,
stress momentum and safe-haven demand, and the Global Stress Index is
now a weighted blend of the market matrix + credit + implied vol +
contagion.

  1. Redeploy from the enriched source + config (FRED_KEY env).
  2. Invoke it -- a live scan that now also hits FRED for VIX and the
     BAML OAS series and FMP for gold.
  3. Read back data/global-stress.json and prove every new block is
     present and well-formed, and that the blended index + the
     matrix-only index are both reported.
  4. Confirm data/global-stress-history.json was written.

Writes aws/ops/reports/881_global_stress_metrics.json.
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
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
FN = "justhodl-global-stress"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json", encoding="utf-8"))

cfg = Config(read_timeout=320, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 881,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Verify the enriched Global Stress Matrix: implied vol, "
               "credit spreads, contagion, breadth, momentum, safe-haven",
    "checks": [],
}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})
    return ok


# ---- deploy ----------------------------------------------------------------
try:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", open(SRC, encoding="utf-8").read())
    lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue())
    for _ in range(60):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(3)
    lam.update_function_configuration(
        FunctionName=FN, Handler=CONF["handler"], Runtime=CONF["runtime"],
        Role=ROLE, Timeout=CONF["timeout"], MemorySize=CONF["memory"],
        Environment={"Variables": CONF.get("environment", {})},
        Description=CONF["description"][:255])
    for _ in range(60):
        c = lam.get_function_configuration(FunctionName=FN)
        if (c.get("LastUpdateStatus") == "Successful"
                and c.get("State") == "Active"):
            break
        time.sleep(3)
    env = c.get("Environment", {}).get("Variables", {})
    check("deployed", True, "redeployed; FRED_KEY set: %s"
          % bool(env.get("FRED_KEY")))
except Exception as e:
    check("deployed", False, f"{type(e).__name__}: {e}")

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
          r.get("FunctionError") or
          "GSI=%s matrix=%s VIX=%s credit=%s contagion=%s dir=%s in %ss"
          % (inv.get("global_stress_index"), inv.get("market_stress_index"),
             inv.get("vix"), inv.get("credit_composite"),
             inv.get("contagion_score"), inv.get("stress_direction"),
             inv.get("build_seconds")))
except Exception as e:
    check("invoke_ok", False, f"{type(e).__name__}: {e}")

time.sleep(2)

# ---- read back -------------------------------------------------------------
d = {}
try:
    d = json.loads(s3.get_object(Bucket=S3_BUCKET,
                   Key="data/global-stress.json")["Body"].read())
    check("output_readable", True, "schema %s" % d.get("schema_version"))
except Exception as e:
    check("output_readable", False, f"{type(e).__name__}: {e}")

# blended index + matrix index both present
check("blended_index_present",
      isinstance(d.get("global_stress_index"), (int, float))
      and isinstance(d.get("market_stress_index"), (int, float)),
      "Global Stress Index %s (blend) vs Market Matrix %s"
      % (d.get("global_stress_index"), d.get("market_stress_index")))

# implied vol
iv = d.get("implied_vol") or {}
check("implied_vol_ok",
      isinstance(iv.get("vix"), (int, float))
      and isinstance(iv.get("stress_score"), (int, float))
      and isinstance(iv.get("series"), list) and len(iv["series"]) > 20,
      "VIX %s, %sth %%ile, stress %s (%s)"
      % (iv.get("vix"), iv.get("percentile_1y"),
         iv.get("stress_score"), iv.get("level")))

# credit spreads
cr = d.get("credit_spreads") or {}
sp = {s.get("key"): s for s in (cr.get("spreads") or [])}
check("credit_spreads_ok",
      isinstance(cr.get("composite_score"), (int, float))
      and "hy" in sp and "ig" in sp
      and isinstance(sp["hy"].get("oas_pct"), (int, float)),
      "composite %s -- HY OAS %s%%, IG OAS %s%%"
      % (cr.get("composite_score"),
         sp.get("hy", {}).get("oas_pct"),
         sp.get("ig", {}).get("oas_pct")))

# contagion
cg = d.get("contagion") or {}
check("contagion_ok",
      isinstance(cg.get("avg_pairwise_correlation"), (int, float))
      and isinstance(cg.get("stress_score"), (int, float))
      and cg.get("pairs", 0) >= 10,
      "avg correlation %s, stress %s (%s), %s pairs"
      % (cg.get("avg_pairwise_correlation"), cg.get("stress_score"),
         cg.get("level"), cg.get("pairs")))

# breadth
br = d.get("breadth") or {}
check("breadth_ok",
      isinstance(br.get("markets"), int)
      and isinstance(br.get("elevated_plus"), int)
      and "acute" in br,
      "%s/%s markets ELEVATED+, %s stressed, %s acute"
      % (br.get("elevated_plus"), br.get("markets"),
         br.get("stressed_plus"), br.get("acute")))

# momentum (direction may be n/a on the very first run -- structure is
# what matters; history accrues over scheduled runs)
mo = d.get("stress_momentum") or {}
check("stress_momentum_ok",
      "direction" in mo and "change_5_runs" in mo
      and "runs_tracked" in mo,
      "direction %s, 5-run change %s, %s runs tracked"
      % (mo.get("direction"), mo.get("change_5_runs"),
         mo.get("runs_tracked")))

# safe haven
sh = d.get("safe_haven") or {}
check("safe_haven_ok",
      isinstance(sh.get("gold"), (int, float))
      and isinstance(sh.get("haven_demand_score"), (int, float)),
      "gold %s, 1m %s%%, haven demand %s"
      % (sh.get("gold"), sh.get("gold_1m_pct"),
         sh.get("haven_demand_score")))

# history file
try:
    hist = json.loads(s3.get_object(Bucket=S3_BUCKET,
                      Key="data/global-stress-history.json")["Body"].read())
    snaps = hist.get("snapshots") or []
    check("history_written",
          isinstance(snaps, list) and len(snaps) >= 1
          and "gsi" in snaps[-1],
          "%d snapshot(s) in global-stress-history.json" % len(snaps))
except Exception as e:
    check("history_written", False, f"{type(e).__name__}: {e}")

# ---- summary ---------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
rep["readings"] = {
    "global_stress_index": d.get("global_stress_index"),
    "market_stress_index": d.get("market_stress_index"),
    "vix": iv.get("vix"),
    "credit_composite": cr.get("composite_score"),
    "contagion_correlation": cg.get("avg_pairwise_correlation"),
    "breadth_elevated": "%s/%s" % (br.get("elevated_plus"),
                                   br.get("markets")),
    "stress_direction": mo.get("direction"),
    "gold_1m_pct": sh.get("gold_1m_pct"),
}
if rep["all_passed"]:
    rep["verdict"] = (
        "GLOBAL STRESS MATRIX ENRICHED + VERIFIED - the engine now blends "
        "six dimensions: the 10-market matrix, credit spreads (HY/IG OAS), "
        "implied volatility (VIX), cross-market contagion, breadth, stress "
        "momentum and safe-haven demand. All six compute live, the Global "
        "Stress Index is the weighted blend, and a rolling history is "
        "being kept. The crisis-composite / signal-board integration is "
        "untouched -- same key, richer number.")
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("ENRICHMENT VERIFICATION INCOMPLETE - failed: %s."
                      % ", ".join(bad))

with open("aws/ops/reports/881_global_stress_metrics.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
