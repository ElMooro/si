"""
ops/877 - REDEPLOY + FULL RE-VERIFY justhodl-chart-patterns.

ops/875 caught a real bug: the charting series was shorter than the
widest possible double-top span, so the first peak of a wide pattern
reindexed to a negative position. The engine has since been fixed
(the series now covers the full pattern span). This op redeploys the
Lambda from the corrected source, runs a fresh live S&P 500 scan, and
validates EVERY row in all four lists -- not just the first -- so the
fix is proven across the whole output:

  - crossover rows: direction / days_since / sma200 + a [date,close,ma]
    series of sane length;
  - double top / bottom rows: CONFIRMED|FORMING + integer quality, a
    [date,close] series, and EVERY peak/trough idx inside [0, len).

Writes aws/ops/reports/877_chart_patterns_reverify.json.
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
FN = "justhodl-chart-patterns"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
OUT_KEY = "data/chart-patterns.json"

cfg = Config(read_timeout=660, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 877,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Redeploy chart-patterns from the fixed source and validate "
               "every pattern row's marks land inside its chart series",
    "checks": [],
}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})
    return ok


# ---- redeploy from fixed source -------------------------------------------
try:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", open(SRC, encoding="utf-8").read())
    lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue())
    ok = False
    for _ in range(50):
        c = lam.get_function_configuration(FunctionName=FN)
        if (c.get("LastUpdateStatus") == "Successful"
                and c.get("State") == "Active"):
            ok = True
            break
        time.sleep(3)
    check("redeploy_ok", ok, "chart-patterns redeployed from fixed source")
except Exception as e:
    check("redeploy_ok", False, f"{type(e).__name__}: {e}")

# ---- fresh live scan -------------------------------------------------------
inv = {}
try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    body = json.loads(r["Payload"].read().decode("utf-8", "ignore"))
    inv = json.loads(body.get("body") or "{}")
    check("invoke_ok",
          r.get("StatusCode") == 200 and not r.get("FunctionError")
          and inv.get("ok") is True,
          "scanned %s, %s with a signal in %ss"
          % (inv.get("universe"), inv.get("n_with_signal"),
             inv.get("build_seconds")))
except Exception as e:
    check("invoke_ok", False, f"{type(e).__name__}: {e}")

time.sleep(2)

d = {}
try:
    d = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=OUT_KEY
                                 )["Body"].read())
    check("output_readable", True, "data/chart-patterns.json read")
except Exception as e:
    check("output_readable", False, f"{type(e).__name__}: {e}")

# ---- validate EVERY crossover row -----------------------------------------
cross = (d.get("cross_up_200dma") or []) + (d.get("cross_down_200dma") or [])
bad_cross = []
for r in cross:
    ser = r.get("series")
    good = (r.get("direction") in ("up", "down")
            and isinstance(r.get("days_since_cross"), int)
            and r.get("sma200") is not None
            and isinstance(ser, list) and len(ser) > 20
            and all(isinstance(p, list) and len(p) == 3 for p in ser))
    if not good:
        bad_cross.append(r.get("symbol"))
check("all_crossover_rows_well_formed", not bad_cross,
      "%d crossover rows checked, %d malformed%s"
      % (len(cross), len(bad_cross),
         "" if not bad_cross else ": " + ", ".join(bad_cross[:8])))

# ---- validate EVERY pattern row -- marks must land inside the series ------
bad_patt, checked = [], 0
for key, marks in (("double_tops", ("peak1", "peak2", "trough")),
                   ("double_bottoms", ("trough1", "trough2", "peak"))):
    for r in (d.get(key) or []):
        checked += 1
        ser = r.get("series")
        n = len(ser) if isinstance(ser, list) else 0
        good = (r.get("status") in ("CONFIRMED", "FORMING")
                and isinstance(r.get("quality"), int)
                and isinstance(ser, list) and n > 20
                and all(isinstance(p, list) and len(p) == 2 for p in ser))
        for m in marks:
            mk = r.get(m)
            if not (isinstance(mk, dict) and isinstance(mk.get("idx"), int)
                    and 0 <= mk["idx"] < n):
                good = False
        if not good:
            bad_patt.append("%s/%s" % (key, r.get("symbol")))
check("all_pattern_marks_in_range", not bad_patt,
      "%d pattern rows checked, %d malformed%s"
      % (checked, len(bad_patt),
         "" if not bad_patt else ": " + ", ".join(bad_patt[:8])))

# spot sample for the report
sample = None
for key in ("double_tops", "double_bottoms"):
    rows = d.get(key) or []
    if rows:
        r = rows[0]
        mk = "peak1" if key == "double_tops" else "trough1"
        sample = {"key": key, "symbol": r.get("symbol"),
                  "status": r.get("status"), "quality": r.get("quality"),
                  "series_len": len(r.get("series") or []),
                  "first_mark_idx": (r.get(mk) or {}).get("idx")}
        break
rep["sample_pattern"] = sample

# ---- summary ---------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
rep["scan"] = {"universe_size": d.get("universe_size"),
               "n_with_signal": d.get("n_with_signal"),
               "counts": d.get("counts")}
if rep["all_passed"]:
    rep["verdict"] = (
        "CHART PATTERN SCANNER FULLY VERIFIED - the series-length fix "
        "holds across the whole output: every crossover series is a "
        "[date,close,ma] triple and every double top/bottom peak and "
        "trough lands inside its own chart window. The scanner is live, "
        "scheduled daily, and the protected Stock Screener is untouched.")
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("RE-VERIFY INCOMPLETE - failed: %s." % ", ".join(bad))

with open("aws/ops/reports/877_chart_patterns_reverify.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
