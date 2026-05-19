"""
ops/876 - RE-VERIFY two things:

  TASK 2 - the chart-patterns SERIES_BARS fix. ops 875 failed
           pattern_rows_well_formed because a double-top's first peak
           fell outside the 130-bar charting window and reindexed
           negative. SERIES_BARS is now 200. This op ships the fix,
           runs a live scan, and proves EVERY double-top and
           double-bottom row has all three marks landing inside its
           own series -- not just the first row.

  TASK 1 - the landing page. Confirms justhodl.ai now carries the
           three new product sections and every one of the 31 newly
           added engine/desk pages is linked.

Writes aws/ops/reports/876_patterns_and_landing.json.
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
FN = "justhodl-chart-patterns"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
OUT_KEY = "data/chart-patterns.json"
LANDING = "https://justhodl.ai/index.html"

NEW_PAGES = [
    "factor-risk", "firm-book", "firm-stress", "liquidity-capacity",
    "merger-arb-risk", "pnl-attribution", "risk-monitor", "risk-radar",
    "merger-arb", "pairs-arb", "pairs-scanner", "spinoff-desk",
    "index-recon", "dividend-growth", "conviction", "options-scanner",
    "signal-scorecard", "portfolio", "cross-asset", "analogs",
    "yen-carry", "cot-extremes", "activity-nowcast", "consumer-pulse",
    "dealer-survey", "narrative", "gdelt", "fleet-health", "benzinga",
    "eia", "nasdaq-datalink",
]
NEW_SECTIONS = ["FIRM RISK DESKS", "STRATEGY DESKS", "MACRO, INTEL"]

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 876,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Re-verify the chart-patterns SERIES_BARS fix and the "
               "31 new landing-page product cards",
    "checks": [],
}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})
    return ok


def http_get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops"})
    with urllib.request.urlopen(req, timeout=40) as r:
        return r.status, r.read().decode("utf-8", "ignore")


# ===== TASK 2 - chart-patterns fix =========================================
try:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", open(SRC, encoding="utf-8").read())
    lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue())
    ok = False
    for _ in range(40):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful" and c.get(
                "State") == "Active":
            ok = True
            break
        time.sleep(3)
    check("cp_deploy_ok", ok, "chart-patterns shipped")
except Exception as e:
    check("cp_deploy_ok", False, f"{type(e).__name__}: {e}")

inner = {}
try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    body = json.loads(r["Payload"].read().decode("utf-8", "ignore"))
    inner = json.loads(body.get("body") or "{}")
    check("cp_invoke_ok",
          r.get("StatusCode") == 200 and not r.get("FunctionError"),
          r.get("FunctionError") or "scan ok")
except Exception as e:
    check("cp_invoke_ok", False, f"{type(e).__name__}: {e}")

time.sleep(2)
cp = {}
try:
    cp = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=OUT_KEY
                                  )["Body"].read())
except Exception as e:
    check("cp_output_readable", False, f"{type(e).__name__}: {e}")

lists = {k: cp.get(k) or [] for k in
         ("cross_up_200dma", "cross_down_200dma",
          "double_tops", "double_bottoms")}
counts = {k: len(v) for k, v in lists.items()}
check("cp_four_lists_present",
      all(isinstance(lists[k], list) for k in lists),
      "counts: " + ", ".join("%s=%d" % (k, counts[k]) for k in counts))

# THE FIX: every double-top / double-bottom mark must land in its series
bad_rows, n_pat = [], 0
for key, marks in (("double_tops", ("peak1", "peak2", "trough")),
                   ("double_bottoms", ("trough1", "trough2", "peak"))):
    for row in lists[key]:
        n_pat += 1
        ser = row.get("series")
        slen = len(ser) if isinstance(ser, list) else 0
        for m in marks:
            mk = row.get(m)
            if not (isinstance(mk, dict)
                    and isinstance(mk.get("idx"), int)
                    and 0 <= mk["idx"] < slen):
                bad_rows.append("%s/%s %s idx=%s slen=%d" % (
                    key, row.get("symbol"), m,
                    (mk or {}).get("idx") if isinstance(mk, dict) else mk,
                    slen))
check("cp_all_pattern_marks_in_range", not bad_rows,
      "all %d double-top/bottom rows: every mark lands inside its series"
      % n_pat if not bad_rows
      else "%d bad: %s" % (len(bad_rows), "; ".join(bad_rows[:4])))

# spot-check the worst-case row: widest peak1->peak2 separation
widest = None
for row in lists["double_tops"]:
    p1, p2 = row.get("peak1") or {}, row.get("peak2") or {}
    if isinstance(p1.get("idx"), int) and isinstance(p2.get("idx"), int):
        span = p2["idx"] - p1["idx"]
        if widest is None or span > widest[1]:
            widest = (row.get("symbol"), span, len(row.get("series") or []))
check("cp_widest_pattern_fits",
      widest is None or (widest[1] < widest[2]),
      "no double tops to check" if widest is None
      else "widest top %s spans %d bars in a %d-pt series"
      % (widest[0], widest[1], widest[2]))

try:
    status, _ = http_get("https://justhodl.ai/chart-patterns.html")
    check("cp_page_live", status == 200, "HTTP %s" % status)
except Exception as e:
    check("cp_page_live", False, f"{type(e).__name__}: {e}")

# ===== TASK 1 - landing page ===============================================
try:
    status, page = http_get(LANDING)
    sec_missing = [s for s in NEW_SECTIONS if s not in page]
    check("landing_new_sections_present",
          status == 200 and not sec_missing,
          "HTTP %s, all 3 new sections present" % status if not sec_missing
          else "missing sections: " + ", ".join(sec_missing))
    link_missing = [p for p in NEW_PAGES if (p + ".html") not in page]
    check("landing_all_31_pages_linked", not link_missing,
          "all 31 new product pages linked from the landing page"
          if not link_missing
          else "%d not linked: %s" % (len(link_missing),
                                      ", ".join(link_missing)))
except Exception as e:
    check("landing_new_sections_present", False, f"{type(e).__name__}: {e}")
    check("landing_all_31_pages_linked", False, "landing fetch failed")

# ===== summary =============================================================
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
rep["state"] = {
    "pattern_counts": counts,
    "scanned": inner.get("universe_size") or inner.get("scanned"),
}
if rep["all_passed"]:
    rep["verdict"] = (
        "BOTH TASKS VERIFIED - the Chart Pattern Scanner fix holds: "
        "across %d double-top/bottom rows every peak and trough now "
        "lands inside its charting series (was failing on wide "
        "patterns). Live scan: %s. And the landing page carries the "
        "three new sections with all 31 product pages one click from "
        "the front door." % (n_pat, ", ".join(
            "%s %d" % (k, counts[k]) for k in counts)))
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("VERIFICATION INCOMPLETE - %d check(s) failed: %s."
                      % (len(bad), ", ".join(bad)))

with open("aws/ops/reports/876_patterns_and_landing.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
