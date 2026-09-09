"""ops_5222 -- audit 2026-09-08 Release B1 gate: justhodl-risk-sizer v2.0 (FR-03/04/05).

Waits for the redeploy, invokes the sizer, then asserts on the LIVE risk/recommendations.json:
  - v "2.0", status OK|ENTRIES_BLOCKED|NO_IDEAS, authority + book + final_constraint_check present;
  - every recommended_size_pct <= 8.0, cluster totals <= 25.0, total <= available gross;
  - entries_allowed is False whenever the authority is not FRESH or forbids entries or the drawdown is UNKNOWN;
  - data/risk-sizer.json mirror identical; risk.html carries the authority strip at the edge.
"""
import json
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-risk-sizer"
FAILS = []
# anchored to this release's own commit (not HEAD): a later ops-only push must not move the reference past the deploy
_rel = subprocess.run(["git", "log", "--format=%ct", "-1", "--grep=Release B1 (ops 5222)"], capture_output=True, text=True, cwd=ROOT).stdout.strip()
T_PUSH = int(_rel or subprocess.run(["git", "log", "-1", "--format=%ct", "HEAD"], capture_output=True, text=True, cwd=ROOT).stdout.strip() or "0")


def expect(cond, msg):
    (R.ok if cond else R.fail)(msg)
    if not cond:
        FAILS.append(msg)


with report("ops_5222_risk_sizer_gate") as R:
    R.heading("ops 5222 -- Release B1 gate: risk-sizer v2.0 capital authority / single-name cap / drawdown brake")
    lam = boto3.client("lambda", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    t0 = time.time()
    cfg = None
    while time.time() - t0 < 1500:
        cfg = lam.get_function_configuration(FunctionName=FN)
        lm = datetime.fromisoformat(cfg["LastModified"].replace("Z", "+00:00")).timestamp()
        if lm >= T_PUSH and cfg.get("LastUpdateStatus", "Successful") == "Successful":
            break
        time.sleep(30)
    R.log("%s LastModified %s (push %s)" % (FN, cfg["LastModified"], datetime.fromtimestamp(T_PUSH, timezone.utc).isoformat()))
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
    body = json.loads(r["Payload"].read() or b"{}")
    R.log("invoke -> %s" % json.dumps(body)[:300])
    expect(r.get("StatusCode") == 200 and not r.get("FunctionError"), "invoke succeeded (FunctionError=%s)" % r.get("FunctionError"))
    doc = json.loads(s3.get_object(Bucket=BUCKET, Key="risk/recommendations.json")["Body"].read())
    mirror = json.loads(s3.get_object(Bucket=BUCKET, Key="data/risk-sizer.json")["Body"].read())
    expect(doc.get("v") == "2.0", "artifact v=2.0 (got %s)" % doc.get("v"))
    expect(doc.get("status") in ("OK", "ENTRIES_BLOCKED", "NO_IDEAS"), "status %s" % doc.get("status"))
    expect(mirror.get("as_of") == doc.get("as_of"), "data/risk-sizer.json mirror is the same run")
    a = doc.get("authority") or {}
    R.log("authority %s mode=%s cap=%s allows=%s age=%sh | book gross=%s available=%s | dd=%s (%s) | entries_allowed=%s | hold=%s" % (
        a.get("status"), a.get("mode"), a.get("exposure_cap_pct"), a.get("allows_new_entries"), a.get("age_h"),
        (doc.get("book") or {}).get("gross_pct"), (doc.get("book") or {}).get("available_gross_pct"),
        (doc.get("drawdown_status") or {}).get("current_dd_pct"), (doc.get("drawdown_status") or {}).get("status"), doc.get("entries_allowed"), doc.get("hold_reasons")))
    R.kv(step="risk-sizer", status=doc.get("status"), authority=a.get("status"), authority_cap=a.get("exposure_cap_pct"), entries_allowed=doc.get("entries_allowed"),
         n=len(doc.get("sized_recommendations") or []), total=(doc.get("summary") or {}).get("total_recommended_size_pct"))
    rows = doc.get("sized_recommendations") or []
    sizes = [float(x.get("recommended_size_pct") or 0) for x in rows]
    expect(all(v <= 8.0 + 1e-9 for v in sizes), "every size <= 8.0%% (max %s)" % (max(sizes) if sizes else None))
    cl = {}
    for x in rows:
        cl[x.get("cluster")] = cl.get(x.get("cluster"), 0.0) + float(x.get("recommended_size_pct") or 0)
    expect(all(v <= 25.0 + 0.01 for v in cl.values()), "every cluster <= 25%% (max %s)" % (max(cl.values()) if cl else None))
    avail = float(((doc.get("book") or {}).get("available_gross_pct")) or 0) if doc.get("status") != "NO_IDEAS" else 0.0
    expect(sum(sizes) <= avail + 0.01 or doc.get("status") == "NO_IDEAS", "total %.2f <= available gross %.2f" % (sum(sizes), avail))
    fc = doc.get("final_constraint_check") or {}
    expect(doc.get("status") == "NO_IDEAS" or (fc.get("single_name_ok") and fc.get("cluster_ok") and fc.get("gross_ok")), "final_constraint_check all ok: %s" % fc)
    must_block = a.get("status") != "FRESH" or a.get("allows_new_entries") is False or (doc.get("drawdown_status") or {}).get("status") == "UNKNOWN"
    expect((not must_block) or doc.get("entries_allowed") is False, "entries blocked whenever the authority is not fresh / forbids entries / drawdown unknown")
    if doc.get("entries_allowed") is False:
        expect(all(v == 0.0 for v in sizes), "all sizes zero while entries are blocked")
    # page marker at the edge
    ok_page = False
    t1 = time.time()
    while time.time() - t1 < 600:
        try:
            with urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/risk.html?v=%d" % int(time.time()), headers={"User-Agent": "ops5222", "Cache-Control": "no-cache"}), timeout=30) as rr:
                ok_page = b"jhAuthorityStrip" in rr.read()
        except Exception:
            ok_page = False
        if ok_page:
            break
        time.sleep(20)
    expect(ok_page, "risk.html carries the authority strip at the edge")
    R.section("verdict")
    for f in FAILS:
        R.fail(f)
    if FAILS:
        sys.exit(1)
    R.ok("GREEN -- risk-sizer obeys the binding authority, the published single-name cap and the drawdown brake")
