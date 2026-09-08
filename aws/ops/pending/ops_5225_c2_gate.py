"""ops_5225 -- audit 2026-09-08 Release C2 gate: freshness monitor v2.0 (INST-13) + outcome-checker v4 (INST-12).

A. Invoke justhodl-fleet-freshness-monitor, read data/_freshness-monitor.json: version 2.0.0, coverage
   block (rules, truncated_rules, bodies_validated, expected_keys_checked), n_invalid_or_empty /
   n_source_stale / n_missing present; scoped results carry artifact_age_h AND (where a timestamp exists)
   source_age_h; report the top findings.
B. Invoke justhodl-outcome-checker, then scan justhodl-signals for records checked since the push:
   every window graded in this run carries outcome.marks provenance + graded_at_session, no window is
   finalised with correct=None except status UNSCOREABLE, and pending windows sit under _pending with an
   attempts counter. Reports the warehouse-vs-yahoo provenance split.
"""
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FAILS = []
T_PUSH = int(subprocess.run(["git", "log", "-1", "--format=%ct", "HEAD"], capture_output=True, text=True, cwd=ROOT).stdout.strip() or "0")
T_PUSH_ISO = datetime.fromtimestamp(T_PUSH, timezone.utc).isoformat()


def expect(cond, msg):
    (R.ok if cond else R.fail)(msg)
    if not cond:
        FAILS.append(msg)


def wait_deployed(lam, fn, budget=1500):
    t0 = time.time()
    cfg = None
    while time.time() - t0 < budget:
        cfg = lam.get_function_configuration(FunctionName=fn)
        lm = datetime.fromisoformat(cfg["LastModified"].replace("Z", "+00:00")).timestamp()
        if lm >= T_PUSH and cfg.get("LastUpdateStatus", "Successful") == "Successful":
            return cfg
        time.sleep(30)
    return cfg


with report("ops_5225_c2_gate") as R:
    R.heading("ops 5225 -- Release C2 gate: freshness monitor v2 + outcome-checker v4")
    lam = boto3.client("lambda", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)

    R.section("A. freshness monitor")
    cfg = wait_deployed(lam, "justhodl-fleet-freshness-monitor")
    R.log("LastModified %s" % cfg["LastModified"])
    r = lam.invoke(FunctionName="justhodl-fleet-freshness-monitor", InvocationType="RequestResponse", Payload=b"{}")
    body = r["Payload"].read()
    expect(r.get("StatusCode") == 200 and not r.get("FunctionError"), "monitor invoke ok (FunctionError=%s) %s" % (r.get("FunctionError"), body[:200]))
    st = json.loads(s3.get_object(Bucket=BUCKET, Key="data/_freshness-monitor.json")["Body"].read())
    expect(st.get("version") == "2.0.0", "state version 2.0.0 (got %s)" % st.get("version"))
    cov = st.get("coverage") or {}
    expect(all(k in cov for k in ("rules", "truncated_rules", "bodies_validated", "expected_keys_checked", "missing")), "coverage block complete: %s" % {k: cov.get(k) for k in ("keys_enumerated", "bodies_validated", "expected_keys_checked", "missing", "declared_absent_never_seen", "truncated_rules")})
    expect(all(k in st for k in ("n_invalid_or_empty", "n_source_stale", "n_missing", "semantics")), "new counters present: invalid/empty=%s source_stale=%s missing=%s" % (st.get("n_invalid_or_empty"), st.get("n_source_stale"), st.get("n_missing")))
    expect(cov.get("bodies_validated", 0) > 0, "bodies were validated (%s)" % cov.get("bodies_validated"))
    R.log("stale=%s fresh=%s invalid/empty=%s source_stale=%s missing=%s | truncated rules %s" % (st.get("n_stale"), st.get("n_fresh"), st.get("n_invalid_or_empty"), st.get("n_source_stale"), st.get("n_missing"), cov.get("truncated_rules")))
    for row in (st.get("invalid_or_empty") or [])[:8]:
        R.log("   INVALID/EMPTY %s: %s" % (row.get("key"), row.get("reason")))
    for row in (st.get("source_stale_top_50") or [])[:8]:
        R.log("   SOURCE_STALE %s: artifact %sh, source %sh (%s)" % (row.get("key"), row.get("artifact_age_h"), row.get("source_age_h"), row.get("source_ts_field")))
    for row in (st.get("missing") or [])[:8]:
        R.log("   MISSING %s (%s) last seen %s" % (row.get("key"), row.get("engine"), row.get("last_seen")))
    R.kv(step="freshness", stale=st.get("n_stale"), fresh=st.get("n_fresh"), invalid_empty=st.get("n_invalid_or_empty"), source_stale=st.get("n_source_stale"), missing=st.get("n_missing"), validated=cov.get("bodies_validated"))

    R.section("B. outcome-checker")
    cfg = wait_deployed(lam, "justhodl-outcome-checker")
    R.log("LastModified %s" % cfg["LastModified"])
    r = lam.invoke(FunctionName="justhodl-outcome-checker", InvocationType="RequestResponse", Payload=b"{}")
    body = r["Payload"].read()
    expect(r.get("StatusCode") == 200 and not r.get("FunctionError"), "checker invoke ok (FunctionError=%s) %s" % (r.get("FunctionError"), body[:200]))
    ddb = boto3.resource("dynamodb", region_name=REGION)
    tbl = ddb.Table("justhodl-signals")
    graded, pending, unscoreable, bad, prov = 0, 0, 0, [], {}
    scanned = 0
    kw = {}
    while True:
        page = tbl.scan(**kw)
        for it in page.get("Items", []):
            scanned += 1
            if str(it.get("last_checked") or "") < T_PUSH_ISO:
                continue
            outs = it.get("outcomes") or {}
            pend = outs.get("_pending") or {}
            for wk, p in pend.items():
                pending += 1
                if not p.get("attempts"):
                    bad.append((str(it.get("signal_id"))[:8], wk, "pending without attempts"))
            for wk, o in outs.items():
                if wk == "_pending" or not isinstance(o, dict):
                    continue
                if str(o.get("checked_at") or "") < T_PUSH_ISO:
                    continue
                if o.get("status") == "UNSCOREABLE":
                    unscoreable += 1
                    continue
                graded += 1
                marks = o.get("marks") or {}
                if not marks.get("asset") or not o.get("graded_at_session"):
                    bad.append((str(it.get("signal_id"))[:8], wk, "graded without mark provenance"))
                else:
                    pv = str((marks.get("asset") or {}).get("provider") or "?").split(":")[0]
                    prov[pv] = prov.get(pv, 0) + 1
                if o.get("correct") is None:
                    bad.append((str(it.get("signal_id"))[:8], wk, "correct=None finalised without UNSCOREABLE"))
        if "LastEvaluatedKey" in page:
            kw["ExclusiveStartKey"] = page["LastEvaluatedKey"]
        else:
            break
    R.log("scanned %d signals | graded this run %d | pending %d | unscoreable %d | provenance %s" % (scanned, graded, pending, unscoreable, prov))
    expect(not bad, "every window graded this run carries provenance and no zero-grade finalisation (%d violations) %s" % (len(bad), bad[:5]))
    R.kv(step="outcome-checker", graded=graded, pending=pending, unscoreable=unscoreable, provenance=json.dumps(prov))

    R.section("verdict")
    for f in FAILS:
        R.fail(f)
    if FAILS:
        sys.exit(1)
    R.ok("GREEN -- freshness is judged on content and expected outputs; outcomes are graded at their own session with provenance")
