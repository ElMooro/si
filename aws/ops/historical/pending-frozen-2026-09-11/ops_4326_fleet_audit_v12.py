"""ops_4325 -- FLEET DATA-TRUTH AUDIT, first sweep. Deploys the
auditor born from today's forensics and runs it across every data/
artifact; this report IS the audit Khalid asked for: per-class counts,
worst offenders with exact findings, cross-artifact price
contradictions."""
import json, subprocess, sys, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from ops_report import report
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=900,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
RUN_START = datetime.now(timezone.utc)
fails = []
with report("4326_fleet_audit_v12") as r:
    r.heading("ops 4326 -- the whole fleet, on the table")
    try:
        ts = subprocess.run(
            ["git", "log", "-1", "--format=%ct", "--",
             "aws/lambdas/justhodl-fleet-auditor"],
            capture_output=True, text=True, timeout=30
        ).stdout.strip()
        fl = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except Exception:
        fl = RUN_START
    ok = False
    for _ in range(60):
        try:
            c = lam.get_function_configuration(
                FunctionName="justhodl-fleet-auditor")
            lm = datetime.strptime(
                c["LastModified"].split(".")[0],
                "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and lm >= fl:
                ok = True
                break
        except Exception:
            pass
        time.sleep(9)
    if not ok:
        fails.append("auditor never deployed")
    else:
        try:
            prev = json.loads(s3.get_object(
                Bucket="justhodl-dashboard-live",
                Key="data/fleet-audit.json")["Body"].read())
            g0 = prev.get("generated_at")
        except Exception:
            g0 = None
        lam.invoke(FunctionName="justhodl-fleet-auditor",
                   InvocationType="Event", Payload=b"{}")
        r.log("async sweep fired; polling for CHANGED artifact "
              "(prev generated_at=%s)" % g0)
        d = None
        t0 = time.time()
        while time.time() - t0 < 840:
            time.sleep(20)
            try:
                cand = json.loads(s3.get_object(
                    Bucket="justhodl-dashboard-live",
                    Key="data/fleet-audit.json")["Body"].read())
                if cand.get("generated_at") \
                        and cand["generated_at"] != g0:
                    d = cand
                    break
            except Exception:
                pass
        if d is None:
            fails.append("sweep artifact never changed in 840s")
            d = {}
        if d.get("truncated"):
            fails.append("truncated even after archive exemption")
        r.log("archives age-exempt: %s" % d.get("n_archive"))
        r.ok("SCANNED %s artifacts in %ss -- OK %s · WARN %s · "
             "FAIL %s · parse-fail %s"
             % (d.get("n_scanned"), d.get("elapsed_s"),
                d.get("n_ok"), d.get("n_warn"), d.get("n_fail"),
                d.get("n_parse_fail")))
        r.log("by class: %s" % json.dumps(d.get("by_class")))
        r.section("worst offenders (non-stale first)")
        for o in (d.get("offenders") or [])[:18]:
            r.log("%s [%s, %dh]" % (o["key"], o["status"],
                                    o["age_h"]))
            for f in o["findings"][:3]:
                r.log("    %s: %s" % (f["cls"], f["msg"]))
        r.section("cross-artifact price contradictions (>12%)")
        for c0 in (d.get("contradictions") or [])[:10]:
            r.log("%s spread %.1f%% -- %s"
                  % (c0["ticker"], c0["spread_pct"],
                     c0["obs"]))
        if (d.get("n_scanned") or 0) < 250:
            fails.append("scanned only %s" % d.get("n_scanned"))
    if fails:
        for f in fails:
            r.fail("  %s" % f)
        sys.exit(1)
    r.ok("OPS 4325 PASS -- the fleet now audits itself daily; "
         "today's findings are the work queue")

# retrigger: async+poll-for-change gate, engine v1.1 budget guard
