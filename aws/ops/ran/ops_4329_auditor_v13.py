"""ops_4329 -- auditor v1.3 verification: truncation must be False
(archives never fetched), the four known false-positives must vanish
(magic-formula rank, macro-nowcast/crisis-composite negatives), the
revived cluster must show OK, and the work-queue artifact must exist."""
import json, subprocess, sys, time
from datetime import datetime, timezone
import boto3
from ops_report import report
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
RUN_START = datetime.now(timezone.utc)
fails = []
with report("4329_auditor_v13") as r:
    r.heading("ops 4329 -- the auditor stops crying wolf")
    try:
        ts = subprocess.run(["git", "log", "-1", "--format=%ct",
                             "--",
                             "aws/lambdas/justhodl-fleet-auditor"],
                            capture_output=True, text=True,
                            timeout=30).stdout.strip()
        fl = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except Exception:
        fl = RUN_START
    ok = False
    for _ in range(50):
        try:
            c = lam.get_function_configuration(
                FunctionName="justhodl-fleet-auditor")
            lm = datetime.strptime(c["LastModified"].split(".")[0],
                                   "%Y-%m-%dT%H:%M:%S").replace(
                tzinfo=timezone.utc)
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and lm >= fl:
                ok = True
                break
        except Exception:
            pass
        time.sleep(9)
    if not ok:
        fails.append("deploy floor")
    else:
        try:
            g0 = json.loads(s3.get_object(
                Bucket=B, Key="data/fleet-audit.json"
            )["Body"].read()).get("generated_at")
        except Exception:
            g0 = None
        lam.invoke(FunctionName="justhodl-fleet-auditor",
                   InvocationType="Event", Payload=b"{}")
        d = None
        t0 = time.time()
        while time.time() - t0 < 700:
            time.sleep(20)
            try:
                cand = json.loads(s3.get_object(
                    Bucket=B, Key="data/fleet-audit.json"
                )["Body"].read())
                if cand.get("generated_at") != g0:
                    d = cand
                    break
            except Exception:
                pass
        if not d:
            fails.append("no changed sweep in 700s")
            d = {}
        r.ok("v%s SCANNED %s live in %ss · archives counted %s · "
             "truncated=%s"
             % (d.get("version"), d.get("n_scanned"),
                d.get("elapsed_s"), d.get("n_archive"),
                d.get("truncated")))
        r.log("by class: %s" % json.dumps(d.get("by_class")))
        if d.get("truncated"):
            fails.append("still truncating")
        off = {o["key"]: o for o in d.get("offenders") or []}
        for fp in ("data/magic-formula.json",
                   "data/macro-nowcast.json",
                   "data/crisis-composite.json"):
            fx = [f for f in (off.get(fp, {}).get("findings")
                              or []) if f["cls"] == "UNITS"]
            if fx:
                fails.append("false-positive persists: %s %s"
                             % (fp, fx[:1]))
            else:
                r.ok("FP cleared: %s" % fp)
        idx = {x["key"]: x for x in d.get("results_index") or []}
        alive = [k for k in ("data/credit-stress.json",
                             "data/global-macro.json",
                             "data/implied-prob.json")
                 if idx.get(k, {}).get("status") == "OK"]
        r.log("revived-cluster spot check OK: %s" % alive)
        if len(alive) < 2:
            fails.append("revived cluster not clean in sweep")
        try:
            q = json.loads(s3.get_object(
                Bucket=B, Key="data/fleet-audit-queue.json"
            )["Body"].read())
            r.ok("work queue: %d actionable items"
                 % len(q.get("queue") or []))
            for it in (q.get("queue") or [])[:6]:
                r.log("  %s [%s] %s"
                      % (it["key"], it["status"],
                         (it["findings"] or [{}])[0].get("msg",
                                                         "")[:90]))
        except Exception as e:
            fails.append("queue artifact: %s" % str(e)[:60])
    if fails:
        for f in fails:
            r.fail("  %s" % f)
        sys.exit(1)
    r.ok("OPS 4329 PASS -- signal without wolf-cries; drift is a "
         "named class; the queue is machine-readable")
