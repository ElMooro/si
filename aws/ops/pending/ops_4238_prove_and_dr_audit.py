"""
ops_4238 — close the reconciler gate, arm the alarm, and establish the
TRUE state of disaster recovery.

Three jobs:

  A. Re-run the ops-4237 proof now that lambda-execution-role can read
     EventBridge Scheduler. The manifest must produce drift == 0 or it
     does not faithfully describe live AWS and must not be enforced.
     Also re-emit the manifest into a repo path the runner actually
     commits (aws/ops/audit/) — ops 4237 wrote it to config/, which is
     not in run-ops.yml's staged path list, so the git copy never
     existed. A source of truth that lives only in S3 is not a source
     of truth.

  B. Arm the JustHodl/Integrity DefectsNew alarm that ops 4236 could not
     create.

  C. AUDIT disaster recovery rather than assume it. aws/lambdas/
     justhodl-dr-snapshot/ exists in the repo and its docstring claims a
     daily 06:00 schedule and cross-region replication configured by an
     earlier op. None of that was verified. This section establishes,
     with evidence: is the function deployed, does a schedule target it,
     when did it last actually run, does the DR bucket exist, how recent
     is the newest manifest in it, is versioning on, and is replication
     configured. Repair follows the findings — it is not pre-judged.
"""
import io, json, os, time, zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"}, read_timeout=180)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
evb = boto3.client("events", region_name=REGION, config=CFG)
sch = boto3.client("scheduler", region_name=REGION, config=CFG)
cw  = boto3.client("cloudwatch", region_name=REGION, config=CFG)
s3  = boto3.client("s3", region_name=REGION, config=CFG)
ACCT = boto3.client("sts").get_caller_identity()["Account"]
ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
NOW = datetime.now(timezone.utc)
OUT = {"ops": 4238, "ts": NOW.isoformat()}

with report("4238_prove_and_dr_audit") as rep:
    rep.heading("ops 4238 — reconciler proof, alarm, DR ground truth")
    fails = []

    # ------------------------------------------------------------------ A
    rep.section("A. Reconciler proof gate (retry with new IAM)")
    try:
        r = lam.invoke(FunctionName="justhodl-schedule-reconciler",
                       InvocationType="RequestResponse",
                       Payload=json.dumps({"mode": "audit"}).encode())
        body = json.loads(r["Payload"].read() or b"{}")
        rep.log("reconciler -> %s" % json.dumps(body)[:400])
        if r.get("FunctionError"):
            fails.append("reconciler still erroring")
        elif body.get("drift_count") == 0:
            rep.ok("DRIFT = 0 — manifest faithfully describes live AWS. "
                   "Safe to enforce.")
            rep.kv(section="reconciler", drift=0, mode=body.get("mode"))
        else:
            rep.warn("drift = %s" % body.get("drift_count"))
            for k, v in (body.get("by_class") or {}).items():
                rep.warn("   %s: %s" % (k, v))
            try:
                d = json.loads(s3.get_object(
                    Bucket=BUCKET, Key="data/schedule-drift.json"
                )["Body"].read())
                for x in d.get("drifts", [])[:15]:
                    rep.log("   %-16s %-42s %s"
                            % (x["drift"], x["key"][:42], x["detail"][:70]))
            except Exception:
                pass
            OUT["drift"] = body.get("by_class")
    except Exception as e:
        fails.append("reconciler: %s" % str(e)[:170])

    rep.log("mirroring the manifest into a committed repo path…")
    try:
        m = s3.get_object(Bucket=BUCKET,
                          Key="config/schedule-manifest.json")["Body"].read()
        p = ROOT / "aws" / "ops" / "audit" / "schedule-manifest.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(m)
        j = json.loads(m)
        rep.ok("manifest mirrored (%d rules + %d schedules) -> %s"
               % (len(j.get("rules", [])), len(j.get("schedules", [])),
                  "aws/ops/audit/schedule-manifest.json"))
    except Exception as e:
        fails.append("manifest mirror: %s" % str(e)[:150])

    # ------------------------------------------------------------------ B
    rep.section("B. Arm the integrity alarm")
    for name, ns, met, desc in (
        ("justhodl-integrity-new-defects", "JustHodl/Integrity",
         "DefectsNew", "A NEW fleet defect appeared since the accepted "
                       "baseline. Open /integrity.html."),
        ("justhodl-schedule-drift", "JustHodl/Schedules", "ScheduleDrift",
         "Live schedules diverged from the declared manifest."),
    ):
        try:
            cw.put_metric_alarm(
                AlarmName=name, AlarmDescription=desc, Namespace=ns,
                MetricName=met, Statistic="Maximum", Period=86400,
                EvaluationPeriods=1, Threshold=0,
                ComparisonOperator="GreaterThanThreshold",
                TreatMissingData="notBreaching")
            rep.ok("alarm %s armed" % name)
        except Exception as e:
            fails.append("alarm %s: %s" % (name, str(e)[:110]))

    # ------------------------------------------------------------------ C
    rep.section("C. Disaster recovery — GROUND TRUTH, not assumptions")
    dr = {"function": None, "schedules": [], "invocations_14d": 0,
          "bucket": None, "latest_manifest": None, "versioning": None,
          "replication": None, "objects_sampled": 0}
    FN = "justhodl-dr-snapshot"
    try:
        c = lam.get_function_configuration(FunctionName=FN)
        dr["function"] = {"exists": True, "runtime": c.get("Runtime"),
                          "timeout": c.get("Timeout"),
                          "memory": c.get("MemorySize"),
                          "last_modified": c.get("LastModified"),
                          "env": list(((c.get("Environment") or {}
                                        ).get("Variables") or {}).keys())}
        rep.ok("function DEPLOYED — modified %s, timeout %ss"
               % (c.get("LastModified"), c.get("Timeout")))
    except Exception:
        dr["function"] = {"exists": False}
        rep.fail("function NOT DEPLOYED — the repo has the code but AWS "
                 "does not have the function")

    try:
        for page in evb.get_paginator("list_rules").paginate():
            for r in page["Rules"]:
                try:
                    tg = evb.list_targets_by_rule(Rule=r["Name"])["Targets"]
                except Exception:
                    continue
                if any(FN in (t.get("Arn") or "") for t in tg):
                    dr["schedules"].append(
                        {"kind": "events", "name": r["Name"],
                         "expr": r.get("ScheduleExpression"),
                         "state": r.get("State")})
        for page in sch.get_paginator("list_schedules").paginate():
            for s_ in page["Schedules"]:
                g = s_.get("GroupName", "default")
                d = sch.get_schedule(Name=s_["Name"], GroupName=g)
                if FN in ((d.get("Target") or {}).get("Arn") or ""):
                    dr["schedules"].append(
                        {"kind": "scheduler", "name": s_["Name"],
                         "expr": d.get("ScheduleExpression"),
                         "state": d.get("State")})
    except Exception as e:
        rep.warn("schedule scan: %s" % str(e)[:120])
    if dr["schedules"]:
        for s_ in dr["schedules"]:
            rep.ok("schedule %s %s (%s)" % (s_["name"], s_["expr"],
                                            s_["state"]))
    else:
        rep.fail("NO SCHEDULE TARGETS %s — the engine exists and never "
                 "runs. This is the gap." % FN)

    try:
        r = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": FN}],
            StartTime=NOW - timedelta(days=14), EndTime=NOW,
            Period=1209600, Statistics=["Sum"])
        dr["invocations_14d"] = int(sum(p["Sum"] for p in
                                        r.get("Datapoints", [])))
    except Exception:
        pass
    rep.log("invocations in the last 14 days: %d" % dr["invocations_14d"])

    cand = []
    try:
        for b in s3.list_buckets()["Buckets"]:
            n = b["Name"]
            if "dr" in n or "backup" in n or "disaster" in n:
                cand.append(n)
    except Exception as e:
        rep.warn("bucket list: %s" % str(e)[:110])
    rep.log("candidate DR buckets: %s" % (", ".join(cand) or "NONE"))
    for n in cand:
        info = {"name": n}
        try:
            info["versioning"] = s3.get_bucket_versioning(
                Bucket=n).get("Status", "Disabled")
        except Exception:
            info["versioning"] = "unknown"
        try:
            rc = s3.get_bucket_replication(Bucket=n)
            info["replication"] = [
                {"status": x.get("Status"),
                 "dest": (x.get("Destination") or {}).get("Bucket")}
                for x in rc["ReplicationConfiguration"]["Rules"]]
        except Exception:
            info["replication"] = None
        newest, count = None, 0
        try:
            for page in s3.get_paginator("list_objects_v2").paginate(
                    Bucket=n, PaginationConfig={"MaxItems": 3000}):
                for o in page.get("Contents", []):
                    count += 1
                    if newest is None or o["LastModified"] > newest:
                        newest = o["LastModified"]
        except Exception as e:
            info["list_error"] = str(e)[:90]
        info["objects_sampled"] = count
        info["newest_object"] = str(newest)[:19] if newest else None
        age_d = ((NOW - newest).days if newest else None)
        info["newest_age_days"] = age_d
        rep.log("  %-34s versioning=%s replication=%s objects~%d newest=%s"
                % (n[:34], info["versioning"],
                   "YES" if info["replication"] else "NO", count,
                   info["newest_object"]))
        if age_d is not None:
            (rep.ok if age_d <= 2 else rep.fail)(
                "     newest backup object is %d day(s) old" % age_d)
        rep.kv(section="dr_bucket", bucket=n,
               versioning=info["versioning"],
               replication="YES" if info["replication"] else "NO",
               objects=count, newest=info["newest_object"],
               age_days=age_d)
        dr.setdefault("buckets", []).append(info)

    OUT["dr"] = dr
    (ROOT / "aws" / "ops" / "reports" / "4238_prove_and_dr_audit.json"
     ).write_text(json.dumps(OUT, indent=1, default=str), encoding="utf-8")

    rep.section("VERDICT")
    if not dr["schedules"] or dr["invocations_14d"] == 0:
        rep.fail("DR IS NOT RUNNING. Code present, execution absent — the "
                 "most dangerous shape a backup can take, because the repo "
                 "makes it look solved.")
    else:
        rep.ok("DR is executing; freshness reported above.")
    if fails:
        for f in fails:
            rep.fail("  %s" % f)
        raise SystemExit("FAILS: %s" % "; ".join(fails[:3]))
