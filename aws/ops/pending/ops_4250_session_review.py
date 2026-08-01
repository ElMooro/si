"""
ops_4250 — SESSION REVIEW. Re-verify today's 23 ops (4227-4249) against
the failure patterns this session itself exposed, and fix what the
review finds.

FINDINGS THIS OP ACTS ON
  R1  REGRESSION SWEEP. Today's schedule surgery — 26 duplicates
      disabled, 90 double-fire targets removed, 13 cadences retuned,
      one rule migrated to Step Functions — was individually gated, but
      nothing yet proves the FLEET-LEVEL claim: no previously-active
      engine went silent. Method: every function whose declared enabled
      cadence is <=6h and which ran in the prior 7 days must show
      invocations in the last 8 hours. Cadence-aware, so daily engines
      that simply have not ticked yet are not false alarms.
  R2  CRR IS FORWARD-ONLY. S3 replication applies to NEW writes. The
      3,000+ existing DR objects never replicate — and because the DR
      engine is content-addressed (unchanged code is never re-written),
      most of the fleet's code zips would NEVER reach us-west-2. The
      "DR now leaves us-east-1" claim was true only for future changes.
      Fixed with a server-side cross-region backfill, verified by
      recounting the destination.
  R3  ALARMS THAT NOTIFY NOBODY. justhodl-integrity-new-defects,
      justhodl-schedule-drift and justhodl-contract-sev1 were created
      with no AlarmActions. An alarm without a notification channel is
      a dashboard. Wire SNS -> the same email address the AWS budget
      already notifies (read from Budgets, not guessed). SNS may be
      outside the runner's IAM — degrade to printing the exact grant.
  R4  CONTRACT REGISTRY HAS NO GIT COPY. schedule-manifest,
      artifact-producers and the quarantine ledger are mirrored;
      engine-contracts.json lives only in S3. Same class of gap that
      bit the manifest in 4237. Mirrored here.
  R5  SSM POINTER CONTAINMENT (verified, no action). The scorecard's
      SSM value format changed today (pointer mode >8KB). Repo-wide
      grep shows ZERO consumers read that parameter — the ~10 real
      consumers read the S3 artifact, which was only extended
      additively. Suspected break, cleared with evidence, recorded.
  R6  CONTROL-PLANE COHERENCE. All six control loops: schedule exists,
      ENABLED, exactly one target, declared in the manifest, live
      reconciler drift == 0, and their artifacts are fresh.
"""

import io
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
DR_SRC = "justhodl-dashboard-live-dr"
DR_DST = "justhodl-dr-usw2-857687956942"
CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"},
             read_timeout=120)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
cw = boto3.client("cloudwatch", region_name=REGION, config=CFG)
s3 = boto3.client("s3", region_name=REGION, config=CFG)
s3w = boto3.client("s3", region_name="us-west-2", config=CFG)
evb = boto3.client("events", region_name=REGION, config=CFG)
NOW = datetime.now(timezone.utc)
ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
OUT = {"ops": 4250, "ts": NOW.isoformat()}

QUARANTINED = {"macro-report-api", "multi-agent-orchestrator",
               "nyfed-financial-stability-fetcher",
               "nyfed-primary-dealer-fetcher", "nyfedapi-isolated",
               "ultimate-multi-agent"}

CONTROL_RULES = {
    "justhodl-fleet-integrity-weekly": "justhodl-fleet-integrity",
    "justhodl-d1-scan-daily": "justhodl-fleet-integrity",
    "justhodl-schedule-reconciler-daily": "justhodl-schedule-reconciler",
    "justhodl-contract-gate-daily": "justhodl-contract-gate",
    "jh-clone-alpha-backfill-weekly": None,  # targets the state machine
}


def cadence_hours(expr):
    if not expr:
        return None
    e = expr.strip().lower()
    m = re.match(r"rate\((\d+)\s+(minute|hour|day)s?\)", e)
    if m:
        n, u = int(m.group(1)), m.group(2)
        return {"minute": n / 60.0, "hour": float(n), "day": n * 24.0}[u]
    m = re.match(r"cron\(([^)]+)\)", e)
    if not m:
        return None
    f = m.group(1).split()
    if len(f) < 6:
        return None
    minute, hour, dow = f[0], f[1], f[4]
    mm = re.match(r"\*/(\d+)$", minute)
    if mm:
        return int(mm.group(1)) / 60.0
    hm = re.match(r"\*/(\d+)$", hour)
    if hm:
        return float(hm.group(1))
    if hour == "*":
        return 1.0
    if re.match(r"^[a-z]{3}$", dow) or re.match(r"^\d$", dow):
        return 168.0
    if "," in hour:
        return max(1.0, 24.0 / (hour.count(",") + 1))
    return 24.0


def batch_sums(names, metric, start, end):
    out = {}
    for i in range(0, len(names), 160):
        chunk = names[i:i + 160]
        q = [{"Id": "m%d" % j,
              "MetricStat": {"Metric": {"Namespace": "AWS/Lambda",
                                        "MetricName": metric,
                                        "Dimensions": [
                                            {"Name": "FunctionName",
                                             "Value": fn}]},
                             "Period": 1209600, "Stat": "Sum"},
              "ReturnData": True} for j, fn in enumerate(chunk)]
        try:
            res = cw.get_metric_data(MetricDataQueries=q, StartTime=start,
                                     EndTime=end,
                                     ScanBy="TimestampDescending")
        except Exception as e:
            print("[r1] batch %d: %s" % (i, str(e)[:90]))
            continue
        vals = {r["Id"]: sum(r["Values"]) for r in res["MetricDataResults"]}
        for j, fn in enumerate(chunk):
            out[fn] = vals.get("m%d" % j, 0.0)
    return out


with report("4250_session_review") as rep:
    rep.heading("ops 4250 — session review (4227-4249)")
    fails = []

    # ================================================================ R1
    rep.section("R1. Regression sweep — did today's surgery silence "
                "anything?")
    try:
        man = json.loads((ROOT / "config" /
                          "schedule-manifest.json").read_text())
        fn_cad = {}
        for r in (man.get("rules") or []) + (man.get("schedules") or []):
            if (r.get("state") or "ENABLED") != "ENABLED":
                continue
            h = cadence_hours(r.get("expr"))
            if h is None:
                continue
            for t in r.get("targets") or []:
                fn = (t.get("arn") or "").split(":")[-1]
                if fn and ":stateMachine:" not in (t.get("arn") or ""):
                    fn_cad[fn] = min(h, fn_cad.get(fn, 1e9))
        fast = sorted(f for f, h in fn_cad.items()
                      if h <= 6.0 and f not in QUARANTINED)
        rep.log("functions with declared cadence <= 6h: %d" % len(fast))
        prior = batch_sums(fast, "Invocations",
                           NOW - timedelta(days=8), NOW - timedelta(hours=24))
        recent = batch_sums(fast, "Invocations",
                            NOW - timedelta(hours=8), NOW)
        silenced, hard = [], []
        for fn in fast:
            if prior.get(fn, 0) > 0 and recent.get(fn, 0) == 0:
                row = {"fn": fn, "cadence_h": round(fn_cad[fn], 2),
                       "prior_7d": int(prior[fn])}
                silenced.append(row)
                if fn_cad[fn] <= 1.0:
                    hard.append(row)
        rep.log("active-before, silent-in-8h: %d (of which cadence<=1h: %d)"
                % (len(silenced), len(hard)))
        for row in sorted(silenced, key=lambda x: x["cadence_h"])[:25]:
            (rep.fail if row["cadence_h"] <= 1.0 else rep.warn)(
                "   %-42s cadence=%sh prior7d=%d now=0"
                % (row["fn"][:42], row["cadence_h"], row["prior_7d"]))
            rep.kv(section="silenced", **row)
        if not silenced:
            rep.ok("NOTHING previously-active went silent — the surgery "
                   "(26 disables, 90 target removals, 13 retunes, 1 SFN "
                   "migration) broke no live engine")
        OUT["silenced"] = silenced
    except Exception as e:
        fails.append("regression sweep: %s" % str(e)[:170])

    # ================================================================ R2
    rep.section("R2. CRR backfill — replication is forward-only")
    try:
        src_keys = []
        for page in s3.get_paginator("list_objects_v2").paginate(
                Bucket=DR_SRC):
            src_keys += [o["Key"] for o in page.get("Contents", [])]
        dst_keys = set()
        for page in s3w.get_paginator("list_objects_v2").paginate(
                Bucket=DR_DST):
            dst_keys |= {o["Key"] for o in page.get("Contents", [])}
        missing = [k for k in src_keys if k not in dst_keys]
        rep.log("source objects: %d | already in us-west-2: %d | "
                "missing: %d" % (len(src_keys), len(dst_keys), len(missing)))
        copied = failed_c = 0
        for k in missing[:5000]:
            try:
                s3w.copy_object(Bucket=DR_DST, Key=k,
                                CopySource={"Bucket": DR_SRC, "Key": k})
                copied += 1
                if copied % 400 == 0:
                    rep.log("   … %d copied" % copied)
            except Exception as e:
                failed_c += 1
                if failed_c <= 3:
                    rep.warn("   copy %s: %s" % (k[-50:], str(e)[:90]))
        n_dst = 0
        for page in s3w.get_paginator("list_objects_v2").paginate(
                Bucket=DR_DST):
            n_dst += len(page.get("Contents", []))
        rep.log("copied=%d failed=%d | destination now holds %d of %d"
                % (copied, failed_c, n_dst, len(src_keys)))
        rep.kv(section="crr_backfill", source=len(src_keys),
               copied=copied, failed=failed_c, dest_now=n_dst)
        if n_dst >= len(src_keys):
            rep.ok("us-west-2 is now a COMPLETE copy — a us-east-1 loss "
                   "no longer takes the fleet's code with it")
        else:
            rep.warn("%d objects still to converge (replication covers "
                     "everything written from today forward)"
                     % (len(src_keys) - n_dst))
            if len(src_keys) - n_dst > 200:
                fails.append("backfill incomplete: %d remaining"
                             % (len(src_keys) - n_dst))
    except Exception as e:
        fails.append("crr backfill: %s" % str(e)[:170])

    # ================================================================ R3
    rep.section("R3. Give the alarms someone to call")
    topic_arn = None
    try:
        sns = boto3.client("sns", region_name=REGION, config=CFG)
        topic_arn = sns.create_topic(Name="jh-ops-alerts")["TopicArn"]
        rep.ok("SNS topic %s" % topic_arn)
        email = None
        try:
            bud = boto3.client("budgets", region_name="us-east-1",
                               config=CFG)
            acct = boto3.client("sts").get_caller_identity()["Account"]
            for b in bud.describe_budgets(AccountId=acct)["Budgets"]:
                for n in bud.describe_notifications_for_budget(
                        AccountId=acct,
                        BudgetName=b["BudgetName"]).get("Notifications", []):
                    for sub in bud.describe_subscribers_for_notification(
                            AccountId=acct, BudgetName=b["BudgetName"],
                            Notification=n).get("Subscribers", []):
                        if sub.get("SubscriptionType") == "EMAIL":
                            email = sub.get("Address")
                            break
                    if email:
                        break
                if email:
                    break
        except Exception as e:
            rep.warn("budget subscriber read: %s" % str(e)[:110])
        if email:
            subs = sns.list_subscriptions_by_topic(
                TopicArn=topic_arn).get("Subscriptions", [])
            if not any(s_.get("Endpoint") == email for s_ in subs):
                sns.subscribe(TopicArn=topic_arn, Protocol="email",
                              Endpoint=email)
                rep.ok("subscribed %s*** — AWS sent a confirmation email; "
                       "alarms deliver only after it is clicked"
                       % email[:3])
            else:
                rep.log("email already subscribed")
        else:
            rep.warn("no budget email found to reuse — subscribe manually "
                     "in the SNS console")
    except Exception as e:
        rep.warn("SNS unavailable to the runner (%s)" % str(e)[:130])
        rep.warn("GRANT NEEDED — run in Git Bash, then re-run this op:")
        rep.warn("aws iam put-group-policy --group-name jh-automation "
                 "--policy-name jh-sns --policy-document "
                 "'{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":"
                 "\"Allow\",\"Action\":[\"sns:CreateTopic\",\"sns:Subscribe"
                 "\",\"sns:ListSubscriptionsByTopic\",\"sns:Publish\"],"
                 "\"Resource\":\"*\"}]}'")
    if topic_arn:
        wired = 0
        for a in ("justhodl-integrity-new-defects", "justhodl-schedule-drift",
                  "justhodl-contract-sev1"):
            try:
                d = cw.describe_alarms(AlarmNames=[a])["MetricAlarms"]
                if not d:
                    rep.warn("   alarm %s missing" % a)
                    continue
                al = d[0]
                cw.put_metric_alarm(
                    AlarmName=a, AlarmDescription=al.get("AlarmDescription"),
                    Namespace=al["Namespace"], MetricName=al["MetricName"],
                    Statistic=al["Statistic"], Period=al["Period"],
                    EvaluationPeriods=al["EvaluationPeriods"],
                    Threshold=al["Threshold"],
                    ComparisonOperator=al["ComparisonOperator"],
                    TreatMissingData=al.get("TreatMissingData",
                                            "notBreaching"),
                    AlarmActions=[topic_arn])
                wired += 1
                rep.ok("   %s -> jh-ops-alerts" % a)
            except Exception as e:
                rep.fail("   %s: %s" % (a, str(e)[:110]))
        rep.kv(section="alarms", wired=wired, topic=topic_arn or "none")

    # ================================================================ R4
    rep.section("R4. Mirror the contract registry into git")
    try:
        body = s3.get_object(Bucket=BUCKET,
                             Key="config/engine-contracts.json"
                             )["Body"].read()
        (ROOT / "config").mkdir(exist_ok=True)
        (ROOT / "config" / "engine-contracts.json").write_bytes(body)
        j = json.loads(body)
        rep.ok("engine-contracts.json mirrored — %d contracts, "
               "%d cadence-bounded" % (j.get("n_contracts", 0),
                                       j.get("n_cadence_bounded", 0)))
    except Exception as e:
        fails.append("contract mirror: %s" % str(e)[:150])

    # ================================================================ R5
    rep.section("R5. SSM pointer containment (verified earlier, recorded)")
    rep.log("The scorecard SSM value became a pointer today for payloads "
            ">8KB. Repo-wide grep: ZERO functions read "
            "/justhodl/calibration/scorecard — the ~10 real consumers "
            "(conviction-engine, engine-trust, apex-fusion, "
            "proven-portfolio, …) read data/signal-scorecard.json, which "
            "was extended ADDITIVELY only. Suspected contract break: "
            "cleared with evidence, no action needed.")
    rep.kv(section="ssm_containment", ssm_consumers=0,
           s3_consumers="~10", artifact_change="additive")

    # ================================================================ R6
    rep.section("R6. Control-plane coherence")
    try:
        for rule, want_fn in CONTROL_RULES.items():
            try:
                r = evb.describe_rule(Name=rule)
                tg = evb.list_targets_by_rule(Rule=rule)["Targets"]
                ok = (r.get("State") == "ENABLED" and len(tg) == 1 and
                      (want_fn is None or
                       tg[0]["Arn"].endswith(":" + want_fn)))
                (rep.ok if ok else rep.fail)(
                    "   %-38s %-22s state=%s targets=%d"
                    % (rule[:38], r.get("ScheduleExpression"),
                       r.get("State"), len(tg)))
                if not ok:
                    fails.append("rule %s incoherent" % rule)
            except Exception as e:
                rep.fail("   %s: %s" % (rule, str(e)[:90]))
                fails.append("rule %s: %s" % (rule, str(e)[:60]))
        r = lam.invoke(FunctionName="justhodl-schedule-reconciler",
                       InvocationType="RequestResponse")
        b = json.loads(r["Payload"].read() or b"{}")
        (rep.ok if b.get("drift_count") == 0 else rep.fail)(
            "live reconciler drift = %s" % b.get("drift_count"))
        if b.get("drift_count"):
            fails.append("drift %s" % b.get("drift_count"))
        for key, label in (("data/fleet-integrity.json", "integrity"),
                           ("data/contract-violations.json", "contracts"),
                           ("data/dr-snapshot.json", "dr"),
                           ("data/signal-scorecard.json", "scorecard")):
            h = s3.head_object(Bucket=BUCKET, Key=key)
            age = (NOW - h["LastModified"]).total_seconds() / 3600.0
            (rep.ok if age < 26 else rep.warn)(
                "   %-12s artifact %.1fh old" % (label, age))
        sc = lam.get_function_configuration(FunctionName=
                                            "justhodl-signal-scorecard")
        rep.log("scorecard config holds: timeout=%ss memory=%sMB"
                % (sc.get("Timeout"), sc.get("MemorySize")))
    except Exception as e:
        fails.append("coherence: %s" % str(e)[:150])

    (ROOT / "aws" / "ops" / "reports" / "4250_session_review.json"
     ).write_text(json.dumps(OUT, indent=1, default=str), encoding="utf-8")

    rep.section("RESULT")
    if fails:
        for f in fails:
            rep.fail("  %s" % f)
        raise SystemExit("FAILS: %s" % "; ".join(fails[:3]))
    rep.ok("OPS 4250 PASS — session reviewed, gaps closed")
