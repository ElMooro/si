"""
ops_4248 — read the scorecard's actual log stream instead of inferring.

ops 4247 fixed the blocking failure: 256MB/120s -> 1024MB/900s, and the
engine went from timing out at 120s to completing in 30 SECONDS. More
memory buys proportionally more CPU, so this was a speed fix as much as
a ceiling fix.

It also surfaced something worse than the timeout. The artifact's
previous generated_at was 2026-07-27 — the signal scorecard had been
FROZEN FOR FIVE DAYS while the function reported failures nobody read.
Everything downstream that consumes it has been running on stale truth.

What is still unexplained: the artifact republished cleanly, but without
the ssm_writes key the ops-4246 fix adds at the very end of the handler.
So the run reaches the FIRST artifact write and exits before the last
one. I have two plausible stories and no evidence, which is exactly the
state that produced three wrong calls earlier today — so this op reads
the log stream rather than guessing again.
"""
import json, time
from datetime import datetime, timedelta, timezone
import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
FN = "justhodl-signal-scorecard"
CFG = Config(retries={"max_attempts": 5, "mode": "adaptive"}, read_timeout=120)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
logs = boto3.client("logs", region_name=REGION, config=CFG)
s3 = boto3.client("s3", region_name=REGION, config=CFG)
NOW = datetime.now(timezone.utc)

with report("4248_scorecard_trace") as rep:
    rep.heading("ops 4248 — scorecard: read the logs")
    fails = []

    rep.section("1. Confirm what code is actually running")
    c = lam.get_function_configuration(FunctionName=FN)
    rep.log("timeout=%ss memory=%sMB modified=%s"
            % (c.get("Timeout"), c.get("MemorySize"), c.get("LastModified")))
    rep.kv(section="config", timeout=c.get("Timeout"),
           memory=c.get("MemorySize"))

    rep.section("2. Most recent run — the whole stream, not one line")
    start = int((NOW - timedelta(hours=2)).timestamp() * 1000)
    try:
        streams = logs.describe_log_streams(
            logGroupName="/aws/lambda/%s" % FN, orderBy="LastEventTime",
            descending=True, limit=3)["logStreams"]
        for st in streams[:2]:
            rep.log("--- stream %s ---" % st["logStreamName"][-40:])
            ev = logs.get_log_events(logGroupName="/aws/lambda/%s" % FN,
                                     logStreamName=st["logStreamName"],
                                     startFromHead=False,
                                     limit=60)["events"]
            for e in ev[-45:]:
                m = e["message"].rstrip()
                if not m:
                    continue
                low = m.lower()
                if "traceback" in low or "error" in low or "exception" in low:
                    rep.fail("   %s" % m[:230])
                else:
                    rep.log("   %s" % m[:200])
    except Exception as e:
        fails.append("log read: %s" % str(e)[:170])

    rep.section("3. Explicit ERROR filter across 24h")
    try:
        r = logs.filter_log_events(
            logGroupName="/aws/lambda/%s" % FN,
            startTime=int((NOW - timedelta(hours=24)).timestamp() * 1000),
            filterPattern='?Traceback ?Error ?Exception ?"Task timed out"',
            limit=25)
        ev = r.get("events", [])
        rep.log("matching lines in 24h: %d" % len(ev))
        seen = set()
        for e in ev:
            m = e["message"].strip().replace("\\n", " | ")[:240]
            k = m[:90]
            if k in seen:
                continue
            seen.add(k)
            rep.fail("   %s" % m)
            rep.kv(section="error_line", line=m[:150])
        if not ev:
            rep.ok("no ERROR lines in 24h — the handler is completing; the "
                   "missing ssm_writes key is a code-path question, not a "
                   "crash")
    except Exception as e:
        rep.warn("filter: %s" % str(e)[:150])

    rep.section("4. What the artifact says now")
    try:
        a = json.loads(s3.get_object(Bucket=BUCKET,
                                     Key="data/signal-scorecard.json"
                                     )["Body"].read())
        rep.log("generated_at=%s keys=%d" % (a.get("generated_at"),
                                             len(a.keys())))
        rep.log("has ssm_writes: %s   ssm_ok: %s"
                % ("ssm_writes" in a, a.get("ssm_ok")))
        rep.log("top-level keys: %s" % ", ".join(sorted(a.keys()))[:300])
        rep.kv(section="artifact", generated_at=a.get("generated_at"),
               has_ssm_writes=("ssm_writes" in a), ssm_ok=a.get("ssm_ok"))
        age_d = None
        try:
            t = datetime.fromisoformat(
                a["generated_at"].replace("Z", "+00:00"))
            age_d = (NOW - t).total_seconds() / 86400.0
        except Exception:
            pass
        if age_d is not None:
            rep.log("artifact age: %.2f days" % age_d)
    except Exception as e:
        fails.append("artifact: %s" % str(e)[:150])

    rep.section("5. Who consumes this artifact")
    try:
        n = 0
        for page in lam.get_paginator("list_functions").paginate():
            for f in page["Functions"]:
                ev = (f.get("Environment") or {}).get("Variables") or {}
                if "signal-scorecard" in json.dumps(ev) or \
                        "signal_enforcement" in json.dumps(ev).lower():
                    rep.warn("   consumer: %s" % f["FunctionName"])
                    n += 1
        rep.log("functions naming the scorecard in their env: %d" % n)
        rep.warn("The scorecard artifact was 5 DAYS STALE before ops 4247. "
                 "Anything reading it — calibrator, best-setups, "
                 "master-ranker — was scoring on week-old truth and had no "
                 "way to know. This is precisely the failure the contract "
                 "gate exists to catch, and its STALE bound should have "
                 "fired: worth checking why it did not.")
    except Exception as e:
        rep.warn("consumers: %s" % str(e)[:130])

    rep.section("RESULT")
    if fails:
        for f in fails:
            rep.fail("  %s" % f)
        raise SystemExit("FAILS: %s" % "; ".join(fails[:3]))
    rep.ok("OPS 4248 — diagnostic complete, no changes made")
