"""ops_4328 -- SCHEDULE RECONCILER: cadence as code. For each engine
in the declarative map: ensure an EventBridge rule (cron), ensure the
lambda target, ensure the invoke permission (idempotent), then fire
each once and verify its artifact's generated_at CHANGES. This both
revives the frozen cluster and installs the never-again layer:
schedules are declared here, reconciled by machine, and the daily
fleet-auditor (v1.3) will flag any future drift as SCHEDULE_DEAD."""
import json, sys, time
from datetime import datetime, timezone
import boto3
from ops_report import report
lam = boto3.client("lambda", region_name="us-east-1")
ev = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
ACC = "857687956942"
# name -> (cron, artifact_key)
CADENCE = {
    "credit-stress":        ("cron(10 22 ? * MON-FRI *)",
                             "data/credit-stress.json"),
    "bond-trace":           ("cron(0 21 ? * MON-FRI *)",
                             "data/bond-trace.json"),
    "crisis-knowledge-base": ("cron(20 22 ? * MON-FRI *)",
                              "data/crisis-knowledge-base.json"),
    "cross-asset-rv":       ("cron(45 22 ? * MON-FRI *)",
                             "data/cross-asset-rv.json"),
    "event-study":          ("cron(30 22 ? * MON-FRI *)",
                             "data/event-study.json"),
    "global-macro":         ("cron(15 22 ? * MON-FRI *)",
                             "data/global-macro.json"),
    "historical-analogs":   ("cron(35 22 ? * MON-FRI *)",
                             "data/historical-analogs.json"),
    "implied-prob":         ("cron(25 22 ? * MON-FRI *)",
                             "data/implied-prob.json"),
    "liquidity-flow":       ("cron(5 22 ? * MON-FRI *)",
                             "data/liquidity-flow.json"),
    "feed-catalog":         ("cron(50 21 * * ? *)",
                             "data/interpretations/yield-curve.json"),
}


def g0(key):
    try:
        d = json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())
        return d.get("generated_at") or d.get("as_of") \
            or d.get("updated_at")
    except Exception:
        return None
fails = []
with report("4328_schedule_reconciler") as r:
    r.heading("ops 4328 -- cadence as code; the cluster breathes")
    before = {n: g0(k) for n, (c, k) in CADENCE.items()}
    for name, (cron, key) in CADENCE.items():
        fn = "justhodl-" + name
        arn = ("arn:aws:lambda:us-east-1:%s:function:%s"
               % (ACC, fn))
        rule = fn + "-cadence"
        line = [name]
        try:
            ev.put_rule(Name=rule, ScheduleExpression=cron,
                        State="ENABLED",
                        Description="reconciled by ops 4328 -- "
                                    "cadence-as-code")
            ev.put_targets(Rule=rule,
                           Targets=[{"Id": "1", "Arn": arn}])
            line.append("rule:OK")
        except Exception as e:
            line.append("rule:%s" % str(e)[:60])
            fails.append("%s rule: %s" % (name, str(e)[:60]))
        try:
            lam.add_permission(
                FunctionName=fn, StatementId="evb-" + rule,
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn="arn:aws:events:us-east-1:%s:rule/%s"
                          % (ACC, rule))
            line.append("perm:ADDED")
        except lam.exceptions.ResourceConflictException:
            line.append("perm:exists")
        except Exception as e:
            line.append("perm:%s" % str(e)[:50])
        try:
            lam.invoke(FunctionName=fn, InvocationType="Event",
                       Payload=b"{}")
            line.append("fired")
        except Exception as e:
            line.append("invoke:%s" % str(e)[:50])
            fails.append("%s invoke: %s" % (name, str(e)[:50]))
        r.log(" | ".join(line))
    r.section("revival poll -- artifacts must CHANGE")
    t0 = time.time()
    changed = {}
    while time.time() - t0 < 600 and len(changed) < len(CADENCE):
        time.sleep(25)
        for name, (c, key) in CADENCE.items():
            if name in changed:
                continue
            now = g0(key)
            if now and now != before.get(name):
                changed[name] = now
                r.ok("%s ALIVE -- generated_at %s" % (name, now))
    dead = [n for n in CADENCE if n not in changed]
    r.log("revived %d/%d in %.0fs · still dead: %s"
          % (len(changed), len(CADENCE), time.time() - t0, dead))
    for n in dead:
        r.warn("%s did not refresh -- its own runtime error is next "
               "(known: event-study FRED 400, implied-prob rogue "
               "polygon key)" % n)
    if len(changed) < 6:
        fails.append("only %d/%d revived" % (len(changed),
                                             len(CADENCE)))
    if fails:
        for f in fails:
            r.fail("  %s" % f)
        sys.exit(1)
    r.ok("OPS 4328 PASS -- schedules are code; drift is now a "
         "detector class, not a surprise")
