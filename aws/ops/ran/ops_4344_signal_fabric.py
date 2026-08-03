"""ops_4344 -- the Signal Fabric goes live: N-to-1 architecture,
empirical-weight fusion, conflicts as first-class. Cadence 00:10."""
import json, subprocess, sys, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from ops_report import report
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=400,
                                 retries={"max_attempts": 1}))
ev = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
ACC = "857687956942"
RUN_START = datetime.now(timezone.utc)
fails = []
with report("4344_signal_fabric") as r:
    r.heading("ops 4344 -- the engines finally talk")
    try:
        ts = subprocess.run(
            ["git", "log", "-1", "--format=%ct", "--",
             "aws/lambdas/justhodl-signal-fabric"],
            capture_output=True, text=True, timeout=30
        ).stdout.strip()
        fl = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except Exception:
        fl = RUN_START
    ok = False
    for _ in range(55):
        try:
            c = lam.get_function_configuration(
                FunctionName="justhodl-signal-fabric")
            lm = datetime.strptime(c["LastModified"].split(".")[0],
                                   "%Y-%m-%dT%H:%M:%S").replace(
                tzinfo=timezone.utc)
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and c.get("State") in (None, "Active") \
                    and lm >= fl:
                ok = True
                break
        except Exception:
            pass
        time.sleep(9)
    if not ok:
        fails.append("deploy floor")
    else:
        rule = "justhodl-signal-fabric-cadence"
        arn = ("arn:aws:lambda:us-east-1:%s:function:"
               "justhodl-signal-fabric" % ACC)
        try:
            ev.put_rule(Name=rule,
                        ScheduleExpression="cron(10 0 * * ? *)",
                        State="ENABLED",
                        Description="daily fabric (ops 4344)")
            ev.put_targets(Rule=rule,
                           Targets=[{"Id": "1", "Arn": arn}])
            try:
                lam.add_permission(
                    FunctionName="justhodl-signal-fabric",
                    StatementId="evb-" + rule,
                    Action="lambda:InvokeFunction",
                    Principal="events.amazonaws.com",
                    SourceArn="arn:aws:events:us-east-1:"
                              "%s:rule/%s" % (ACC, rule))
            except lam.exceptions.ResourceConflictException:
                pass
            r.ok("cadence 00:10 UTC (after leaderboard+council)")
        except Exception as e:
            fails.append("cadence: %s" % str(e)[:80])
        for _try in range(6):
            try:
                lam.invoke(FunctionName="justhodl-signal-fabric",
                           InvocationType="RequestResponse",
                           Payload=b"{}")
                break
            except Exception as _e:
                if "Pending" in str(_e) and _try < 5:
                    time.sleep(20)
                    continue
                raise
        d = json.loads(s3.get_object(
            Bucket=B, Key="data/signal-fabric.json"
        )["Body"].read())
        r.ok("FABRIC: %s tickers · %s conflicts · sources=%s"
             % (d.get("n_tickers"), d.get("n_conflicts"),
                json.dumps(d.get("source_stats"))))
        tk = d.get("tickers") or []
        r.section("densest weave (most engines on one name)")
        for x in sorted(tk, key=lambda z: -z["n_engines"])[:6]:
            r.log("%s: %d engines · fabric=%s %s · agree=%s%% · %s"
                  % (x["ticker"], x["n_engines"],
                     x["fabric_score"], x["net_direction"],
                     x["agreement_pct"],
                     [e["engine"] for e in x["engines"][:6]]))
        r.section("the debate floor (conflicts)")
        for x in (d.get("conflicts") or [])[:5]:
            r.log("%s: UP=%s vs DOWN=%s"
                  % (x["ticker"], x["up"], x["down"]))
        live = sum(1 for v in (d.get("source_stats")
                               or {}).values() if v)
        if live < 6:
            fails.append("only %d adapters produced rows" % live)
        if not tk or tk[0].get("engines") is None:
            fails.append("fabric rows malformed")
        e0 = (tk[0]["engines"][0] if tk and tk[0]["engines"]
              else {})
        if "empirical" not in str(e0.get("weight_basis", "")) \
                and "neutral" not in str(e0.get("weight_basis",
                                                "")):
            fails.append("weights lack basis")
        if not d.get("n_conflicts"):
            r.warn("zero conflicts this run -- watch tomorrow")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
        sys.exit(1)
    r.ok("OPS 4344 PASS -- N-squared wiring is dead; the fleet "
         "speaks one language, and disagreement is on the record")

# retrigger: v1.1 adapters (recon-corrected keys, congress+SI custom)
