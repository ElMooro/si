"""ops_4337 -- leaderboard becomes an organ: daily-scheduled engine +
live page + THE DISTRIBUTION (pooled, median, ge55 share) that answers
'is the system worthless?' with numbers. Same gate carries both
autopsies: wl-engines writer + auction-decisive-call units."""
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


def sh(cmd):
    try:
        return subprocess.run(cmd, capture_output=True, text=True,
                              timeout=25).stdout[:900]
    except Exception as e:
        return "sh: %s" % e
fails = []
with report("4337_leaderboard_live") as r:
    r.heading("ops 4337 -- the distribution answers despair")
    try:
        ts = subprocess.run(
            ["git", "log", "-1", "--format=%ct", "--",
             "aws/lambdas/justhodl-engine-leaderboard"],
            capture_output=True, text=True, timeout=30
        ).stdout.strip()
        fl = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except Exception:
        fl = RUN_START
    ok = False
    for _ in range(55):
        try:
            c = lam.get_function_configuration(
                FunctionName="justhodl-engine-leaderboard")
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
        rule = "justhodl-engine-leaderboard-cadence"
        arn = ("arn:aws:lambda:us-east-1:%s:function:"
               "justhodl-engine-leaderboard" % ACC)
        try:
            ev.put_rule(Name=rule,
                        ScheduleExpression="cron(40 23 * * ? *)",
                        State="ENABLED",
                        Description="daily self-grade (ops 4337)")
            ev.put_targets(Rule=rule,
                           Targets=[{"Id": "1", "Arn": arn}])
            try:
                lam.add_permission(
                    FunctionName="justhodl-engine-leaderboard",
                    StatementId="evb-" + rule,
                    Action="lambda:InvokeFunction",
                    Principal="events.amazonaws.com",
                    SourceArn="arn:aws:events:us-east-1:%s:rule/%s"
                              % (ACC, rule))
            except lam.exceptions.ResourceConflictException:
                pass
            r.ok("daily cadence installed: 23:40 UTC")
        except Exception as e:
            fails.append("cadence: %s" % str(e)[:80])
        lam.invoke(FunctionName="justhodl-engine-leaderboard",
                   InvocationType="RequestResponse", Payload=b"{}")
        d = json.loads(s3.get_object(
            Bucket=B, Key="data/engine-leaderboard.json"
        )["Body"].read())
        D = d.get("distribution") or {}
        r.ok("THE DISTRIBUTION: pooled %s%% over %s graded calls · "
             "median engine %s%% · %s engines >=55%% (%s%% of %s)"
             % (D.get("pooled_win_pct"), d.get("n_graded"),
                D.get("median_engine_win_pct"),
                D.get("engines_ge_55_pct"),
                D.get("engines_ge_55_share"), d.get("n_engines")))
        r.log("histogram: %s" % json.dumps(D.get("histogram")))
        for k in ("pooled_win_pct", "median_engine_win_pct",
                  "histogram"):
            if D.get(k) in (None, {}):
                fails.append("distribution lacks %s" % k)
    r.section("autopsy A: wl-engines writer")
    who = sh(["grep", "-rln", "wl-engines", "aws/lambdas/"])
    r.log("writers: %s" % who.replace("\n", " ")[:300])
    tgt = next((x for x in who.splitlines()
                if x.endswith(".py")), "")
    if tgt:
        r.log(sh(["grep", "-n", "-B3", "-A6", "wl-engines",
                  tgt])[:800])
    r.section("autopsy B: auction-decisive-call units")
    who2 = sh(["grep", "-rln", "auction-decisive",
               "aws/lambdas/"])
    r.log("writers: %s" % who2.replace("\n", " ")[:300])
    tgt2 = next((x for x in who2.splitlines()
                 if x.endswith(".py")), "")
    if tgt2:
        r.log(sh(["grep", "-n", "-B3", "-A6",
                  "return_pct\\|predicted_magnitude\\|"
                  "auction-decisive", tgt2])[:900])
    import urllib.request
    body = ""
    for _ in range(13):
        try:
            body = urllib.request.urlopen(urllib.request.Request(
                "https://justhodl.ai/engine-leaderboard.html",
                headers={"User-Agent": "ops/4337"}),
                timeout=25).read().decode("utf-8", "ignore")
            if "Engine Leaderboard" in body:
                break
        except Exception:
            pass
        time.sleep(20)
    for mk in ("Engine Leaderboard", "pooled win-rate",
               "distribution", "Top success", "Top failure",
               "sign-flip"):
        if mk not in body:
            fails.append("edge missing %s" % mk)
    if "Engine Leaderboard" in body:
        r.ok("PAGE LIVE: https://justhodl.ai/"
             "engine-leaderboard.html (%d bytes)" % len(body))
    if fails:
        for f in fails:
            r.fail("  %s" % f)
        sys.exit(1)
    r.ok("OPS 4337 PASS -- the fleet grades itself daily, in "
         "public, with the whole distribution on the table")
