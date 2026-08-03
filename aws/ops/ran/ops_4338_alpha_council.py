"""ops_4338 -- ALPHA COUNCIL live: wilson-proven membership, WHY
profiles (regime-sliced), regime-filtered consensus votes on open
ledger signals, and self-logging as eng:alpha-council. Daily cadence
installed. This report prints the founding council and its first
consensus board."""
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
ddbq = boto3.resource("dynamodb",
                      region_name="us-east-1")
B = "justhodl-dashboard-live"
ACC = "857687956942"
RUN_START = datetime.now(timezone.utc)
fails = []
with report("4338_alpha_council") as r:
    r.heading("ops 4338 -- the council convenes")
    try:
        ts = subprocess.run(
            ["git", "log", "-1", "--format=%ct", "--",
             "aws/lambdas/justhodl-alpha-council"],
            capture_output=True, text=True, timeout=30
        ).stdout.strip()
        fl = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except Exception:
        fl = RUN_START
    ok = False
    for _ in range(55):
        try:
            c = lam.get_function_configuration(
                FunctionName="justhodl-alpha-council")
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
        rule = "justhodl-alpha-council-cadence"
        arn = ("arn:aws:lambda:us-east-1:%s:function:"
               "justhodl-alpha-council" % ACC)
        try:
            ev.put_rule(Name=rule,
                        ScheduleExpression="cron(55 23 * * ? *)",
                        State="ENABLED",
                        Description="daily council (ops 4338)")
            ev.put_targets(Rule=rule,
                           Targets=[{"Id": "1", "Arn": arn}])
            try:
                lam.add_permission(
                    FunctionName="justhodl-alpha-council",
                    StatementId="evb-" + rule,
                    Action="lambda:InvokeFunction",
                    Principal="events.amazonaws.com",
                    SourceArn="arn:aws:events:us-east-1:"
                              "%s:rule/%s" % (ACC, rule))
            except lam.exceptions.ResourceConflictException:
                pass
            r.ok("daily cadence: 23:55 UTC (after leaderboard)")
        except Exception as e:
            fails.append("cadence: %s" % str(e)[:80])
        for _try in range(6):
            try:
                lam.invoke(FunctionName="justhodl-alpha-council",
                           InvocationType="RequestResponse",
                           Payload=b"{}")
                break
            except Exception as _e:
                if "Pending" in str(_e) and _try < 5:
                    time.sleep(20)
                    continue
                raise
        d = json.loads(s3.get_object(
            Bucket=B, Key="data/alpha-council.json"
        )["Body"].read())
        r.ok("COUNCIL: %s members (rule %s) · regime=%s "
             "posture=%s"
             % (d.get("n_council"), d.get("membership_rule"),
                d.get("current_regime"), d.get("risk_posture")))
        r.section("founding council (top 10 by wilson)")
        for c0 in (d.get("council") or [])[:10]:
            r.log("%-26s wilson=%s%% (wr %s%% n=%s) bias=%s "
                  "fit_now=%s"
                  % (c0["engine"], c0["wilson_lb"],
                     c0["win_pct"], c0["n"],
                     c0["direction_bias"],
                     c0.get("regime_fit_now")))
        r.section("first consensus board")
        cc = d.get("consensus_calls") or []
        for x in cc[:8]:
            r.log("%s %s · council_n=%s · score=%s · %s"
                  % (x["symbol"], x["direction"],
                     x["council_n"], x["weighted_score"],
                     [e0["engine"] for e0 in
                      x["engines"][:4]]))
        r.log("self_logged=%s consensus calls into the ledger "
              "as eng:alpha-council" % d.get("self_logged"))
        if (d.get("n_council") or 0) < 5:
            fails.append("council too small: %s"
                         % d.get("n_council"))
        if not (d.get("council") or [{}])[0].get("win_by_regime"):
            fails.append("WHY profiles lack regime slices")
        if cc and d.get("self_logged", 0) == 0:
            fails.append("consensus exists but self-log failed")
        if cc:
            tb = ddbq.Table("justhodl-signals")
            got = tb.get_item(Key={"signal_id":
                                   "alpha-council#%s#%s"
                                   % (cc[0]["symbol"],
                                      RUN_START.strftime(
                                          "%Y-%m-%d"))}
                              ).get("Item")
            if got:
                r.ok("ledger verified: council's own %s call is "
                     "pending grade" % cc[0]["symbol"])
            else:
                fails.append("council self-log not found in "
                             "ledger")
        else:
            r.warn("no consensus overlap among open council "
                   "signals right now -- honest zero; the board "
                   "fills as members log fresh calls")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
        sys.exit(1)
    r.ok("OPS 4338 PASS -- the ultimate engine exists, and it "
         "will be judged by the same scoreboard as everyone else")
