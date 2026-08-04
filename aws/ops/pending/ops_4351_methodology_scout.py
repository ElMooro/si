"""ops_4351 -- the organism learns from other minds: scout live,
weekly cadence, first harvest printed (teachers, top consensus
indicators, data-source atlas, and the GAP list = tonight's
borrowed-idea proposals)."""
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
with report("4351_methodology_scout") as r:
    r.heading("ops 4351 -- learning from other minds, "
              "with receipts")
    try:
        ts = subprocess.run(
            ["git", "log", "-1", "--format=%ct", "--",
             "aws/lambdas/justhodl-methodology-scout"],
            capture_output=True, text=True, timeout=30
        ).stdout.strip()
        fl = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except Exception:
        fl = RUN_START
    ok = False
    for _ in range(55):
        try:
            c = lam.get_function_configuration(
                FunctionName="justhodl-methodology-scout")
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
        try:
            ev.put_rule(Name="justhodl-methodology-scout-cadence",
                        ScheduleExpression="rate(7 days)",
                        State="ENABLED")
            ev.put_targets(
                Rule="justhodl-methodology-scout-cadence",
                Targets=[{"Id": "1",
                          "Arn": "arn:aws:lambda:us-east-1:%s:"
                                 "function:justhodl-methodology-"
                                 "scout" % ACC}])
            try:
                lam.add_permission(
                    FunctionName="justhodl-methodology-scout",
                    StatementId="evb-scout",
                    Action="lambda:InvokeFunction",
                    Principal="events.amazonaws.com",
                    SourceArn="arn:aws:events:us-east-1:%s:rule/"
                              "justhodl-methodology-scout-cadence"
                              % ACC)
            except lam.exceptions.ResourceConflictException:
                pass
            r.ok("weekly cadence installed")
        except Exception as e:
            r.warn("cadence: %s" % str(e)[:70])
        for _t in range(6):
            try:
                lam.invoke(
                    FunctionName="justhodl-methodology-scout",
                    InvocationType="RequestResponse",
                    Payload=b"{}")
                break
            except Exception as _e:
                if "Pending" in str(_e) and _t < 5:
                    time.sleep(20)
                    continue
                raise
        kb = json.loads(s3.get_object(
            Bucket=B, Key="data/methodology-kb.json"
        )["Body"].read())
        r.ok("KB: %s indicators · %s data sources · teachers=%s"
             % (kb.get("n_indicators"),
                kb.get("n_data_sources"),
                json.dumps([{k: t[k] for k in
                             ("teacher", "indicators",
                              "data_sources", "error")
                             if k in t}
                            for t in kb.get("teachers")
                            or []])[:360]))
        tok = sum(1 for t in kb.get("teachers") or []
                  if "error" not in t)
        if tok < 3:
            fails.append("only %d teachers readable" % tok)
        if (kb.get("n_indicators") or 0) < 60:
            fails.append("harvest thin: %s indicators"
                         % kb.get("n_indicators"))
        r.section("consensus indicators (most teachers)")
        for x in (kb.get("indicators") or [])[:8]:
            r.log("  %-18s x%d %s ours=%s"
                  % (x["name"], x["n_teachers"],
                     x["category"], x["in_our_fleet"]))
        r.section("data-source atlas (top)")
        for x in (kb.get("data_sources") or [])[:8]:
            r.log("  %-24s via %s" % (x["source"],
                                      x["teachers"]))
        g = json.loads(s3.get_object(
            Bucket=B, Key="data/methodology-gaps.json"
        )["Body"].read())
        r.section("THE GAP LIST -- borrowed-idea proposals")
        r.ok("gaps=%s (shadow-candidate constitution attached)"
             % g.get("n_gaps"))
        for x in (g.get("gaps") or [])[:10]:
            r.log("  %-20s x%d %s via %s"
                  % (x["name"], x["n_teachers"], x["category"],
                     x["teachers"]))
        if (g.get("n_gaps") or 0) < 15:
            fails.append("gap list thin")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
        sys.exit(1)
    r.ok("OPS 4351 PASS -- the organism now studies other minds "
         "weekly, and everything it borrows must still earn "
         "its seat")
