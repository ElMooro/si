"""
ops_4272 -- honor the contract I just violated: 4271 created the -30m
schedule as rate(30 minutes) from its NAME, but the manifest declares
cron(33 15 * * ? *). The manifest wins (and 48x/day of a 300s/1GB
scanner isn't what anyone declared). Update in place; name's legacy.
"""
import sys
import boto3
from ops_report import report

sch = boto3.client("scheduler", region_name="us-east-1")
fails = []
with report("4272_options_30m_amend") as r:
    r.heading("ops 4272 -- -30m schedule amended to declared cron")
    try:
        d = sch.get_schedule(Name="justhodl-options-flow-30m",
                             GroupName="default")
        sch.update_schedule(
            Name="justhodl-options-flow-30m", GroupName="default",
            ScheduleExpression="cron(33 15 * * ? *)",
            FlexibleTimeWindow={"Mode": "OFF"},
            State="ENABLED", Target=d["Target"])
        d2 = sch.get_schedule(Name="justhodl-options-flow-30m",
                              GroupName="default")
        if d2.get("ScheduleExpression") == "cron(33 15 * * ? *)":
            r.ok("amended: %s (name is legacy; manifest expr honored)"
                 % d2["ScheduleExpression"])
        else:
            fails.append("expr readback: %s"
                         % d2.get("ScheduleExpression"))
    except Exception as e:
        fails.append(str(e)[:140])
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4272 PASS")
if fails:
    sys.exit(1)
