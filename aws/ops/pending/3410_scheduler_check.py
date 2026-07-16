"""ops 3410 — confirm the EventBridge SCHEDULER cadence + count actual 24h invocations from
CloudWatch metrics (authoritative) for news-wire + research-critique."""
import json, boto3, time
from datetime import datetime, timedelta, timezone
from ops_report import report
sched=boto3.client("scheduler",region_name="us-east-1")
cw=boto3.client("cloudwatch",region_name="us-east-1")
def find_sched(fn):
    out=[]
    tok=None
    while True:
        kw={"MaxResults":100}
        if tok: kw["NextToken"]=tok
        resp=sched.list_schedules(**kw)
        for s in resp.get("Schedules",[]):
            try:
                d=sched.get_schedule(Name=s["Name"],GroupName=s.get("GroupName","default"))
                if fn in json.dumps(d.get("Target",{})):
                    out.append((s["Name"], d.get("ScheduleExpression"), d.get("State")))
            except Exception: pass
        tok=resp.get("NextToken")
        if not tok: break
    return out
def invokes_24h(fn):
    r=cw.get_metric_statistics(Namespace="AWS/Lambda",MetricName="Invocations",
        Dimensions=[{"Name":"FunctionName","Value":fn}],
        StartTime=datetime.now(timezone.utc)-timedelta(days=1),
        EndTime=datetime.now(timezone.utc),Period=86400,Statistics=["Sum"])
    dp=r.get("Datapoints",[])
    return dp[0]["Sum"] if dp else 0
with report("3410_scheduler_check") as r:
    for fn in ["justhodl-news-wire","justhodl-research-critique"]:
        r.section(fn)
        scheds=find_sched(fn)
        for name,expr,state in scheds:
            r.log(f"  scheduler: {name} | {expr} | {state}")
        if not scheds: r.log("  no Scheduler entry found either")
        inv=invokes_24h(fn)
        r.log(f"  ⭐ ACTUAL invocations last 24h (CloudWatch): {inv:.0f}")
        if inv>2: r.log(f"  ⚠⚠ this is a '1/day' engine but ran {inv:.0f}× → ~{inv:.0f}× the intended LLM cost")
