"""ops 3409 — why do news-wire ($2.36/day, haiku) + research-critique ($1.91/day, sonnet) cost
so much for '1/day' engines? Check the ACTUAL deployed EventBridge schedule vs config (rule
named 'news-wire-15m' suggests 15-min firing = 96x/day) + measure real token I/O per run."""
import json, boto3
from ops_report import report
eb=boto3.client("events",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1")
logs=boto3.client("logs",region_name="us-east-1")
def rule_for(fn):
    # find rules that target this function
    out=[]
    for r in eb.list_rules()["Rules"]:
        try:
            ts=eb.list_targets_by_rule(Rule=r["Name"])["Targets"]
            if any(fn in (t.get("Arn","")) for t in ts):
                out.append((r["Name"], r.get("ScheduleExpression"), r.get("State")))
        except Exception: pass
    return out
with report("3409_llm_cost_audit") as r:
    for fn in ["justhodl-news-wire","justhodl-research-critique"]:
        r.section(f"{fn} — deployed schedule")
        rules=rule_for(fn)
        for name,expr,state in rules:
            r.log(f"  rule: {name} | {expr} | {state}")
            # compute runs/day
            if expr and "rate(" in expr:
                import re
                m=re.search(r"rate\((\d+)\s*(\w+)", expr)
                if m:
                    n,unit=int(m.group(1)),m.group(2)
                    per_day={"minute":1440,"minutes":1440,"hour":24,"hours":24,"day":1,"days":1}.get(unit,0)/max(n,1)
                    r.log(f"    → ~{per_day:.0f} runs/day")
            elif expr and "cron(" in expr:
                # rough: count if it's hourly/15min via minute field
                inside=expr[expr.find("(")+1:expr.rfind(")")]
                mins=inside.split()[0]
                if mins in ("*","0/15","*/15","0/1","*/1"):
                    r.log(f"    ⚠ cron minute field '{mins}' → fires MANY times/day, not once")
                else:
                    r.log(f"    cron minute '{mins}' → looks like once/few daily")
        if not rules:
            r.log("  no EventBridge rule found targeting it (maybe Scheduler, not classic rules)")
        # concurrency / recent invocation count from CloudWatch logs
        r.section(f"{fn} — recent invocation volume (last 24h)")
        try:
            lg=f"/aws/lambda/{fn}"
            streams=logs.describe_log_streams(logGroupName=lg,orderBy="LastEventTime",descending=True,limit=50)["logStreams"]
            import time
            cutoff=(time.time()-86400)*1000
            recent=[s for s in streams if s.get("lastEventTimestamp",0)>cutoff]
            r.log(f"  log streams active in last 24h: {len(recent)} (each stream ≈ a container; many = frequent invokes)")
            # count START lines in the newest stream to see invokes
            if streams:
                ev=logs.get_log_events(logGroupName=lg,logStreamName=streams[0]["logStreamName"],limit=100,startFromHead=False)["events"]
                starts=sum(1 for e in ev if "START RequestId" in e.get("message",""))
                r.log(f"  invokes in newest stream sample: {starts}")
        except Exception as e:
            r.log(f"  logs: {str(e)[:60]}")
