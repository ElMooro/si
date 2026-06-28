import boto3, json, time
lam=boto3.client("lambda","us-east-1"); logs=boto3.client("logs","us-east-1"); ev=boto3.client("events","us-east-1")
for fn,rule in [("justhodl-fleet-freshness-monitor","fleet-freshness-monitor-30min"),("justhodl-fleet-error-monitor","fleet-error-monitor-5min")]:
    print("=== %s ==="%fn)
    # rule check
    try:
        rd=ev.describe_rule(Name=rule); print("  rule:",rd.get("State"),rd.get("ScheduleExpression"))
        tg=ev.list_targets_by_rule(Rule=rule).get("Targets",[])
        print("  targets:",[t.get("Arn","").split(":")[-1] for t in tg])
    except Exception as e: print("  rule MISSING:",str(e)[:60])
    # invoke
    try:
        r=lam.invoke(FunctionName=fn,InvocationType="RequestResponse",Payload=b"{}")
        print("  FunctionError:",r.get("FunctionError"))
        print("  resp:",r["Payload"].read().decode()[:240])
    except Exception as e: print("  invoke err:",str(e)[:100])
    time.sleep(3)
    try:
        grp="/aws/lambda/"+fn
        st=logs.describe_log_streams(logGroupName=grp,orderBy="LastEventTime",descending=True,limit=1)["logStreams"]
        if st:
            ev2=logs.get_log_events(logGroupName=grp,logStreamName=st[0]["logStreamName"],limit=30,startFromHead=False)["events"]
            for l in [e["message"].strip()[:180] for e in ev2 if any(w in e["message"] for w in ("Error","Traceback","Exception","[ERROR]","Task timed","fail"))][-6:]:
                print("   LOG:",l)
    except Exception as e: print("  logs err:",str(e)[:80])
print("DONE 2413")
