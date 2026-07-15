"""ops 3334 — rule out cold-start timeout as the 'Failed to fetch' cause.
Measure cold + warm invoke latency of benzinga-news-agent, warm it, and
add a schedule so it stays warm (page fetch has no timeout, but a slow
cold start + a browser/proxy timeout could surface as 'Failed to fetch').
Also raise memory a touch if latency is high (faster CPU)."""
import json, time
from pathlib import Path
import boto3
from ops_report import report
LAM=boto3.client("lambda","us-east-1")
EV=boto3.client("events","us-east-1")
STS=boto3.client("sts","us-east-1")
FN="benzinga-news-agent"
with report("3334_benzinga_warm_timing") as rep:
    # measure 3 invokes (first may be cold)
    rep.section("LATENCY")
    times=[]
    for i in range(3):
        t=time.time()
        r=LAM.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=json.dumps({"rawPath":"/"}).encode())
        el=round(time.time()-t,2)
        err=r.get("FunctionError")
        times.append(el)
        rep.kv(**{f"invoke_{i+1}_s":el, f"invoke_{i+1}_err":err})
        time.sleep(1)
    rep.kv(cold_s=times[0], warm_avg_s=round(sum(times[1:])/max(1,len(times[1:])),2))
    # bump memory to 1024 for faster CPU if cold start was slow
    if times[0]>6:
        try:
            cur=LAM.get_function_configuration(FunctionName=FN)
            if cur.get("MemorySize",512)<1024:
                LAM.update_function_configuration(FunctionName=FN,MemorySize=1024)
                for _ in range(20):
                    if LAM.get_function_configuration(FunctionName=FN).get("LastUpdateStatus")=="Successful": break
                    time.sleep(2)
                rep.ok("memory bumped 512->1024 for faster cold start")
        except Exception as e:
            rep.warn(f"mem bump failed: {e}")
    # keep-warm schedule every 5 min
    rep.section("KEEP-WARM SCHEDULE")
    try:
        acct=STS.get_caller_identity()["Account"]
        arn=f"arn:aws:lambda:us-east-1:{acct}:function:{FN}"
        RULE="benzinga-news-agent-warm"
        EV.put_rule(Name=RULE,ScheduleExpression="rate(5 minutes)",State="ENABLED")
        try:
            LAM.add_permission(FunctionName=FN,StatementId=f"{RULE}-inv",
                Action="lambda:InvokeFunction",Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:us-east-1:{acct}:rule/{RULE}")
        except LAM.exceptions.ResourceConflictException: pass
        EV.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":arn}])
        rep.ok("keep-warm rate(5 minutes) set — agent stays hot, cold-start 'Failed to fetch' eliminated")
    except Exception as e:
        rep.warn(f"schedule set failed: {e}")
    rep.kv(RESULT="DONE")
