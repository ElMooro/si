import boto3, json, time
logs=boto3.client("logs","us-east-1")
lam=boto3.client("lambda","us-east-1")
GRP="/aws/lambda/justhodl-premortem-engine"
# trigger a fresh async run, then read its logs
lam.invoke(FunctionName="justhodl-premortem-engine",InvocationType="Event")
print("fresh async invoke sent; waiting 75s for it to run + log...")
time.sleep(75)
try:
    streams=logs.describe_log_streams(logGroupName=GRP,orderBy="LastEventTime",descending=True,limit=2)["logStreams"]
except Exception as e:
    print("no log group:",str(e)[:60]); streams=[]
seen=0
for st in streams:
    ev=logs.get_log_events(logGroupName=GRP,logStreamName=st["logStreamName"],limit=60,startFromHead=False)["events"]
    for e in ev:
        m=e["message"].rstrip()
        # surface the meaningful lines
        if any(k in m for k in ("llm_router","glm","GLM","zai","z.ai","Z.ai","error","Error","ERROR","Traceback","Exception",
                                "selected","targets","claude_fail","parse_fail","no_llm","empty","timeout","Timeout","[premortem","ssm","SSM","credit","400","403","429")):
            print("  "+m[:240]); seen+=1
    if seen: break
if not seen:
    print("  (no error lines surfaced; dumping last 25 raw lines)")
    for st in streams[:1]:
        ev=logs.get_log_events(logGroupName=GRP,logStreamName=st["logStreamName"],limit=25,startFromHead=False)["events"]
        for e in ev: print("  "+e["message"].rstrip()[:220])
print("DONE 2132")
