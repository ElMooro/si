import boto3, time
logs=boto3.client("logs","us-east-1")
grp="/aws/lambda/justhodl-equity-research"
start=int((time.time()-900)*1000)
streams=logs.describe_log_streams(logGroupName=grp,orderBy="LastEventTime",descending=True,limit=3)["logStreams"]
want=("[claude","GLM","glm","llm_router","parse","PARSE","ERROR","usage","reasoning","empty","Empty","z.ai","1113","tier","Traceback")
seen=0
for st in streams:
    ev=logs.get_log_events(logGroupName=grp,logStreamName=st["logStreamName"],startTime=start,limit=200,startFromHead=False)["events"]
    for e in ev:
        m=e["message"].rstrip()
        if any(w in m for w in want):
            print(m[:300]); seen+=1
print(f"--- {seen} relevant log lines ---")
print("DONE 2249")
