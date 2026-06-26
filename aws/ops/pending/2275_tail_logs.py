import boto3, time
logs=boto3.client("logs","us-east-1"); lam=boto3.client("lambda","us-east-1")
grp="/aws/lambda/justhodl-equity-research"
# fire a fresh run on a cold ticker to get a clean isolated log
lam.invoke(FunctionName="justhodl-equity-research",InvocationType="Event",
           Payload=__import__("json").dumps({"ticker":"GD","force_refresh":True,"_internal":"1"}).encode())
print("fired GD; waiting 175s for the run to log...")
time.sleep(175)
start=int((time.time()-200)*1000)
streams=logs.describe_log_streams(logGroupName=grp,orderBy="LastEventTime",descending=True,limit=2)["logStreams"]
allev=[]
for st in streams:
    for e in logs.get_log_events(logGroupName=grp,logStreamName=st["logStreamName"],startTime=start,limit=300,startFromHead=True)["events"]:
        allev.append((e["timestamp"],e["message"].rstrip()))
allev.sort()
for ts,m in allev[-50:]:
    if m.strip(): print(m[:240])
print("DONE 2275")
