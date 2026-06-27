import boto3, time, json
lam=boto3.client("lambda","us-east-1"); logs=boto3.client("logs","us-east-1")
cfg=lam.get_function_configuration(FunctionName="justhodl-bottleneck-research")
print("Timeout:", cfg.get("Timeout"), "s | Mem:", cfg.get("MemorySize"), "| LastUpdate:", cfg.get("LastUpdateStatus"))
# fire a run and tail logs
lam.invoke(FunctionName="justhodl-bottleneck-research",InvocationType="Event",Payload=b"{}")
print("fired; waiting 200s to capture the run...")
time.sleep(200)
grp="/aws/lambda/justhodl-bottleneck-research"
start=int((time.time()-220)*1000)
streams=logs.describe_log_streams(logGroupName=grp,orderBy="LastEventTime",descending=True,limit=2)["logStreams"]
ev=[]
for st in streams:
    for e in logs.get_log_events(logGroupName=grp,logStreamName=st["logStreamName"],startTime=start,limit=300,startFromHead=True)["events"]:
        ev.append((e["timestamp"],e["message"].rstrip()))
ev.sort()
for ts,m in ev[-45:]:
    if m.strip() and not m.startswith("REPORT") or "Duration" in m or "Task timed" in m: print(m[:200])
print("DONE 2299")
