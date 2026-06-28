import boto3, time
logs=boto3.client("logs","us-east-1")
lg="/aws/lambda/justhodl-eurodollar-plumbing"
try:
    streams=logs.describe_log_streams(logGroupName=lg,orderBy="LastEventTime",descending=True,limit=2)["logStreams"]
    for st in streams:
        for e in logs.get_log_events(logGroupName=lg,logStreamName=st["logStreamName"],limit=60,startFromHead=False)["events"]:
            m=e["message"].rstrip()
            if any(k in m for k in ["stablecoin","offshore","Error","Traceback","HTTP","URLError","timed out","llama","Task timed"]):
                print(m[:200])
except Exception as e: print("log err:",str(e)[:100])
print("DONE 2373")
