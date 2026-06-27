import boto3
logs=boto3.client("logs","us-east-1")
lg="/aws/lambda/justhodl-cycle-clock"
streams=logs.describe_log_streams(logGroupName=lg,orderBy="LastEventTime",descending=True,limit=2)["logStreams"]
for st in streams:
    for e in logs.get_log_events(logGroupName=lg,logStreamName=st["logStreamName"],limit=40,startFromHead=False)["events"]:
        m=e["message"].rstrip()
        if any(k in m for k in ["Error","Traceback","NameError","line ","undefined","not defined","cycle-clock"]):
            print(m[:200])
print("DONE 2343")
