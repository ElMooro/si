import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=120,retries={"max_attempts":0}))
logs=boto3.client("logs","us-east-1")
r=lam.invoke(FunctionName="justhodl-signal-logger",InvocationType="RequestResponse",Payload=b"{}")
print("resp:",r["Payload"].read().decode()[:80])
time.sleep(5)
lg="/aws/lambda/justhodl-signal-logger"
streams=logs.describe_log_streams(logGroupName=lg,orderBy="LastEventTime",descending=True,limit=2)["logStreams"]
hits=[]
for stm in streams:
    ev=logs.get_log_events(logGroupName=lg,logStreamName=stm["logStreamName"],limit=300,startFromHead=False)["events"]
    for e in ev:
        m=e["message"].rstrip()
        if any(k in m for k in ["crypto_options_rr","options surface","crypto-options-surface","skip-directional","skip-relative"]):
            hits.append(m[:200])
print("matching log lines:",len(hits))
for h in hits[-12:]: print("  ",h)
print("DONE 2385")
