import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=150,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); logs=boto3.client("logs","us-east-1")
# 1) kick crypto-intel async (heavy)
lam.invoke(FunctionName="justhodl-crypto-intel",InvocationType="Event",Payload=b"{}")
print("crypto-intel async kicked")
# 2) logger sync + CloudWatch confirm
lam.invoke(FunctionName="justhodl-signal-logger",InvocationType="RequestResponse",Payload=b"{}")
time.sleep(5)
lg="/aws/lambda/justhodl-signal-logger"
streams=logs.describe_log_streams(logGroupName=lg,orderBy="LastEventTime",descending=True,limit=2)["logStreams"]
hits=[]
for stm in streams:
    for e in logs.get_log_events(logGroupName=lg,logStreamName=stm["logStreamName"],limit=300,startFromHead=False)["events"]:
        m=e["message"].rstrip()
        if any(k in m for k in ["puell_multiple","hash_ribbon","crypto_options_rr","crypto_dvol"]): hits.append(m[:160])
print("ledger [LOG] lines:")
seen=set()
for h in hits:
    key=h.split("=")[0] if "=" in h else h
    if key not in seen: seen.add(key); print("  ",h)
# 3) wait for crypto-intel then read miners block
print("waiting for crypto-intel async write...")
time.sleep(80)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="crypto-intel.json")["Body"].read())
print("crypto-intel miners block:",json.dumps(d.get("miners")) if d.get("miners") else "MISSING (may need another run)")
print("DONE 2389")
