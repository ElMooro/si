import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); logs=boto3.client("logs","us-east-1")
r=lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="RequestResponse",Payload=b"{}")
print("FunctionError:",r.get("FunctionError"))
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
print("version:",d.get("version"))
print("ppi_pricing raw:",json.dumps(d.get("ppi_pricing")))
# how many signals does supply-inflection expose + do any contain 'ppi'?
si=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/supply-inflection.json")["Body"].read())
sigs=si.get("signals") or []
print("supply-inflection signals:",len(sigs))
ppimatch=[s for s in sigs if "ppi" in " ".join(str(s.get(k,"")) for k in ("name","description","category","symbol")).lower()]
print("signals matching 'ppi':",len(ppimatch))
if ppimatch: print("  sample:",json.dumps(ppimatch[0])[:300])
if sigs: print("  generic signal keys:",sorted(sigs[0].keys()))
time.sleep(4)
try:
    st=logs.describe_log_streams(logGroupName="/aws/lambda/justhodl-bottleneck-boom",orderBy="LastEventTime",descending=True,limit=2)["logStreams"]
    for s in st:
        for e in logs.get_log_events(logGroupName="/aws/lambda/justhodl-bottleneck-boom",logStreamName=s["logStreamName"],limit=60,startFromHead=False)["events"]:
            if "[ppi]" in e["message"]: print("LOG:",e["message"].strip()[:160])
except Exception as e: print("logerr",str(e)[:60])
print("DONE 2482")
