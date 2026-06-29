import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
logs=boto3.client("logs","us-east-1")
lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="RequestResponse",Payload=b"{}")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
p=d.get("physical_throughput") or {}
print("version:",d.get("version"),"GSCPI:",json.dumps(p.get("gscpi")))
print("physical_pressure_z:",p.get("physical_pressure_z"),"state:",p.get("physical_state"))
if not p.get("gscpi"):
    time.sleep(5)
    try:
        lg="/aws/lambda/justhodl-bottleneck-boom"
        st=logs.describe_log_streams(logGroupName=lg,orderBy="LastEventTime",descending=True,limit=2)["logStreams"]
        for s in st:
            ev=logs.get_log_events(logGroupName=lg,logStreamName=s["logStreamName"],limit=50,startFromHead=False)["events"]
            for e in ev:
                if "gscpi" in e["message"].lower(): print("LOG:",e["message"].strip()[:160])
    except Exception as ex: print("logfetch:",str(ex)[:80])
print("DONE 2463")
