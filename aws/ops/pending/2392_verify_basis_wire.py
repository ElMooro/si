import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=150,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); logs=boto3.client("logs","us-east-1")
# kick crypto-intel async (heavy)
lam.invoke(FunctionName="justhodl-crypto-intel",InvocationType="Event",Payload=b"{}")
print("crypto-intel async kicked")
# logger sync — must run clean even though cc_basis_extreme evaluates NEUTRAL (no extreme now)
r=lam.invoke(FunctionName="justhodl-signal-logger",InvocationType="RequestResponse",Payload=b"{}")
print("logger FunctionError:",r.get("FunctionError"),"| resp:",r["Payload"].read().decode()[:80])
time.sleep(5)
lg="/aws/lambda/justhodl-signal-logger"
streams=logs.describe_log_streams(logGroupName=lg,orderBy="LastEventTime",descending=True,limit=2)["logStreams"]
errs=[]; crypto_logs=[]
for stm in streams:
    for e in logs.get_log_events(logGroupName=lg,logStreamName=stm["logStreamName"],limit=300,startFromHead=False)["events"]:
        m=e["message"].rstrip()
        if any(k in m for k in ["Traceback","Error","cc_basis","basis"]): errs.append(m[:140])
        if "[LOG] crypto" in m or "[LOG] puell" in m or "[LOG] cc_basis" in m or "[LOG] hash" in m:
            k=m.split("=")[0]
            if k not in [x.split("=")[0] for x in crypto_logs]: crypto_logs.append(m[:120])
print("crypto signals logged this run:")
for c in crypto_logs: print("  ",c)
print("basis/error lines:", errs if errs else "none (clean; cc_basis_extreme NEUTRAL as expected)")
# read crypto-intel basis block
print("waiting for crypto-intel write...")
time.sleep(80)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="crypto-intel.json")["Body"].read())
print("crypto-intel basis block:",json.dumps(d.get("basis")) if d.get("basis") else "MISSING")
# also confirm full implied_vol+miners+basis coverage in one place
iv=d.get("implied_vol") or {}
print("implied_vol has surface:", "surface" in iv, "| miners present:", bool(d.get("miners")), "| basis present:", bool(d.get("basis")))
print("DONE 2392")
