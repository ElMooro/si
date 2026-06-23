import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=480,retries={"max_attempts":0}))
ev=boto3.client("events","us-east-1"); s3=boto3.client("s3","us-east-1")

# 1. market-internals — invoke + verify volume/rotation
for _ in range(20):
    c=lam.get_function(FunctionName="justhodl-market-internals")["Configuration"]
    if c.get("LastUpdateStatus")=="Successful": break
    time.sleep(3)
lam.invoke(FunctionName="justhodl-market-internals",InvocationType="RequestResponse")
mi=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/market-internals.json")["Body"].read())
print("=== MARKET-INTERNALS ===  sessions:",mi.get("sessions"))
print("latest:",json.dumps(mi.get("latest",{})))
print("volume:",json.dumps(mi.get("volume",{})))
print("rotation:",json.dumps(mi.get("rotation",{})))
print("mcclellan:",json.dumps(mi.get("mcclellan",{})))

# 2. breadth-thrust — is it scheduled now? invoke + verify
print("\n=== BREADTH-THRUST ===")
try:
    r=ev.describe_rule(Name="justhodl-breadth-thrust-daily")
    print("schedule rule:",r.get("ScheduleExpression"),r.get("State"))
except Exception as e: print("rule:",str(e)[:50])
try:
    for _ in range(20):
        c=lam.get_function(FunctionName="justhodl-breadth-thrust")["Configuration"]
        if c.get("LastUpdateStatus")=="Successful": break
        time.sleep(3)
    resp=lam.invoke(FunctionName="justhodl-breadth-thrust",InvocationType="RequestResponse")
    print("invoke status:",resp["StatusCode"])
    bt=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/breadth-thrust.json")["Body"].read())
    print("state:",bt.get("state"),"| zweig:",json.dumps(bt.get("zweig",bt.get("zweig_thrust",{})))[:160])
    print("keys:",list(bt.keys())[:12])
except Exception as e: print("breadth-thrust:",str(e)[:120])
print("DONE 2156")
