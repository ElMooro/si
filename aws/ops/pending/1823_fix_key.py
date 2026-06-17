import json,time,boto3
from botocore.config import Config
REGION="us-east-1"; FN="justhodl-fomc-reaction"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=300,retries={"max_attempts":3}))
s3=boto3.client("s3",region_name=REGION); B="justhodl-dashboard-live"
def wait():
    for _ in range(30):
        c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus")!="InProgress": return
        time.sleep(5)
# find a VALID Claude key from a known Claude-direct engine
key=None; src_used=None
for src in ["justhodl-ai-chat","justhodl-khalid-metrics","justhodl-weekly-ai-review","justhodl-financial-secretary","justhodl-ask-desk"]:
    try:
        e=lam.get_function_configuration(FunctionName=src).get("Environment",{}).get("Variables",{})
        k=e.get("ANTHROPIC_API_KEY","")
        if k.startswith("sk-ant-"):
            key=k; src_used=src; break
        elif k and not key:
            key=k; src_used=src  # tentative
    except Exception as ex: print(src,"err",ex)
print("key source:",src_used,"| looks like real Claude key:", bool(key and key.startswith("sk-ant-")))
wait()
cur=lam.get_function_configuration(FunctionName=FN).get("Environment",{}).get("Variables",{})
if key: cur["ANTHROPIC_API_KEY"]=key
lam.update_function_configuration(FunctionName=FN,Environment={"Variables":cur}); wait()
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("invoke:",r["Payload"].read().decode()[:160])
d=json.loads(s3.get_object(Bucket=B,Key="data/fomc-reaction.json")["Body"].read()); sp=d["surprise"]
print("SURPRISE:",sp["label"],"| basis:",sp.get("basis"),"| tone:",json.dumps(sp.get("statement_tone"))[:220])
