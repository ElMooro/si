import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
for _ in range(24):
    st=lam.get_function_configuration(FunctionName="justhodl-morning-intelligence").get("LastUpdateStatus")
    if st=="Successful": break
    time.sleep(5)
print("LastUpdateStatus:",st)
r=lam.invoke(FunctionName="justhodl-morning-intelligence",InvocationType="RequestResponse",Payload=b"{}")
print("invoke err:",r.get("FunctionError")); time.sleep(3)
# find the brief output
import io
key=None
for k in ["data/morning-intelligence.json","data/morning-brief.json","data/intelligence-brief.json"]:
    try: d=json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read()); key=k; break
    except Exception: continue
print("brief key:",key)
blob=json.dumps(d) if key else ""
import re
for ln in re.findall(r'CROSS_ASSET_FLOW:[^"]+', blob)[:1]:
    print("LINE:",ln[:520])
print("has CROSS_ASSET_FLOW:", "CROSS_ASSET_FLOW" in blob)
print("DONE 2518")
