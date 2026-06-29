import boto3, json
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
r=lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="RequestResponse",Payload=b"{}")
print("err:",r.get("FunctionError"))
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
print("version:",d.get("version"),"dur:",d.get("duration_s"))
P=d.get("ppi_signal") or {}
print("=== #5 PPI PRICING POWER ===",json.dumps(P)[:600])
L=d.get("leading_bottleneck_read") or {}
print("CAPSTONE:",L.get("forward_state"),"n=",L.get("n_confirmations"))
for c in L.get("confirmations",[]): print("   +",c)
print("DONE 2486")
