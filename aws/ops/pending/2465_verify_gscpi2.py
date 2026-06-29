import boto3, json
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
r=lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="RequestResponse",Payload=b"{}")
print("err:",r.get("FunctionError"))
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
p=d.get("physical_throughput") or {}
print("version:",d.get("version"))
print("GSCPI:",json.dumps(p.get("gscpi")))
print("physical_pressure_z:",p.get("physical_pressure_z"),"state:",p.get("physical_state"),"confirms:",p.get("confirms_bottleneck"))
print("DONE 2465")
