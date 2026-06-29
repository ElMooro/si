import boto3, json
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
r=lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="RequestResponse",Payload=b"{}")
print("FunctionError:",r.get("FunctionError"))
pl=json.loads(r["Payload"].read())
print("errorType:",pl.get("errorType"))
print("errorMessage:",pl.get("errorMessage"))
for line in (pl.get("stackTrace") or [])[-8:]:
    print("ST:",str(line).strip()[:200])
print("DONE 2484")
