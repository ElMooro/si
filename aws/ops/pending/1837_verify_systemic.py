import json, time, boto3
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=300,retries={"max_attempts":2}))
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"; FN="justhodl-systemic-stress"
for _ in range(30):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("State")=="Active" and c.get("LastUpdateStatus")!="InProgress": break
    time.sleep(5)
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("invoke:",r["Payload"].read().decode()[:200])
d=json.loads(s3.get_object(Bucket=B,Key="data/systemic-stress.json")["Body"].read())
print("composite:",d.get("composite"))
cr=d.get("cross_reference") or d.get("cross") or {}
# find cross block wherever it is
def find_euro(o):
    if isinstance(o,dict):
        if "eurodollar_verdict" in o: return {k:o[k] for k in o if k.startswith("eurodollar")}
        for v in o.values():
            r=find_euro(v)
            if r: return r
    return None
print("eurodollar in payload:", find_euro(d))
print("headline:", d.get("headline"))
