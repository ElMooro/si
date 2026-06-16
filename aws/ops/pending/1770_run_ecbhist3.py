import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
print("invoke:", lam.invoke(FunctionName="justhodl-ecb-history",InvocationType="RequestResponse")["Payload"].read().decode()[:120])
mani=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ecb-hist/_manifest.json")["Body"].read())
ser=mani.get("series") if isinstance(mani,dict) else mani
unemp=sorted([s["id"] for s in ser if s["id"].startswith("unemp")])
print(f"unemployment series now ({len(unemp)}): {unemp}")
indp=sorted([s["id"] for s in ser if s["id"].startswith("indprod") or s["id"] in ("manuf_turnover","retail_turnover")])
print(f"industrial/manuf series ({len(indp)}): {indp}")
print("total manifest series:", len(ser))
