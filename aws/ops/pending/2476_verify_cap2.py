import boto3, json
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="RequestResponse",Payload=b"{}")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
C=d.get("capital_availability") or {}
print("version:",d.get("version"))
print("hy_oas_pct:",C.get("hy_oas_pct"),"capital_cost:",C.get("capital_cost"))
print("issuance_90d:",C.get("issuance_90d"),"prior:",C.get("issuance_prior_90d"),"trend:",C.get("issuance_trend"))
print("issuers_by_industry:",json.dumps(C.get("issuers_by_industry")))
print("supply_response_funded:",C.get("supply_response_funded"),"| read:",C.get("read"))
print("DONE 2476")
