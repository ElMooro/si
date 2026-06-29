import boto3, json
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
r=lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="RequestResponse",Payload=b"{}")
print("err:",r.get("FunctionError"))
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
print("version:",d.get("version"),"dur:",d.get("duration_s"))
C=d.get("capital_availability") or {}
print("=== #1 CAPITAL AVAILABILITY (Marks) ===")
print("hy_oas_pct:",C.get("hy_oas_pct"),"| capital_cost:",C.get("capital_cost"),"| credit_regime:",C.get("credit_regime"))
print("issuance_90d:",C.get("issuance_90d"),"prior:",C.get("issuance_prior_90d"),"trend:",C.get("issuance_trend"))
print("issuers_by_industry:",json.dumps(C.get("issuers_by_industry")))
print("supply_response_funded:",C.get("supply_response_funded"))
print("read:",C.get("read"))
print("DONE 2475")
