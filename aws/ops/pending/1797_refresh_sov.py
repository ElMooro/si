import json, boto3
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=240,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name="us-east-1")
lam.invoke(FunctionName="justhodl-sovereign-fiscal",InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/sovereign-fiscal.json")["Body"].read())
print("ranking top8 (should be clean countries):")
for x in d["tic"]["ranking"][:8]: print("  ",x["country"], x["holdings_bn"], x["chg_12m_bn"])
print("tic as_of:",d["tic"]["as_of"],"| holders:",len(d["tic"]["holders"]))
