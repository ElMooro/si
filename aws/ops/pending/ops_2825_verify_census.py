"""ops 2825 — verify census retail fix + BEA saving rate + GDP sanity."""
import os, json, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=120,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name="us-east-1")
R={"ops":2825,"ts":datetime.now(timezone.utc).isoformat()}
lam.invoke(FunctionName="census-economic-agent",InvocationType="RequestResponse")["Payload"].read(); time.sleep(2)
c=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/census-economic.json")["Body"].read())
R["census"]={"series_live":c.get("_series_live"),"summary":c.get("summary"),
    "retail_trade":c.get("retail",{}).get("retail_trade"),"total":c.get("retail",{}).get("retail_and_food_services")}
b=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bea-economic.json")["Body"].read())
R["bea_income"]=b.get("income"); R["bea_gdp"]=b.get("gdp")
print(json.dumps(R,indent=1,default=str))
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2825_verify_census.json","w"),indent=1,default=str)
print("OPS 2825 COMPLETE")
