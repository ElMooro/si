import os, json, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=120,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name="us-east-1")
R={"ops":2831,"ts":datetime.now(timezone.utc).isoformat()}
for fn in ("bls-labor-agent","bea-economic-agent"):
    try: lam.invoke(FunctionName=fn,InvocationType="RequestResponse")["Payload"].read()
    except Exception: pass
time.sleep(3)
b=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bls-labor.json")["Body"].read())
R["bls_prod"]={k:b.get("summary",{}).get(k) for k in ("unit_labor_costs_qoq_pct","productivity_qoq_pct","real_hourly_comp_qoq_pct")}
be=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bea-economic.json")["Body"].read())
R["bea"]={"gdp_gdi":be.get("gdp_gdi"),"corporate_profits":be.get("corporate_profits"),"contributions":be.get("gdp_contributions_pp")}
print(json.dumps(R,indent=1,default=str))
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2831_verify2.json","w"),indent=1,default=str)
print("OPS 2831 COMPLETE")
