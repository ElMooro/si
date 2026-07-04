import os, json, time, boto3
from datetime import datetime, timezone
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=120,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name="us-east-1")
R={"ops":2837,"ts":datetime.now(timezone.utc).isoformat()}
lam.invoke(FunctionName="justhodl-nowcast-desk",InvocationType="RequestResponse")["Payload"].read(); time.sleep(2)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/nowcast-desk.json")["Body"].read())
R["supercore_yoy"]=(d.get("underlying_inflation",{}).get("supercore") or {}).get("yoy_pct")
R["quadrant_supercore"]=(d.get("nowcast_quadrant") or {}).get("supercore_yoy")
R["hard_data_bias"]=(d.get("growth_confirmation") or {}).get("hard_data_bias")
R["regime_confidence"]=(d.get("nowcast_quadrant") or {}).get("regime_confidence")
R["status"]="FIXED" if R["supercore_yoy"] and R["supercore_yoy"]<6 else "CHECK"
print(json.dumps(R,indent=1,default=str))
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2837_recheck.json","w"),indent=1,default=str)
print("OPS 2837 COMPLETE")
