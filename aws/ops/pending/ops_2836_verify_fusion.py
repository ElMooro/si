"""ops 2836 — verify nowcast-desk fusion: supercore + growth confirmation + regime_confidence."""
import os, json, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=120,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name="us-east-1")
R={"ops":2836,"ts":datetime.now(timezone.utc).isoformat()}
lam.invoke(FunctionName="justhodl-nowcast-desk",InvocationType="RequestResponse")["Payload"].read(); time.sleep(2)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/nowcast-desk.json")["Body"].read())
R["supercore"]=(d.get("underlying_inflation",{}) or {}).get("supercore")
R["growth_confirmation"]=d.get("growth_confirmation")
R["quadrant"]=d.get("nowcast_quadrant")
R["status"]="FUSION LIVE" if (R["supercore"] and R["growth_confirmation"] and (R["quadrant"] or {}).get("regime_confidence")) else "CHECK"
print(json.dumps(R,indent=1,default=str))
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2836_verify_fusion.json","w"),indent=1,default=str)
print("OPS 2836 COMPLETE")
