import os, json, time, boto3
from datetime import datetime, timezone
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name="us-east-1")
R={"ops":2842,"ts":datetime.now(timezone.utc).isoformat()}
try: lam.invoke(FunctionName="justhodl-cycle-clock",InvocationType="RequestResponse")["Payload"].read()
except Exception as e: R["inv_note"]=str(e)[:100]
time.sleep(3)
cc=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/cycle-clock.json")["Body"].read()).get("cycle",{})
R["manufacturing_cycle_lead"]=cc.get("manufacturing_cycle_lead")
R["recession_prob_pct"]=cc.get("recession_prob_pct")
R["hard_data"]=(cc.get("hard_data_recession") or {}).get("read")
ml=cc.get("manufacturing_cycle_lead") or {}
R["status"]="MFG LEAD LIVE" if ml.get("lead_verdict") and ml.get("capacity_utilization_pct") else "CHECK"
print(json.dumps(R,indent=1,default=str))
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2842_verify_mfglead.json","w"),indent=1,default=str)
print("OPS 2842 COMPLETE")
