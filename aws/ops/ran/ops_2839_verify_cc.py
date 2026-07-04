"""ops 2839 — verify hard-data recession cluster fused into cycle-clock."""
import os, json, time, boto3
from datetime import datetime, timezone
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name="us-east-1")
R={"ops":2839,"ts":datetime.now(timezone.utc).isoformat()}
try:
    lam.invoke(FunctionName="justhodl-cycle-clock",InvocationType="RequestResponse")["Payload"].read()
except Exception as e:
    R["invoke_note"]=str(e)[:100]
time.sleep(3)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/cycle-clock.json")["Body"].read())
R["recession_prob_pct"]=d.get("recession_prob_pct")
R["hard_data_recession"]=d.get("hard_data_recession")
R["sahm"]=(d.get("sahm") or {}).get("value")
R["headline_phase"]=d.get("headline_phase")
R["cycle_verdict"]=(d.get("verdict") or d.get("executive_read") or "")[:160] if isinstance(d.get("verdict") or d.get("executive_read"),str) else None
hdr=d.get("hard_data_recession") or {}
R["status"]="FUSION LIVE" if hdr.get("read") and R["recession_prob_pct"] is not None else "CHECK"
print(json.dumps(R,indent=1,default=str))
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2839_verify_cc.json","w"),indent=1,default=str)
print("OPS 2839 COMPLETE")
