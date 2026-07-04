import os, json, boto3
from datetime import datetime, timezone
s3=boto3.client("s3",region_name="us-east-1")
R={"ops":2840,"ts":datetime.now(timezone.utc).isoformat()}
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/cycle-clock.json")["Body"].read())
cyc=d.get("cycle") or {}
R["recession_prob_pct"]=cyc.get("recession_prob_pct")
R["hard_data_recession"]=cyc.get("hard_data_recession")
R["sahm"]=(cyc.get("sahm") or {}).get("value")
R["headline_phase"]=cyc.get("headline_phase")
R["status"]="FUSION LIVE" if (cyc.get("hard_data_recession") or {}).get("read") else "CHECK"
print(json.dumps(R,indent=1,default=str))
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2840_recheck_cc.json","w"),indent=1,default=str)
print("OPS 2840 COMPLETE")
