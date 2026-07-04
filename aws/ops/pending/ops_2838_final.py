import os, json, time, boto3
from datetime import datetime, timezone
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=120,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name="us-east-1")
R={"ops":2838,"ts":datetime.now(timezone.utc).isoformat()}
lam.invoke(FunctionName="justhodl-nowcast-desk",InvocationType="RequestResponse")["Payload"].read(); time.sleep(2)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/nowcast-desk.json")["Body"].read())
q=d.get("nowcast_quadrant") or {}
R["fusion"]={"supercore_yoy":(d.get("underlying_inflation",{}).get("supercore") or {}).get("yoy_pct"),
    "regime":q.get("regime"),"supercore_in_quadrant":q.get("supercore_yoy"),
    "growth_confirmation":q.get("growth_confirmation"),"regime_confidence":q.get("regime_confidence"),
    "hard":{k:(d.get("growth_confirmation") or {}).get(k) for k in ("gdp_gdi_gap_pct","freight_read","core_capex_yoy_pct")}}
R["status"]="OK" if R["fusion"]["supercore_yoy"]==3.7 else "supercore="+str(R["fusion"]["supercore_yoy"])
print(json.dumps(R,indent=1,default=str))
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2838_final.json","w"),indent=1,default=str)
print("OPS 2838 COMPLETE")
