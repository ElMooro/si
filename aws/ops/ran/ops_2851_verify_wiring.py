import os, json, time, boto3
from datetime import datetime, timezone
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=60,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name="us-east-1")
R={"ops":2851,"ts":datetime.now(timezone.utc).isoformat()}
# apac-flows: sync (quick); bottleneck-boom: ASYNC (heavy) then poll S3
try: lam.invoke(FunctionName="justhodl-apac-flows",InvocationType="RequestResponse")["Payload"].read()
except Exception as e: R["af_inv"]=str(e)[:80]
try: lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="Event")
except Exception as e: R["bb_inv"]=str(e)[:80]
# poll bottleneck-boom S3 for a fresh run with the taiwan block (up to ~5min)
bb_taiwan=None
for _ in range(18):
    time.sleep(18)
    try:
        bb=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
        if isinstance(bb.get("taiwan_semiconductor"),dict):
            bb_taiwan=bb["taiwan_semiconductor"]; break
    except Exception: pass
R["bottleneck_taiwan_semiconductor"]=bb_taiwan
try:
    af=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/apac-flows.json")["Body"].read())
    R["apac_taiwan_fundamentals"]=af.get("taiwan_fundamentals")
except Exception as e: R["af_err"]=str(e)[:80]
ok=(isinstance(bb_taiwan,dict) and bb_taiwan.get("read")) and (isinstance(R.get("apac_taiwan_fundamentals"),dict) and R["apac_taiwan_fundamentals"].get("export_orders_yoy_pct") is not None)
R["status"]="WIRED LIVE" if ok else "CHECK"
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:2000])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2851_verify_wiring.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2851 COMPLETE")
