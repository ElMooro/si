import os, json, time, boto3
from datetime import datetime, timezone
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=180,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name="us-east-1")
R={"ops":2849,"ts":datetime.now(timezone.utc).isoformat()}
try: lam.invoke(FunctionName="justhodl-canary-grid",InvocationType="RequestResponse")["Payload"].read()
except Exception as e: R["inv"]=str(e)[:100]
time.sleep(3)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/canary-grid.json")["Body"].read())
sigs=d.get("signals") or []
want={"taiwan_export_orders":None,"taiwan_semiconductor":None,"semiconductor_ip":None,"korea_exports":None}
for s in sigs:
    if s.get("key") in want:
        want[s["key"]]={"available":s.get("available"),"value":s.get("value"),"band":s.get("band"),"stress":s.get("stress_score") or s.get("stress"),"as_of":s.get("as_of"),"reason":s.get("reason")}
R["taiwan_canaries"]=want
R["early_warning_level"]=d.get("early_warning_level"); R["band"]=d.get("band"); R["n_available"]=d.get("n_available"); R["n_total"]=d.get("n_total")
R["headline"]=d.get("headline")
R["status"]="LIVE" if (want["taiwan_export_orders"] and want["taiwan_export_orders"].get("available")) and (want["taiwan_semiconductor"] and want["taiwan_semiconductor"].get("available")) else "CHECK"
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:2400])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2849_verify_tw_canary.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2849 COMPLETE")
