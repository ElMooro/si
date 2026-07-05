import os, json, time, boto3
from datetime import datetime, timezone
lam=boto3.client("lambda",region_name="us-east-1"); s3=boto3.client("s3",region_name="us-east-1")
R={"ops":2864,"ts":datetime.now(timezone.utc).isoformat()}
try: lam.invoke(FunctionName="justhodl-canary-grid",InvocationType="RequestResponse")["Payload"].read()
except Exception as e: R["inv"]=str(e)[:60]
time.sleep(3)
cg=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/canary-grid.json")["Body"].read())
jp=[s for s in (cg.get("signals") or []) if s.get("key")=="japan_mfg_orders"]
R["japan_orders"]=({"available":jp[0].get("available"),"value":jp[0].get("value"),"stress":jp[0].get("stress"),"age":jp[0].get("age_days"),"reason":jp[0].get("reason")} if jp else "missing")
# full trade+commodity canary snapshot
keys=["korea_exports","china_exports","semiconductor_ip","taiwan_export_orders","taiwan_semiconductor","singapore_nodx","copper","chile_exports","peru_copper","japan_mfg_orders"]
R["canaries"]={s["key"]:{"avail":s.get("available"),"val":s.get("value"),"stress":s.get("stress")} for s in (cg.get("signals") or []) if s.get("key") in keys}
R["grid"]={"avail":cg.get("n_available"),"total":cg.get("n_total"),"ew":cg.get("early_warning_level"),"band":cg.get("band")}
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:2200])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2864_verify_japan.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2864 COMPLETE")
