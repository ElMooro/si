import os, json, time, boto3
from datetime import datetime, timezone
lam=boto3.client("lambda",region_name="us-east-1"); s3=boto3.client("s3",region_name="us-east-1")
R={"ops":2871,"ts":datetime.now(timezone.utc).isoformat()}
try: lam.invoke(FunctionName="justhodl-canary-grid",InvocationType="RequestResponse")["Payload"].read()
except Exception as e: R["inv"]=str(e)[:60]
time.sleep(3)
cg=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/canary-grid.json")["Body"].read())
new=["finland_exports","ppi_pulp_paper","hqm_credit_spread"]
R["new"]={s["key"]:{"avail":s.get("available"),"val":s.get("value"),"stress":s.get("stress"),"grid":s.get("sub_grid"),"age":s.get("age_days"),"reason":s.get("reason")} for s in (cg.get("signals") or []) if s.get("key") in new}
R["grid"]={"avail":cg.get("n_available"),"total":cg.get("n_total"),"ew":cg.get("early_warning_level"),"band":cg.get("band")}
R["status"]="LIVE" if sum(1 for k in new if R["new"].get(k,{}).get("avail")) >=3 else "CHECK"
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:1600])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2871_verify.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2871 COMPLETE")
