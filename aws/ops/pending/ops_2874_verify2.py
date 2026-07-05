import os, json, time, boto3
from datetime import datetime, timezone
lam=boto3.client("lambda",region_name="us-east-1"); s3=boto3.client("s3",region_name="us-east-1")
R={"ops":2874,"ts":datetime.now(timezone.utc).isoformat()}
for fn in ("justhodl-canary-grid","justhodl-crisis-canaries"):
    try: lam.invoke(FunctionName=fn,InvocationType="RequestResponse")["Payload"].read()
    except Exception as e: R.setdefault("inv_err",{})[fn]=str(e)[:60]
time.sleep(4)
cg=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/canary-grid.json")["Body"].read())
R["cfnai"]=next(({"avail":s.get("available"),"val":s.get("value"),"stress":s.get("stress"),"grid":s.get("sub_grid")} for s in cg.get("signals",[]) if s.get("key")=="cfnai_activity"),"missing")
R["grid_total"]=cg.get("n_total"); R["grid_avail"]=cg.get("n_available"); R["grid_ew"]=cg.get("early_warning_level")
cc=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crisis-canaries.json")["Body"].read())
can=cc.get("canaries",{})
R["small_large"]=can.get("small_large_breadth","missing") if isinstance(can,dict) else "canaries-not-dict"
R["crisis_composite"]=cc.get("composite_score"); R["crisis_internals"]=(cc.get("families") or {}).get("internals")
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:1800])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2874_verify2.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2874 COMPLETE")
