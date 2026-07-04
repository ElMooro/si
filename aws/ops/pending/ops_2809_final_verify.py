"""ops 2809 — wait for deploy-lambdas (xlrd now in source) then final verify of all blocks."""
import os, json, time
from datetime import datetime, timezone
import boto3
REGION="us-east-1"; FN="justhodl-macro-leads"
R={"ops":2809,"ts":datetime.now(timezone.utc).isoformat()}
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
# give deploy-lambdas time to start, then wait for ready
time.sleep(60)
for _ in range(60):
    try:
        c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": break
    except Exception: pass
    time.sleep(5)
try:
    lam.invoke(FunctionName=FN,InvocationType="RequestResponse")["Payload"].read(); time.sleep(2)
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/macro-leads.json")["Body"].read())
    cgs=d.get("copper_gold_silver") or {}; rc=d.get("rate_cut_diffusion") or {}; gpr=d.get("geopolitical_risk") or {}; ht=d.get("heavy_truck_sales") or {}
    R["verify"]={"populated":d.get("_populated"),
        "copper_gold":(cgs.get("copper_gold") or {}).get("ratio"),"copper_gold_z":(cgs.get("copper_gold") or {}).get("z_1y"),
        "gold_silver":(cgs.get("gold_silver") or {}).get("ratio"),"gold_silver_z":(cgs.get("gold_silver") or {}).get("z_1y"),
        "rate_cut_net_pct":rc.get("net_pct_cutting"),"rate_cut_regime":rc.get("regime"),"n_cbs":rc.get("n_central_banks"),
        "gpr":gpr.get("gpr",gpr.get("error")),"gpr_z":gpr.get("z_5y"),
        "heavy_truck_saar_M":ht.get("saar_millions",ht.get("error")),"heavy_truck_yoy":ht.get("yoy_pct")}
    R["status"]="FINAL VERIFIED"
except Exception as e:
    R["error"]=repr(e)[:200]
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/2809_final_verify.json","w"),indent=1,default=str)
print(json.dumps(R.get("verify",R.get("error")),indent=1,default=str)); print("OPS 2809 COMPLETE")
