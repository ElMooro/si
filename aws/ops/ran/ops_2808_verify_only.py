"""ops 2808 — wait for deploy-lambdas to finish redeploying macro-leads, then invoke + verify (no code update, avoids the race)."""
import os, json, time
from datetime import datetime, timezone
import boto3
REGION="us-east-1"; FN="justhodl-macro-leads"
R={"ops":2808,"ts":datetime.now(timezone.utc).isoformat()}
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
# wait until no update in progress
for _ in range(50):
    try:
        c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": break
    except Exception: pass
    time.sleep(4)
try:
    lam.invoke(FunctionName=FN,InvocationType="RequestResponse")["Payload"].read(); time.sleep(2)
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/macro-leads.json")["Body"].read())
    cgs=d.get("copper_gold_silver") or {}; rc=d.get("rate_cut_diffusion") or {}; gpr=d.get("geopolitical_risk") or {}; ht=d.get("heavy_truck_sales") or {}
    R["verify"]={"populated":d.get("_populated"),
        "copper_gold":(cgs.get("copper_gold") or {}).get("ratio") if isinstance(cgs.get("copper_gold"),dict) else cgs.get("error"),
        "copper_gold_z":(cgs.get("copper_gold") or {}).get("z_1y") if isinstance(cgs.get("copper_gold"),dict) else None,
        "gold_silver":(cgs.get("gold_silver") or {}).get("ratio") if isinstance(cgs.get("gold_silver"),dict) else None,
        "gold_silver_z":(cgs.get("gold_silver") or {}).get("z_1y") if isinstance(cgs.get("gold_silver"),dict) else None,
        "rate_cut_net_pct":rc.get("net_pct_cutting",rc.get("error")),"rate_cut_regime":rc.get("regime"),"n_cbs":rc.get("n_central_banks"),
        "rate_cut_detail":rc.get("by_country"),
        "gpr":gpr.get("gpr",gpr.get("error")),
        "heavy_truck_saar_k":ht.get("units_saar_thousands",ht.get("error")),"heavy_truck_yoy":ht.get("yoy_pct"),"heavy_truck_z":ht.get("z_1y")}
    R["status"]="VERIFIED"
except Exception as e:
    R["error"]=repr(e)[:200]
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/2808_verify_only.json","w"),indent=1,default=str)
print(json.dumps(R.get("verify",R.get("error")),indent=1,default=str)); print("OPS 2808 COMPLETE")
