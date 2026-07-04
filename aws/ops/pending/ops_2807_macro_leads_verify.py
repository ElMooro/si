"""ops 2807 — redeploy macro-leads (Yahoo metals + FRED recency fix), verify all 4 blocks."""
import os, io, json, time, zipfile, subprocess
from datetime import datetime, timezone
import boto3
REGION="us-east-1"; FN="justhodl-macro-leads"; SRC="aws/lambdas/%s/source"%FN
R={"ops":2807,"ts":datetime.now(timezone.utc).isoformat(),"steps":{}}
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
def wait_ready(fn,tries=40):
    for _ in range(tries):
        try:
            c=lam.get_function_configuration(FunctionName=fn)
            if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return True
        except Exception: pass
        time.sleep(3)
    return False
try:
    tmp="/tmp/ml4"; subprocess.run("rm -rf %s && mkdir -p %s"%(tmp,tmp),shell=True)
    subprocess.run("pip install xlrd==2.0.1 -t %s --quiet --break-system-packages"%tmp,shell=True)
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
        for fn in os.listdir(SRC):
            if fn.endswith(".py"): z.write(os.path.join(SRC,fn),fn)
        for fn in os.listdir("aws/shared"):
            if fn.endswith(".py"): z.write(os.path.join("aws/shared",fn),fn)
        for root,_,files in os.walk(tmp):
            for f in files: fp=os.path.join(root,f); z.write(fp,os.path.relpath(fp,tmp))
    lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); R["steps"]["ready"]=wait_ready(FN)
    lam.invoke(FunctionName=FN,InvocationType="RequestResponse")["Payload"].read(); time.sleep(2)
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/macro-leads.json")["Body"].read())
    cgs=d.get("copper_gold_silver") or {}; rc=d.get("rate_cut_diffusion") or {}; gpr=d.get("geopolitical_risk") or {}; ht=d.get("heavy_truck_sales") or {}
    R["steps"]["verify"]={"populated":d.get("_populated"),
        "copper_gold":(cgs.get("copper_gold") or {}).get("ratio") if isinstance(cgs.get("copper_gold"),dict) else cgs.get("error"),
        "copper_gold_z":(cgs.get("copper_gold") or {}).get("z_1y") if isinstance(cgs.get("copper_gold"),dict) else None,
        "gold_silver":(cgs.get("gold_silver") or {}).get("ratio") if isinstance(cgs.get("gold_silver"),dict) else None,
        "rate_cut_net_pct":rc.get("net_pct_cutting",rc.get("error")),"rate_cut_regime":rc.get("regime"),"n_cbs":rc.get("n_central_banks"),
        "gpr":gpr.get("gpr",gpr.get("error")),
        "heavy_truck":ht.get("units_saar_thousands",ht.get("error")),"heavy_truck_yoy":ht.get("yoy_pct")}
    R["status"]="VERIFIED"
except Exception as e:
    R["steps"]["ERROR"]=repr(e)[:200]; R["status"]="ERR"
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/2807_macro_leads_verify.json","w"),indent=1,default=str)
print(json.dumps(R["steps"],indent=1,default=str)); print("OPS 2807 COMPLETE")
