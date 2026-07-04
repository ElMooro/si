"""ops 2806 v2 — find working FRED key, redeploy macro-leads (waits between
Lambda updates), verify all blocks. Always writes a report."""
import os, io, json, time, zipfile, subprocess, urllib.request
from datetime import datetime, timezone
import boto3
REGION="us-east-1"; FN="justhodl-macro-leads"; SRC="aws/lambdas/%s/source"%FN
R={"ops":2806,"ts":datetime.now(timezone.utc).isoformat(),"steps":{}}
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)

def wait_ready(fn,tries=40):
    for _ in range(tries):
        try:
            c=lam.get_function_configuration(FunctionName=fn)
            if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return True
        except Exception: pass
        time.sleep(3)
    return False

def finish():
    R.setdefault("status","DONE")
    os.makedirs("aws/ops/reports",exist_ok=True)
    json.dump(R,open("aws/ops/reports/2806_macro_leads_fix.json","w"),indent=1,default=str)
    print(json.dumps(R["steps"],indent=1,default=str)); print("OPS 2806 COMPLETE")

try:
    # 1) working FRED key
    def fred_ok(key):
        try:
            u="https://api.stlouisfed.org/fred/series/observations?series_id=DGS10&api_key=%s&file_type=json&limit=1"%key
            return "observations" in json.loads(urllib.request.urlopen(u,timeout=20).read())
        except Exception: return False
    working=None
    for eng in ("fedliquidityapi","economyapi","justhodl-secretary","justhodl-china-liquidity","justhodl-global-macro","justhodl-cycle-clock","daily-report-v3","justhodl-morning-intelligence","justhodl-eurodollar-plumbing"):
        try:
            env=lam.get_function_configuration(FunctionName=eng).get("Environment",{}).get("Variables",{})
            k=env.get("FRED_API_KEY") or env.get("FRED_KEY")
            if k and fred_ok(k): working=k; R["steps"]["fred_key_from"]=eng; break
        except Exception: pass
    R["steps"]["fred_key_working"]=bool(working)

    # 2) rebuild zip + redeploy code, WAIT, then config
    tmp="/tmp/ml3"; subprocess.run("rm -rf %s && mkdir -p %s"%(tmp,tmp),shell=True)
    subprocess.run("pip install xlrd==2.0.1 -t %s --quiet --break-system-packages"%tmp,shell=True)
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
        for fn in os.listdir(SRC):
            if fn.endswith(".py"): z.write(os.path.join(SRC,fn),fn)
        for fn in os.listdir("aws/shared"):
            if fn.endswith(".py"): z.write(os.path.join("aws/shared",fn),fn)
        for root,_,files in os.walk(tmp):
            for f in files: fp=os.path.join(root,f); z.write(fp,os.path.relpath(fp,tmp))
    lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    R["steps"]["code_ready"]=wait_ready(FN)
    envv={"FMP_KEY":"wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"}
    if working: envv["FRED_API_KEY"]=working
    lam.update_function_configuration(FunctionName=FN,Environment={"Variables":envv},Timeout=150)
    R["steps"]["cfg_ready"]=wait_ready(FN)

    # 3) reseed + verify
    lam.invoke(FunctionName=FN,InvocationType="RequestResponse")["Payload"].read(); time.sleep(2)
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/macro-leads.json")["Body"].read())
    cgs=d.get("copper_gold_silver") or {}; rc=d.get("rate_cut_diffusion") or {}; gpr=d.get("geopolitical_risk") or {}; ht=d.get("heavy_truck_sales") or {}
    R["steps"]["verify"]={"populated":d.get("_populated"),
        "copper_gold":(cgs.get("copper_gold") or {}).get("ratio") if isinstance(cgs.get("copper_gold"),dict) else cgs.get("error"),
        "gold_silver":(cgs.get("gold_silver") or {}).get("ratio") if isinstance(cgs.get("gold_silver"),dict) else None,
        "rate_cut_net_pct":rc.get("net_pct_cutting",rc.get("error")),"rate_cut_regime":rc.get("regime"),"n_cbs":rc.get("n_central_banks"),
        "gpr":gpr.get("gpr",gpr.get("error")),
        "heavy_truck":ht.get("units_saar_thousands",ht.get("error")),"heavy_truck_yoy":ht.get("yoy_pct")}
    R["status"]="MACRO-LEADS VERIFIED"
except Exception as e:
    R["steps"]["ERROR"]=repr(e)[:200]; R["status"]="ERR"
finish()
