"""ops 2805 — create justhodl-macro-leads (missing free indicators) + resurrect
dormant eia-energy-agent (schedule it). Seed + verify both."""
import os, io, json, time, zipfile, subprocess
from datetime import datetime, timezone
import boto3

REGION="us-east-1"; ACCT="857687956942"; ROLE="arn:aws:iam::%s:role/lambda-execution-role"%ACCT
FN="justhodl-macro-leads"; SRC="aws/lambdas/%s/source"%FN
R={"ops":2805,"ts":datetime.now(timezone.utc).isoformat(),"steps":{}}
lam=boto3.client("lambda",region_name=REGION); events=boto3.client("events",region_name=REGION); s3=boto3.client("s3",region_name=REGION)

# 0) keys: FRED from an existing engine's env; FMP from memory value
FRED_KEY=""
for src in ("justhodl-china-liquidity","fedliquidityapi","justhodl-global-macro"):
    try:
        env=lam.get_function_configuration(FunctionName=src).get("Environment",{}).get("Variables",{})
        FRED_KEY=env.get("FRED_API_KEY") or env.get("FRED_KEY") or FRED_KEY
        if FRED_KEY: break
    except Exception: pass
FMP_KEY="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
R["steps"]["fred_key_found"]=bool(FRED_KEY)

# 1) build zip: source + shared + vendored xlrd (for GPR)
tmp="/tmp/mlpkg"; subprocess.run("rm -rf %s && mkdir -p %s"%(tmp,tmp),shell=True)
subprocess.run("pip install xlrd==2.0.1 -t %s --quiet --break-system-packages"%tmp,shell=True)
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    for fn in os.listdir(SRC):
        if fn.endswith(".py"): z.write(os.path.join(SRC,fn),fn)
    for fn in os.listdir("aws/shared"):
        if fn.endswith(".py"): z.write(os.path.join("aws/shared",fn),fn)
    for root,_,files in os.walk(tmp):
        for f in files:
            fp=os.path.join(root,f); z.write(fp,os.path.relpath(fp,tmp))
zb=buf.getvalue(); R["steps"]["zip_bytes"]=len(zb)

# 2) create/update macro-leads
envvars={"FRED_API_KEY":FRED_KEY,"FMP_KEY":FMP_KEY}
try:
    lam.get_function(FunctionName=FN)
    lam.update_function_code(FunctionName=FN,ZipFile=zb); time.sleep(6)
    lam.update_function_configuration(FunctionName=FN,Timeout=150,MemorySize=256,Environment={"Variables":envvars})
    R["steps"]["fn"]="updated"
except lam.exceptions.ResourceNotFoundException:
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Timeout=150,MemorySize=256,Environment={"Variables":envvars},Code={"ZipFile":zb})
    R["steps"]["fn"]="created"
for _ in range(30):
    try:
        if lam.get_function_configuration(FunctionName=FN)["State"]=="Active": break
    except Exception: pass
    time.sleep(2)

# 3) EventBridge for macro-leads
def wire_schedule(fn,rule,cron,desc):
    events.put_rule(Name=rule,ScheduleExpression=cron,State="ENABLED",Description=desc)
    arn=lam.get_function(FunctionName=fn)["Configuration"]["FunctionArn"]
    try: lam.add_permission(FunctionName=fn,StatementId=rule+"-inv",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn="arn:aws:events:%s:%s:rule/%s"%(REGION,ACCT,rule))
    except lam.exceptions.ResourceConflictException: pass
    events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":arn}])
try: wire_schedule(FN,"justhodl-macro-leads-daily","cron(20 12 * * ? *)","Daily macro leads"); R["steps"]["ml_schedule"]="wired"
except Exception as e: R["steps"]["ml_schedule"]="ERR "+str(e)[:100]

# 4) seed macro-leads + verify
try:
    lam.invoke(FunctionName=FN,InvocationType="RequestResponse")["Payload"].read(); time.sleep(2)
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/macro-leads.json")["Body"].read())
    R["steps"]["macro_leads"]={"populated":d.get("_populated"),
        "copper_gold":(d.get("copper_gold_silver") or {}).get("copper_gold"),
        "rate_cut":(d.get("rate_cut_diffusion") or {}).get("net_pct_cutting"),
        "gpr":(d.get("geopolitical_risk") or {}).get("gpr") or (d.get("geopolitical_risk") or {}).get("error"),
        "heavy_truck":(d.get("heavy_truck_sales") or {}).get("units_saar_thousands") or (d.get("heavy_truck_sales") or {}).get("error")}
except Exception as e: R["steps"]["macro_leads"]="ERR "+str(e)[:120]

# 5) resurrect eia-energy-agent (schedule + seed + find output)
try:
    wire_schedule("eia-energy-agent","eia-energy-agent-daily","cron(40 11 * * ? *)","Daily EIA energy refresh")
    R["steps"]["eia_schedule"]="wired cron(40 11)"
    inv=lam.invoke(FunctionName="eia-energy-agent",InvocationType="RequestResponse")
    payload=inv["Payload"].read().decode()[:300]
    R["steps"]["eia_invoke"]=payload
    # probe likely output keys
    found=None
    for k in ("data/eia-energy.json","data/energy.json","data/eia.json","data/eia-energy-agent.json","eia-energy.json"):
        try:
            o=s3.head_object(Bucket="justhodl-dashboard-live",Key=k); found={"key":k,"mtime":str(o["LastModified"])}; break
        except Exception: continue
    R["steps"]["eia_output"]=found or "no known output key — engine may not write S3 (needs code fix)"
except Exception as e:
    R["steps"]["eia"]="ERR "+str(e)[:120]

R["status"]="MACRO-LEADS LIVE + EIA CHECKED"
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/2805_macro_leads.json","w"),indent=1,default=str)
print(json.dumps(R["steps"],indent=1,default=str)); print("OPS 2805 COMPLETE")
