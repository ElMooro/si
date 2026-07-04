"""ops 2816 — provision the EIA API key (from injected secret) onto eia-energy-agent
+ SSM, ONLY if it validates against the EIA API. Never prints the key."""
import os, json, time, urllib.request
from datetime import datetime, timezone
import boto3
REGION="us-east-1"; FN="eia-energy-agent"
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION); ssm=boto3.client("ssm",region_name=REGION)
R={"ops":2816,"ts":datetime.now(timezone.utc).isoformat()}
KEY=os.environ.get("EIA_API_KEY","").strip()
R["key_present"]=bool(KEY); R["key_len"]=len(KEY)
def wait_ready(t=40):
    for _ in range(t):
        try:
            c=lam.get_function_configuration(FunctionName=FN)
            if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return True
        except Exception: pass
        time.sleep(3)
    return False
try:
    if not KEY:
        R["status"]="NO KEY IN ENV"; raise SystemExit
    # 1) VALIDATE against EIA before doing anything
    url="https://api.eia.gov/v2/seriesid/PET.WCESTUS1.W?api_key=%s"%KEY
    try:
        raw=urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"jh"}),timeout=25).read()
        d=json.loads(raw); rows=d.get("response",{}).get("data",[])
        R["eia_validate"]={"http":200,"rows":len(rows)}
        valid=len(rows)>0
    except urllib.error.HTTPError as he:
        R["eia_validate"]={"http":he.code,"msg":he.reason}; valid=False
    if not valid:
        R["status"]="KEY REJECTED BY EIA (403/empty) — likely a typo; not wired"
        raise SystemExit
    # 2) store in SSM (SecureString) + set on Lambda env (merge, keep FRED key)
    ssm.put_parameter(Name="/justhodl/eia-api-key",Value=KEY,Type="SecureString",Overwrite=True)
    cur=lam.get_function_configuration(FunctionName=FN).get("Environment",{}).get("Variables",{})
    cur["EIA_API_KEY"]=KEY
    wait_ready(); lam.update_function_configuration(FunctionName=FN,Environment={"Variables":cur}); wait_ready()
    # 3) invoke + verify the gated blocks now populate
    lam.invoke(FunctionName=FN,InvocationType="RequestResponse")["Payload"].read(); time.sleep(2)
    fd=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/eia-energy.json")["Body"].read())
    inv=fd.get("inventories_production") or {}; steo=fd.get("steo_forecast") or {}
    def v(block,k):
        o=block.get(k); return (o or {}).get("data",{}).get("value") if o else None
    R["unlocked"]={"eia_key_present":fd.get("eia_key_present"),
        "crude_inventories_kbbl":v(inv,"PET.WCESTUS1.W"),"crude_production_kbd":v(inv,"PET.WCRFPUS2.W"),
        "SPR_kbbl":v(inv,"PET.WCSSTUS1.W"),"gasoline_stocks":v(inv,"PET.WGTSTUS1.W"),
        "OPEC_prod_Mbd":v(steo,"PAPR_OPEC"),"world_consumption_Mbd":v(steo,"PATC_WORLD")}
    inv_ok=sum(1 for x in inv.values() if x.get("data")); steo_ok=sum(1 for x in steo.values() if x.get("data"))
    R["blocks_live"]={"inventories":inv_ok,"steo":steo_ok}
    R["status"]="EIA KEY LIVE — full energy dashboard unlocked" if (inv_ok+steo_ok)>=3 else "KEY SET but blocks empty (check series ids)"
except SystemExit:
    pass
except Exception as e:
    R["status"]="ERR"; R["error"]=repr(e)[:200]
print(json.dumps(R,indent=1,default=str))
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/2816_eia_key.json","w"),indent=1,default=str)
print("OPS 2816 COMPLETE")
