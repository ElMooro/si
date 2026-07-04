"""ops 2811 — set working FRED key on eia-energy-agent, invoke, verify real feed."""
import os, json, time, urllib.request
from datetime import datetime, timezone
import boto3
REGION="us-east-1"; FN="eia-energy-agent"
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
R={"ops":2811,"ts":datetime.now(timezone.utc).isoformat()}
def wait_ready(fn,t=40):
    for _ in range(t):
        try:
            c=lam.get_function_configuration(FunctionName=fn)
            if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return True
        except Exception: pass
        time.sleep(3)
    return False
def fred_ok(k):
    try:
        u="https://api.stlouisfed.org/fred/series/observations?series_id=DCOILWTICO&api_key=%s&file_type=json&limit=1"%k
        return "observations" in json.loads(urllib.request.urlopen(u,timeout=15).read())
    except Exception: return False
try:
    working=None
    for eng in ("justhodl-china-liquidity","fedliquidityapi","economyapi","justhodl-macro-leads"):
        env=lam.get_function_configuration(FunctionName=eng).get("Environment",{}).get("Variables",{})
        k=env.get("FRED_API_KEY") or env.get("FRED_KEY")
        if k and fred_ok(k): working=k; R["fred_from"]=eng; break
    R["fred_working"]=bool(working)
    # merge env (keep existing)
    cur=lam.get_function_configuration(FunctionName=FN).get("Environment",{}).get("Variables",{})
    cur["FRED_API_KEY"]=working or cur.get("FRED_API_KEY","")
    wait_ready(FN)
    lam.update_function_configuration(FunctionName=FN,Environment={"Variables":cur},Timeout=120,MemorySize=256)
    wait_ready(FN)
    body=json.loads(json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse")["Payload"].read())["body"])
    R["invoke"]={"metrics_ok":body.get("metrics_ok"),"metrics_err":body.get("metrics_err"),"s3":body.get("_s3") or body.get("_s3_error")}
    time.sleep(2)
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/eia-energy.json")["Body"].read())
    def val(k):
        o=(d.get("all_series") or {}).get(k)
        return (o or {}).get("data",{}).get("value") if o else None
    R["feed"]={"generated_at":d.get("generated_at"),"metrics_ok":d.get("metrics_ok"),
        "WTI":val("WTIPUUS"),"Brent":val("BREPROD"),"HenryHub":val("PRCE_NOM_HENRY"),
        "US_crude_inv_kbbl":val("COPS_US"),"US_crude_prod_kbd":val("COPRPUS"),
        "SPR_kbbl":val("SPR_US"),"gasoline_usd_gal":val("MGWHUUS"),"diesel_usd_gal":val("D2WHUUS")}
    R["status"]="EIA FEED LIVE (FRED)" if d.get("metrics_ok",0)>=8 else "CHECK"
except Exception as e:
    R["status"]="ERR"; R["error"]=repr(e)[:200]
print(json.dumps(R,indent=1,default=str))
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/2811_eia_fred.json","w"),indent=1,default=str)
print("OPS 2811 COMPLETE")
