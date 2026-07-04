"""ops 2809 — invoke resurrected eia-energy-agent, verify data/eia-energy.json feed."""
import os, json, time
from datetime import datetime, timezone
import boto3
REGION="us-east-1"
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
R={"ops":2809,"ts":datetime.now(timezone.utc).isoformat()}
try:
    inv=lam.invoke(FunctionName="eia-energy-agent",InvocationType="RequestResponse")
    body=json.loads(json.loads(inv["Payload"].read())["body"])
    R["invoke_metrics_ok"]=body.get("metrics_ok"); R["invoke_s3"]=body.get("_s3") or body.get("_s3_error")
    time.sleep(2)
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/eia-energy.json")["Body"].read())
    def val(sid):
        for grp in ("oil_markets","natural_gas","global_supply_demand"):
            o=(d.get(grp) or {}).get(sid)
            if o and o.get("data"): return o["data"].get("value")
        o=(d.get("all_series") or {}).get(sid)
        return (o or {}).get("data",{}).get("value") if o else None
    R["feed"]={"generated_at":d.get("generated_at"),"metrics_ok":d.get("metrics_ok"),"metrics_err":d.get("metrics_err"),
        "WTI":val("WTIPUUS"),"Brent":val("BREPROD"),"US_crude_prod_Mbd":val("COPRPUS"),
        "OPEC_prod_Mbd":val("PAPR_OPEC"),"US_crude_inv_Mb":val("COPS_US"),"HenryHub":val("PRCE_NOM_HENRY"),
        "world_consumption_Mbd":val("PATC_WORLD"),"world_supply_Mbd":val("PASC_WORLD")}
    R["status"]="EIA FEED LIVE" if d.get("metrics_ok",0)>=15 else "CHECK"
except Exception as e:
    R["status"]="ERR"; R["error"]=repr(e)[:200]
print(json.dumps(R,indent=1,default=str))
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/2809_eia_verify.json","w"),indent=1,default=str)
print("OPS 2809 COMPLETE")
