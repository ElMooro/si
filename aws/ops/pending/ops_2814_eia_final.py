"""ops 2814 — verify clean eia-energy feed (5 live FRED series, no null clutter)."""
import os, json, time
from datetime import datetime, timezone
import boto3
lam=boto3.client("lambda",region_name="us-east-1"); s3=boto3.client("s3",region_name="us-east-1")
R={"ops":2814,"ts":datetime.now(timezone.utc).isoformat()}
try:
    lam.invoke(FunctionName="eia-energy-agent",InvocationType="RequestResponse")["Payload"].read(); time.sleep(2)
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/eia-energy.json")["Body"].read())
    def v(k):
        o=(d.get("all_series") or {}).get(k); dd=(o or {}).get("data") or {}
        return {"value":dd.get("value"),"chg_pct":dd.get("chg_pct"),"yoy":dd.get("yoy")} if dd else (o or {}).get("error")
    R["feed"]={"generated_at":d.get("generated_at"),"metrics_ok":d.get("metrics_ok"),"metrics_err":d.get("metrics_err"),
        "eia_key_present":d.get("eia_key_present"),
        "WTI":v("WTIPUUS"),"Brent":v("BREPROD"),"HenryHub":v("PRCE_NOM_HENRY"),"gasoline":v("MGWHUUS"),"diesel":v("D2WHUUS")}
    R["status"]="EIA FEED CLEAN+LIVE" if d.get("metrics_ok",0)>=5 else "CHECK"
except Exception as e:
    R["status"]="ERR"; R["error"]=repr(e)[:200]
print(json.dumps(R,indent=1,default=str))
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/2814_eia_final.json","w"),indent=1,default=str)
print("OPS 2814 COMPLETE")
