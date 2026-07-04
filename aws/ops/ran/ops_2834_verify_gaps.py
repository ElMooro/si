"""ops 2834 — set FRED key on census agent + verify supercore, control group,
core capex, and freight desk."""
import os, json, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
REGION="us-east-1"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=120,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":2834,"ts":datetime.now(timezone.utc).isoformat()}
def wait_ready(fn,t=40):
    for _ in range(t):
        try:
            c=lam.get_function_configuration(FunctionName=fn)
            if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return True
        except Exception: pass
        time.sleep(3)
    return False
# set FRED_KEY on census-economic-agent (for core capex)
try:
    FRED=lam.get_function_configuration(FunctionName="justhodl-china-liquidity").get("Environment",{}).get("Variables",{}).get("FRED_API_KEY","")
    cur=lam.get_function_configuration(FunctionName="census-economic-agent").get("Environment",{}).get("Variables",{})
    if FRED and cur.get("FRED_API_KEY")!=FRED:
        cur["FRED_API_KEY"]=FRED
        wait_ready("census-economic-agent"); lam.update_function_configuration(FunctionName="census-economic-agent",Environment={"Variables":cur}); wait_ready("census-economic-agent")
    R["census_fred_set"]=bool(FRED)
except Exception as e: R["census_fred_set"]="err "+str(e)[:80]
# invoke all 3
for fn in ("bls-labor-agent","census-economic-agent","justhodl-macro-leads"):
    try: lam.invoke(FunctionName=fn,InvocationType="RequestResponse")["Payload"].read()
    except Exception as e: R.setdefault("inv_err",{})[fn]=str(e)[:80]
time.sleep(3)
# BLS supercore
b=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bls-labor.json")["Body"].read())
R["supercore"]={"supercore_yoy_pct":b.get("summary",{}).get("supercore_yoy_pct"),
    "core_services_yoy":b.get("summary",{}).get("core_services_yoy_pct"),
    "shelter_yoy":b.get("summary",{}).get("shelter_yoy_pct"),
    "raw":(b.get("inflation",{}).get("cpi_supercore") or {})}
# Census control group + core capex
c=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/census-economic.json")["Body"].read())
R["census"]={"control_group":c.get("control_group"),
    "core_capex":(c.get("manufacturing_orders",{}) or {}).get("core_capex_orders"),
    "durable_goods":(c.get("manufacturing_orders",{}) or {}).get("durable_goods_orders"),
    "summary_ctrl":{k:c.get("summary",{}).get(k) for k in ("control_group_mom_pct","control_group_yoy_pct","core_capex_mom_pct")}}
# Freight
m=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/macro-leads.json")["Body"].read())
fr=m.get("freight_activity") or {}
R["freight"]={"composite":fr.get("composite"),
    "cass":(fr.get("cass_shipments") or {}).get("yoy_pct"),
    "bts":(fr.get("bts_freight_tsi") or {}).get("yoy_pct"),
    "rail":(fr.get("rail_intermodal") or {}).get("yoy_pct"),
    "truck":(fr.get("truck_tonnage") or {}).get("yoy_pct")}
print(json.dumps(R,indent=1,default=str))
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2834_verify_gaps.json","w"),indent=1,default=str)
print("OPS 2834 COMPLETE")
