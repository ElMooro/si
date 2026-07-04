"""ops 2829 — verify the new macro-depth series across BLS/BEA/EIA."""
import os, json, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=120,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name="us-east-1")
R={"ops":2829,"ts":datetime.now(timezone.utc).isoformat()}
for fn in ("bls-labor-agent","bea-economic-agent","eia-energy-agent"):
    try: lam.invoke(FunctionName=fn,InvocationType="RequestResponse")["Payload"].read()
    except Exception as e: R.setdefault("invoke_err",{})[fn]=str(e)[:80]
time.sleep(3)
# BLS productivity
b=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bls-labor.json")["Body"].read())
R["bls_productivity"]={"ulc_yoy":b.get("summary",{}).get("unit_labor_costs_yoy_pct"),
    "productivity_yoy":b.get("summary",{}).get("productivity_yoy_pct"),
    "raw":{k:(v.get("value") if isinstance(v,dict) else v) for k,v in (b.get("productivity") or {}).items()},
    "api":b.get("api_version")}
# BEA gdp-gdi + contributions + profits
be=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bea-economic.json")["Body"].read())
R["bea_depth"]={"gdp_gdi":be.get("gdp_gdi"),"contributions_pp":be.get("gdp_contributions_pp"),
    "corporate_profits":be.get("corporate_profits"),"err":be.get("_error")}
# EIA natgas + cushing + refinery
e=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/eia-energy.json")["Body"].read())
ng=e.get("natural_gas_storage") or {}
inv=e.get("inventories_production") or {}
def val(blk,sid):
    o=blk.get(sid); return (o or {}).get("data",{}).get("value") if o else None
R["eia_new"]={"natgas_storage_bcf":val(ng,"NG.NW2_EPG0_SWO_R48_BCF.W"),
    "natgas_wow_chg":(ng.get("NG.NW2_EPG0_SWO_R48_BCF.W",{}).get("data",{}) or {}).get("chg"),
    "cushing_kbbl":val(inv,"PET.W_EPC0_SAX_YCUOK_MBBL.W"),
    "refinery_util_pct":val(inv,"PET.WPULEUS3.W"),
    "gasoline_demand_kbd":val(inv,"PET.WGFUPUS2.W")}
print(json.dumps(R,indent=1,default=str))
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2829_verify_depth.json","w"),indent=1,default=str)
print("OPS 2829 COMPLETE")
