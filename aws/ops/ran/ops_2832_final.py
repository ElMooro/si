"""ops 2832 — final: confirm BEA GDP fix + page live with new depth."""
import os, json, time, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=120,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name="us-east-1")
R={"ops":2832,"ts":datetime.now(timezone.utc).isoformat()}
try: lam.invoke(FunctionName="bea-economic-agent",InvocationType="RequestResponse")["Payload"].read(); time.sleep(2)
except Exception: pass
be=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bea-economic.json")["Body"].read())
R["bea_gdp_fixed"]={"headline_gdp":be.get("gdp",{}).get("real_gdp_qoq_saar_pct"),"gdp_gdi_gap":be.get("gdp_gdi",{}).get("gap_pct")}
def get(u):
    req=urllib.request.Request(u+"?cb=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0"})
    with urllib.request.urlopen(req,timeout=25) as r: return r.status,r.read().decode("utf-8","ignore")
okp=False
for _ in range(5):
    time.sleep(20)
    try:
        st,b=get("https://justhodl.ai/us-data-desk.html")
        marks=["GDP\u2013GDI gap","Unit labor costs","natural_gas_storage","Corporate profits","renderEIA"]
        hits={m:(m in b) for m in marks}; okp=(st==200 and all(hits.values()))
        R["page"]={"status":st,"markers":hits}
        if okp: break
    except Exception as e: R["page"]="err "+str(e)[:50]
R["status"]="ALL LIVE" if okp and R["bea_gdp_fixed"]["headline_gdp"] and R["bea_gdp_fixed"]["headline_gdp"]<4 else "CHECK"
print(json.dumps(R,indent=1,default=str))
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2832_final.json","w"),indent=1,default=str)
print("OPS 2832 COMPLETE")
