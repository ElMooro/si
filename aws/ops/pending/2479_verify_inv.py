import boto3, json
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
r=lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="RequestResponse",Payload=b"{}")
print("err:",r.get("FunctionError"))
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
print("version:",d.get("version"),"dur:",d.get("duration_s"))
I=d.get("inventory_signal") or {}
print("=== #2 INVENTORY-TO-SALES (Burry) ===")
print("n_sectors_drawing:",I.get("n_sectors_drawing"))
for s in (I.get("sectors_drawing_down") or [])[:6]:
    print("   %-22s ratio=%-5s chg6m=%-6s pctile5y=%s"%(s.get("sector"),s.get("ratio"),s.get("chg_6m"),s.get("percentile_5y")))
print("pre_shortage_names:",[(n.get("ticker"),n.get("industry"),"dio "+str(n.get("dio_chg_pct"))+"%","rev "+str(n.get("rev_growth_yoy"))+"%") for n in (I.get("pre_shortage_names") or [])[:6]])
L=d.get("leading_bottleneck_read") or {}
print("CAPSTONE forward_state:",L.get("forward_state"),"n_confirmations:",L.get("n_confirmations"))
for c in L.get("confirmations",[]): print("   +",c)
print("DONE 2479")
