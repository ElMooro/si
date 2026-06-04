import json, boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
s3=boto3.client("s3",region_name="us-east-1",config=cfg)
# 1. current env FRED_KEY
try:
    c=lam.get_function_configuration(FunctionName="justhodl-bond-vol")
    env=c.get("Environment",{}).get("Variables",{})
    print("FRED_KEY set:", bool(env.get("FRED_KEY")), "| len:", len(env.get("FRED_KEY","")))
    print("timeout:", c.get("Timeout"), "| last:", c.get("LastModified"))
except Exception as e: print("cfg err:", str(e)[:120])
# 2. current output
try:
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bond-vol.json")["Body"].read())
    print("\nbond-vol.json generated_at:", d.get("generated_at"))
    print("regime:", d.get("regime"), "| z:", d.get("composite_z"), "| channels_live:", d.get("channels_live"))
    for ch in d.get("channels",[])[:5]:
        print(f"  {ch.get('id')}: {ch.get('status','?')} z={ch.get('z')}")
except Exception as e: print("output err:", str(e)[:150])
open("aws/ops/reports/1274_bondvol_diag.txt","w").write("done")
