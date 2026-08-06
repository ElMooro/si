import json,os
import boto3
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
R={}
try:
    R["dbg"]=json.loads(s3.get_object(Bucket=B,Key="data/audit/provider-join-debug.json")["Body"].read())
except Exception as e: R["dbg"]={"err":str(e)[:60]}
try:
    f=json.loads(s3.get_object(Bucket=B,Key="data/providers/fred.json")["Body"].read())
    R["fred_keys_sample"]=[k["key"] for k in f.get("keys",[])[:6]]
    R["fred_via_rollup"]=sum(1 for k in f.get("keys",[]) if k.get("via")=="rollup")
except Exception as e: R["fred_err"]=str(e)[:60]
os.makedirs("aws/ops/reports",exist_ok=True)
open("aws/ops/reports/4521_truth.md","w").write("# 4521 — "+json.dumps(R,default=str)[:900]+"\n")
print(json.dumps(R,default=str)[:400])
