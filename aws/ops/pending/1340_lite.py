import json, time
import boto3
from botocore.config import Config
cfg=Config(read_timeout=60,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={}
# best-setups already ran in 1339 (deploy happens before the failing ask invoke)
try:
    bs=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/best-setups.json")["Body"].read())
    out["best_setups_has_brain_field"]=any("brain_aligned" in s for s in bs.get("top_setups",[]))
    out["n_brain_aligned"]=len(bs.get("brain_aligned",[]))
    out["sample_setup_keys"]=list((bs.get("top_setups") or [{}])[0].keys())[:14]
except Exception as e: out["bs_err"]=str(e)[:100]
# confirm ask deployed (code has brain_directive)
try:
    c=lam.get_function_configuration(FunctionName="justhodl-ask")
    out["ask_last_modified"]=c.get("LastModified")
    out["ask_state"]=c.get("State")
except Exception as e: out["ask_err"]=str(e)[:80]
open("aws/ops/reports/1340_lite.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
