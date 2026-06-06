import json, time
import boto3
from botocore.config import Config
cfg=Config(read_timeout=90,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={}
# confirm best-setups code is the new one (it deployed in 1339); just re-invoke
r=lam.invoke(FunctionName="justhodl-best-setups",InvocationType="RequestResponse",Payload=b"{}")
out["invoke"]=r.get("Payload").read().decode()[:120]
time.sleep(3)
bs=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/best-setups.json")["Body"].read())
out["has_brain_field"]=any("brain_aligned" in s for s in bs.get("top_setups",[]))
out["n_brain_aligned"]=len(bs.get("brain_aligned",[]))
out["keys"]=list((bs.get("top_setups") or [{}])[0].keys())[:16]
open("aws/ops/reports/1341_rr.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
