"""1275 — diagnose bond-vol into a report."""
import json, boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={}
try:
    c=lam.get_function_configuration(FunctionName="justhodl-bond-vol")
    env=c.get("Environment",{}).get("Variables",{})
    out["fred_key_set"]=bool(env.get("FRED_KEY")); out["fred_key_len"]=len(env.get("FRED_KEY",""))
    out["timeout"]=c.get("Timeout"); out["last_modified"]=c.get("LastModified")
    out["env_keys"]=list(env.keys())
except Exception as e: out["cfg_err"]=str(e)[:150]
try:
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bond-vol.json")["Body"].read())
    out["output"]={"generated_at":d.get("generated_at"),"regime":d.get("regime"),
        "composite_z":d.get("composite_z"),"channels_live":d.get("channels_live"),
        "channels":[{"id":ch.get("id"),"status":ch.get("status"),"z":ch.get("z"),"err":ch.get("error")} for ch in d.get("channels",[])]}
except Exception as e: out["output_err"]=str(e)[:150]
open("aws/ops/reports/1275_bondvol.json","w").write(json.dumps(out,indent=2,default=str))
print("done")
