"""1276 — deploy-wait + invoke bond-vol + verify all 5 channels live."""
import json, time, boto3
from datetime import datetime, timezone
from botocore.config import Config
cfg=Config(read_timeout=200,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={"started":datetime.now(timezone.utc).isoformat()}
time.sleep(20)
try:
    r=lam.invoke(FunctionName="justhodl-bond-vol",InvocationType="RequestResponse",Payload=b"{}")
    out["invoke"]=r.get("Payload").read().decode()[:200]
except Exception as e: out["invoke"]=str(e)[:200]
time.sleep(2)
try:
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bond-vol.json")["Body"].read())
    out["regime"]=d.get("regime"); out["composite_z"]=d.get("composite_z"); out["channels_live"]=d.get("channels_live")
    out["channels"]=[{"id":ch.get("id"),"status":ch.get("status"),"z":ch.get("z"),"rv":ch.get("realized_vol")} for ch in d.get("channels",[])]
except Exception as e: out["err"]=str(e)[:150]
open("aws/ops/reports/1276_bondvol.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
