import json, time, boto3
from datetime import datetime, timezone
from botocore.config import Config
cfg=Config(read_timeout=200,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={"started":datetime.now(timezone.utc).isoformat()}
time.sleep(15)
try:
    r=lam.invoke(FunctionName="justhodl-bond-vol",InvocationType="RequestResponse",Payload=b"{}")
    out["invoke"]=r.get("Payload").read().decode()[:250]
except Exception as e: out["invoke"]=str(e)[:200]
time.sleep(2)
try:
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bond-vol.json")["Body"].read())
    out["regime"]=d.get("regime"); out["z"]=d.get("composite_z_score"); out["pct"]=d.get("composite_percentile")
    out["live"]=str(d.get("n_channels_live"))+"/"+str(d.get("n_channels_total"))
    out["trend"]=d.get("trend"); out["term_structure"]=d.get("term_structure"); out["risk_posture"]=d.get("risk_posture")
    out["channels"]=[{"id":c.get("id"),"ok":c.get("ok"),"z":c.get("z_score"),"pct":c.get("percentile_1y")} for c in d.get("channels",[])]
except Exception as e: out["err"]=str(e)[:200]
open("aws/ops/reports/1280_bondvol_v2.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
