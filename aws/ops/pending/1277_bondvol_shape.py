"""1277 — dump bond-vol.json exact shape for the page."""
import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1",config=Config(read_timeout=60))
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bond-vol.json")["Body"].read())
out={"top_keys":list(d.keys()),"regime":d.get("regime"),"composite_z":d.get("composite_z"),
     "n_channels_live":d.get("n_channels_live"),"n_channels_total":d.get("n_channels_total"),
     "generated_at":d.get("generated_at")}
chs=d.get("channels",[])
out["channel_keys"]=list(chs[0].keys()) if chs else []
out["channels"]=[{k:c.get(k) for k in ("id","name","z","realized_vol","ok","status","contributing")} for c in chs]
open("aws/ops/reports/1277_shape.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
