import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1",config=Config(read_timeout=60))
bv=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bond-vol.json")["Body"].read())
ch=bv.get("channels",[])
out={"n_channels":len(ch),"channel_names":[c.get("name") or c.get("key") or c.get("id") for c in ch][:20] if isinstance(ch,list) else ch,
     "one_channel_shape":ch[0] if isinstance(ch,list) and ch else None}
open("aws/ops/reports/1480_bv.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
