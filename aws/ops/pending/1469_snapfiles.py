import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1",config=Config(read_timeout=90))
r=s3.list_objects_v2(Bucket="justhodl-dashboard-live",Prefix="data/snapshots/",MaxKeys=400)
keys=[o["Key"] for o in r.get("Contents",[])]
import re
# what engines + what dates are snapshotted?
engines={}; dates=set()
for k in keys:
    m=re.match(r"data/snapshots/data_(.+?)-(\d{4}-\d{2}-\d{2})\.json",k)
    if m:
        engines[m.group(1)]=engines.get(m.group(1),0)+1
        dates.add(m.group(2))
out={"n_files":len(keys),"engines":engines,"n_dates":len(dates),"date_range":[min(dates),max(dates)] if dates else None,"latest_dates":sorted(dates)[-5:] if dates else None}
# peek one best-setups snapshot if exists
bs_snaps=[k for k in keys if 'best-setups' in k]
out["best_setups_snapshots"]=len(bs_snaps)
open("aws/ops/reports/1469_sf.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
