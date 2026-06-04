import json, time, boto3
from datetime import datetime, timezone
from botocore.config import Config
cfg=Config(read_timeout=200,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={}
try:
    r=lam.invoke(FunctionName="justhodl-best-setups",InvocationType="RequestResponse",Payload=b"{}"); time.sleep(2)
    bs=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/best-setups.json")["Body"].read())
    out["bond_vol_regime"]=bs.get("bond_vol_regime")
    out["top3"]=[{"t":s["ticker"],"conv":s["conviction"],"adj":s.get("bond_vol_adjusted")} for s in bs.get("top_setups",[])[:3]]
except Exception as e: out["err"]=str(e)[:200]
open("aws/ops/reports/1281_board.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
