import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1",config=Config(read_timeout=60))
def gj(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception as e: return {"err":str(e)[:40]}
out={}
r=gj("portfolio/risk.json")
out["risk_message"]=r.get("message"); out["risk_status"]=r.get("status")
snap=gj("portfolio/snapshot.json")
pos=snap.get("positions",[]) if isinstance(snap,dict) else []
out["n_positions"]=len(pos) if isinstance(pos,list) else pos
out["pos_sample"]=pos[:2] if isinstance(pos,list) else None
out["portfolio_summary"]=snap.get("portfolio_summary") if isinstance(snap,dict) else None
open("aws/ops/reports/1482_w.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
