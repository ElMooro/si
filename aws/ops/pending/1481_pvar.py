import json, boto3
from datetime import datetime, timezone
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1",config=Config(read_timeout=60))
B="justhodl-dashboard-live"
def gj(k):
    try:
        o=s3.get_object(Bucket=B,Key=k); d=json.loads(o["Body"].read())
        return d,round((datetime.now(timezone.utc)-o["LastModified"]).total_seconds()/3600,1)
    except Exception as e: return {"err":str(e)[:40]},None
out={}
d,age=gj("portfolio/risk.json")
if isinstance(d,dict):
    out["portfolio_risk"]={"age_h":age,"keys":list(d.keys())[:20],
        "var_fields":{k:d[k] for k in d if 'var' in k.lower() or 'cvar' in k.lower() or 'shortfall' in k.lower() or 'beta' in k.lower()}}
# does a portfolio snapshot even exist? (VaR needs positions)
snap,sage=gj("portfolio/snapshot.json")
out["snapshot"]={"age_h":sage,"keys":list(snap.keys())[:10] if isinstance(snap,dict) else snap}
open("aws/ops/reports/1481_pv.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
