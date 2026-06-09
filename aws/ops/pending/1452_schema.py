import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1",config=Config(read_timeout=60))
def gj(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception as e: return {"err":str(e)[:60]}
b=gj("data/best-setups.json")
out={}
if isinstance(b,dict) and b.get("top_setups"):
    s=b["top_setups"][0]  # full schema of one setup
    out["one_setup_full"]=s
    out["all_fields"]=sorted(s.keys())
# backtest by_signal full one entry (to see if ANY have hit rates)
bt=gj("data/backtest-summary.json")
bs=bt.get("by_signal",{}) if isinstance(bt,dict) else {}
out["backtest_signal_full"]={k:v for k,v in list(bs.items())[:2]} if isinstance(bs,dict) else bs
open("aws/ops/reports/1452_sc.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
