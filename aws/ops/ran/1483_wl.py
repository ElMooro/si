import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1",config=Config(read_timeout=60))
def gj(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception as e: return {"err":str(e)[:40]}
out={}
for k in ["data/user-watchlist.json","portfolio/snapshot.json","data/watchlist.json","data/best-setups.json"]:
    d=gj(k)
    if isinstance(d,dict):
        wl=d.get("watchlist") or d.get("tickers") or d.get("symbols")
        ts=d.get("top_setups")
        out[k]={"keys":list(d.keys())[:8],
                "watchlist_sample":(wl[:10] if isinstance(wl,list) else (list(wl)[:10] if isinstance(wl,dict) else None)),
                "top_setup_tickers":[s.get("ticker") for s in ts[:10]] if isinstance(ts,list) else None}
open("aws/ops/reports/1483_wl.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
