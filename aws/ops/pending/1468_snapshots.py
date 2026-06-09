"""What's IN the daily snapshots, for Signal Replay? From AWS."""
import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1",config=Config(read_timeout=90))
B="justhodl-dashboard-live"
out={}
# the backtest stores snapshots — find the key pattern
# common: data/signal-snapshots/*.json OR data/backtest-snapshots.json OR signals/snapshot-DATE.json
prefixes=["data/signal-snapshots/","signals/","data/snapshots/","data/backtest/","data/signal-log/"]
for p in prefixes:
    try:
        r=s3.list_objects_v2(Bucket=B,Prefix=p,MaxKeys=8)
        ks=[o["Key"] for o in r.get("Contents",[])]
        if ks: out["prefix_"+p]=ks
    except Exception as e: pass
# also check the signal-backtest + backtest-summary for embedded history/snapshots
for k in ["data/signal-backtest.json","data/backtest-summary.json"]:
    try:
        d=json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
        out[k+"_keys"]=list(d.keys())[:14] if isinstance(d,dict) else None
        if isinstance(d,dict):
            for hk in ["snapshots","history","by_date","daily","vintages","snapshots_used"]:
                if hk in d: out[k+"_"+hk]=(d[hk] if not isinstance(d[hk],list) else {"len":len(d[hk]),"sample":d[hk][:2]})
    except Exception as e: out[k]="ERR"
open("aws/ops/reports/1468_sn.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
