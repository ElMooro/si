import json, boto3, time
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={}
# trigger signal-logger + backtest now to seed a fresh snapshot
for fn in ["justhodl-signal-logger","justhodl-backtest-engine","justhodl-backtest-harness","justhodl-outcome-checker"]:
    try:
        r=lam.invoke(FunctionName=fn,InvocationType="RequestResponse",Payload=b"{}")
        out[fn]=r["Payload"].read().decode()[:80]
    except Exception as e: out[fn]="ERR:"+str(e)[:70]
    time.sleep(2)
time.sleep(5)
# read what backtest-summary + any signal-backtest now shows
for k in ["data/backtest-summary.json","data/signal-backtest.json","data/signal-log.json","data/outcomes.json"]:
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
        if isinstance(d,dict):
            out["FILE_"+k]={"keys":list(d.keys())[:10],"generated_at":str(d.get("generated_at",""))[:19]}
            bs=d.get("by_signal") or d.get("signals") or {}
            if isinstance(bs,dict) and bs:
                out["FILE_"+k]["signal_sample"]={kk:(vv if not isinstance(vv,dict) else {x:vv.get(x) for x in ['hit_rate','n','avg_fwd_return','avg_return_30d','count'] if x in vv}) for kk,vv in list(bs.items())[:6]}
    except Exception as e: out["FILE_"+k]="MISSING/"+str(e)[:40]
open("aws/ops/reports/1456_seed.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
