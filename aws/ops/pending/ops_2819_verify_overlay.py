"""ops 2819 — verify nowcast overlay live in master-ranker + best-setups outputs."""
import os, json, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
REGION="us-east-1"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=300,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":2819,"ts":datetime.now(timezone.utc).isoformat()}
def invoke_read(fn,key,tries=6):
    try: lam.invoke(FunctionName=fn,InvocationType="Event")
    except Exception as e: R.setdefault("invoke_err",{})[fn]=str(e)[:80]
    for _ in range(tries):
        time.sleep(25)
        try:
            d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=key)["Body"].read())
            ga=d.get("generated_at","")
            if ga and ga>R["ts"][:10]: return d
        except Exception: pass
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=key)["Body"].read())
    except Exception as e: return {"_err":str(e)[:80]}
# master-ranker
mr=invoke_read("justhodl-master-ranker","data/master-ranker.json")
tt=mr.get("top_tickers") or []
mr_tilts=[{"t":x.get("ticker"),"m":x.get("nowcast_regime_mult")} for x in tt if x.get("nowcast_regime_mult") not in (None,1.0)][:8]
R["master_ranker"]={"nowcast_regime":mr.get("nowcast_regime"),
    "n_top":len(tt),"n_tilted":sum(1 for x in tt if x.get("nowcast_regime_mult") not in (None,1.0)),
    "sample_tilts":mr_tilts,"has_mult_field":any("nowcast_regime_mult" in x for x in tt)}
# best-setups
bs=invoke_read("justhodl-best-setups","data/best-setups.json")
su=bs.get("setups") or bs.get("top_setups") or []
bs_tilts=[{"t":x.get("ticker"),"m":x.get("nowcast_regime_mult")} for x in su if x.get("nowcast_regime_mult") not in (None,1.0)][:8]
R["best_setups"]={"nowcast_regime":bs.get("nowcast_regime"),
    "n_setups":len(su),"n_tilted":sum(1 for x in su if x.get("nowcast_regime_mult") not in (None,1.0)),
    "sample_tilts":bs_tilts,"has_mult_field":any("nowcast_regime_mult" in x for x in su)}
ok=(R["master_ranker"]["nowcast_regime"] or {}).get("regime") and (R["best_setups"]["nowcast_regime"] or {}).get("regime")
R["status"]="NOWCAST OVERLAY LIVE (both engines)" if ok else "CHECK"
print(json.dumps(R,indent=1,default=str))
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/2819_verify_overlay.json","w"),indent=1,default=str)
print("OPS 2819 COMPLETE")
