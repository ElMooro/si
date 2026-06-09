"""Read the actual CONTENT of backtest + best-setups + master-ranker to see if
signals have proven edge and what's being ranked. From AWS."""
import json, boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
s3=boto3.client("s3",region_name="us-east-1",config=cfg)
B="justhodl-dashboard-live"
def gj(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e: return {"err":str(e)[:60]}
out={}
# backtest — does anything have a real hit rate?
bt=gj("data/backtest-summary.json")
bs=bt.get("by_signal",{}) if isinstance(bt,dict) else {}
out["backtest"]={"snapshot_today":bt.get("snapshot_today"),"fwd_computed":bt.get("forward_returns_computed_today"),
                 "n_signals":len(bs) if isinstance(bs,dict) else 0,
                 "sample":{k:(v if not isinstance(v,dict) else {kk:v.get(kk) for kk in ['hit_rate','n','avg_return','avg_fwd_return','count'] if kk in v}) for k,v in (list(bs.items())[:8] if isinstance(bs,dict) else [])}}
# best-setups top names
b=gj("data/best-setups.json")
ts=b.get("top_setups",[])[:8] if isinstance(b,dict) else []
out["best_setups_top"]=[{"t":s.get("ticker"),"verdict":s.get("verdict"),"conv":s.get("conviction"),"sig":s.get("signal_keys")} for s in ts]
out["best_setups_stats"]=b.get("stats") if isinstance(b,dict) else None
# master-ranker top
m=gj("data/master-ranker.json")
mt=m.get("top_tickers",[])[:8] if isinstance(m,dict) else []
out["master_ranker_top"]=[{k:t.get(k) for k in ['ticker','score','rank','reason','signals'] if k in t} for t in mt] if mt else mt
# opportunities top
o=gj("data/opportunities.json")
ot=o.get("top_opportunities",[])[:6] if isinstance(o,dict) else []
out["opportunities_top"]=[{k:t.get(k) for k in ['ticker','verdict','score','implied_growth_pct'] if k in t} for t in ot]
out["factor_weights"]=o.get("factor_weights") if isinstance(o,dict) else None
open("aws/ops/reports/1451_dr.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
