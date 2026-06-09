"""Map per-ticker data across engines for a sample ticker (NVDA/MU) so the
dossier joins them correctly. From AWS."""
import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1",config=Config(read_timeout=90))
def gj(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception as e: return {"err":str(e)[:50]}
T="NVDA"
out={}
# fundamentals — find NVDA entry shape
fu=gj("data/fundamentals.json")
comps=fu.get("companies") if isinstance(fu,dict) else None
if isinstance(comps,dict): out["fundamentals_NVDA"]={k:comps.get(T,{}).get(k) for k in list(comps.get(T,{}).keys())[:20]} if T in comps else "no NVDA"
elif isinstance(comps,list):
    e=next((c for c in comps if c.get("ticker")==T or c.get("symbol")==T),None)
    out["fundamentals_NVDA"]={"fields":sorted(e.keys())[:25]} if e else "no NVDA in list"
# backlog
bk=gj("data/backlog.json"); bt=bk.get("by_ticker",{}) if isinstance(bk,dict) else {}
out["backlog_NVDA"]=bt.get(T) if isinstance(bt,dict) else (next((x for x in bt if x.get("ticker")==T),None) if isinstance(bt,list) else None)
# deep-value-overlap board
dv=gj("data/deep-value-overlap.json"); board=dv.get("board",[]) if isinstance(dv,dict) else []
out["deepvalue_NVDA"]=next((x for x in board if x.get("ticker")==T),"not in board") if isinstance(board,list) else None
# estimate revisions
er=gj("data/estimate-revisions-latest.json")
out["est_rev_keys"]=list(er.keys())[:8] if isinstance(er,dict) else None
# asymmetric-scorer
asy=gj("data/asymmetric-scorer.json"); ats=asy.get("top_setups",[]) if isinstance(asy,dict) else []
out["asym_NVDA"]=next((x for x in ats if x.get("ticker")==T),"not in top") if isinstance(ats,list) else None
# short interest
si=gj("data/short-interest.json")
out["short_int_shape"]=list(si.keys())[:6] if isinstance(si,dict) else (len(si) if isinstance(si,list) else None)
open("aws/ops/reports/1463_tf.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
