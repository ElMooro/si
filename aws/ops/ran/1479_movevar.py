import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1",config=Config(read_timeout=90))
B="justhodl-dashboard-live"
def gj(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e: return {"err":str(e)[:40]}
out={}
# 1) MOVE — does bond-vol actually have a MOVE value, or just a placeholder?
bv=gj("data/bond-vol.json")
ch=bv.get("channels",{}) if isinstance(bv,dict) else {}
movech=None
if isinstance(ch,dict):
    for k,v in ch.items():
        if "move" in k.lower(): movech={k:v}
elif isinstance(ch,list):
    movech=[c for c in ch if "move" in str(c).lower()][:1]
out["MOVE_in_bondvol"]={"channels_type":type(ch).__name__,"move_channel":movech,"has_move":bool(movech)}
# also check eurodollar-stress signals for FRA-OIS / xccy / move
ed=gj("data/eurodollar-stress.json")
sigs=ed.get("signals",{}) if isinstance(ed,dict) else {}
sig_names=list(sigs.keys()) if isinstance(sigs,dict) else [s.get("name") for s in sigs if isinstance(s,dict)]
out["eurodollar_signals"]=sig_names
# 2) VaR — does ANY risk file have a VaR/CVaR number?
for k in ["data/risk-monitor.json","data/portfolio-risk.json","data/factor-risk.json","data/risk-sizer.json"]:
    d=gj(k)
    if isinstance(d,dict):
        hits=[kk for kk in d.keys() if re.search(r'var|cvar|expected_shortfall|tail',kk,re.I)] if (re:=__import__('re')) else []
        out["VaR_in_"+k.split('/')[-1]]=hits or "none"
open("aws/ops/reports/1479_mv.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
