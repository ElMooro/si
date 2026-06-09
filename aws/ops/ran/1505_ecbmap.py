"""Map ALL ECB data available, so the hub page surfaces everything."""
import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1",config=Config(read_timeout=60))
def gj(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception as e: return {"err":str(e)[:40]}
out={}
# ecb-detail: current tiles
ed=gj("data/ecb-detail.json")
out["ecb_detail"]={"keys":list(ed.keys()) if isinstance(ed,dict) else None,
                   "liquidity":ed.get("liquidity"),"balance_sheet":ed.get("balance_sheet"),
                   "policy_rates":ed.get("policy_rates"),"eurodollar_read":ed.get("eurodollar_read")}
# ecb-derived: the 5 dump signals
der=gj("data/ecb-derived.json")
out["ecb_derived_indicators"]=list((der.get("indicators") or {}).keys()) if isinstance(der,dict) else None
# ecb-hist manifest: history series
m=gj("data/ecb-hist/_manifest.json")
out["history_series"]=[(s["id"],s["label"][:40]) for s in m.get("series",[])] if isinstance(m,dict) else None
# euro-fragmentation + eurodollar-stress + systemic-stress (ECB-sourced)
for k in ["data/euro-fragmentation.json","data/eurodollar-stress.json","data/systemic-stress.json"]:
    d=gj(k)
    out[k.split('/')[-1]]={"keys":list(d.keys())[:12] if isinstance(d,dict) else None,
                            "headline":d.get("headline") if isinstance(d,dict) else None}
open("aws/ops/reports/1505_map.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
