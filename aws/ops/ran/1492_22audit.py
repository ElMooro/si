"""Audit B: which of the 22 ECB-derived dump-predictor indicators already exist?
Read the live engines that would contain them. From AWS. NO building."""
import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1",config=Config(read_timeout=90))
B="justhodl-dashboard-live"
def gj(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e: return None
def keys_of(d):
    out=set()
    def walk(o,pfx=""):
        if isinstance(o,dict):
            for k,v in o.items():
                out.add(k.lower()); 
                if isinstance(v,(dict,list)) and pfx.count(".")<2: walk(v,pfx+k+".")
        elif isinstance(o,list) and o and isinstance(o[0],dict):
            for k in o[0]: out.add(k.lower())
    walk(d); return out
# pull the engines that would hold these
engines={k:gj(f"data/{k}.json") for k in ["eurodollar-stress","euro-fragmentation","systemic-stress","ecb-detail","cb-injection","crisis-composite","global-stress","liquidity","global-liquidity"]}
allkeys={k:keys_of(v) for k,v in engines.items() if v}
# the 22 indicators → keyword signatures to detect
checks={
 "#2 USD Funding Stress Composite":["usd","dollar","swap","a030000","funding"],
 "#5 TARGET2 Imbalance":["target","target2","imbalance","intra"],
 "#8 CISS Acceleration":["accel","ciss_30","delta_ciss","ciss_chg","acceleration"],
 "#9 CISS Subindex Divergence":["divergence","subindex","dispersion","sub_index"],
 "#10 SovCISS Fragmentation Spread":["fragmentation","sovciss","it_de","spread","periphery"],
 "#12 BLS Credit Standards":["bls","credit_standard","tightening","lending_survey"],
 "#14 Bank Pass-Through Premium":["pass_through","passthrough","nfc","lending_premium"],
 "#16 Eurodollar Stress Index ESI":["esi","eurodollar_stress","composite_score"],
 "#17 European Liquidity Pulse":["liquidity_pulse","eu_pulse","net_liq","european_pulse"],
 "#18 EU/US Liquidity Divergence":["divergence_gauge","eu_us","liquidity_divergence","cross_cb"],
 "#21 Global CB Composite Stress":["global_cb","global_stress","cross_reference","multi_cb"],
}
results={}
for ind,sigs in checks.items():
    hits=[]
    for eng,ks in allkeys.items():
        if any(any(s in k for k in ks) for s in sigs): hits.append(eng)
    results[ind]={"exists_in":hits,"status":"EXISTS" if hits else "GAP"}
out={"engines_checked":[k for k,v in engines.items() if v],"indicator_audit":results,
     "gaps":[i for i,r in results.items() if r["status"]=="GAP"],
     "exists":[i for i,r in results.items() if r["status"]=="EXISTS"]}
open("aws/ops/reports/1492_22.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
