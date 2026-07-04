"""ops 2812 — probe candidate FRED IDs for the failing weekly energy series."""
import os, json, urllib.request
from datetime import datetime, timezone
import boto3
lam=boto3.client("lambda",region_name="us-east-1")
R={"ops":2812,"ts":datetime.now(timezone.utc).isoformat()}
# working FRED key
FRED=""
for eng in ("justhodl-china-liquidity","justhodl-macro-leads"):
    env=lam.get_function_configuration(FunctionName=eng).get("Environment",{}).get("Variables",{})
    FRED=env.get("FRED_API_KEY") or env.get("FRED_KEY") or FRED
    if FRED: break
def probe(sid):
    try:
        u="https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s&file_type=json&sort_order=desc&limit=1"%(sid,FRED)
        obs=json.loads(urllib.request.urlopen(u,timeout=15).read()).get("observations",[])
        if obs and obs[0].get("value") not in (".","",None): return obs[0]["value"]+" @ "+obs[0]["date"]
        return "empty"
    except Exception as e: return "ERR "+str(e)[:40]
cands={
 "crude_inv_exSPR":["WCESTUS1","WCRSTUS1","WTTSTUS1","W_EPC0_SAX_NUS_MBBL"],
 "crude_production":["WCRFPUS2","MCRFPUS2","WPRD_EPC0_FPF_NUS_MBBLD"],
 "SPR_stocks":["WCSSTUS1","WCESTUS1"],
 "gasoline_stocks":["WGTSTUS1","WGFSTUS1"],
 "distillate_stocks":["WDISTUS1","WDIESTUS1"],
 "crude_imports":["WCEIMUS2","WCRIMUS2"],
 "crude_exports":["WCREXUS2","WCRExUS2"],
 "natgas_storage":["NGSTOR","WNGSTUS1","NW2_EPG0_SWO_R48_BCF"],
}
R["probe"]={concept:{c:probe(c) for c in ids} for concept,ids in cands.items()}
print(json.dumps(R["probe"],indent=1))
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/2812_fred_probe.json","w"),indent=1,default=str)
print("OPS 2812 COMPLETE")
