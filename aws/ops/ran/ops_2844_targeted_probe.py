"""ops 2844 — targeted probe: FRED (Chile/Taiwan/semi) + DBnomics KOF datasets + Taiwan providers."""
import os, json, urllib.parse, urllib.request
from datetime import datetime, timezone
import boto3
R={"ops":2844,"ts":datetime.now(timezone.utc).isoformat()}
lam=boto3.client("lambda",region_name="us-east-1")
try: FRED=lam.get_function_configuration(FunctionName="justhodl-china-liquidity").get("Environment",{}).get("Variables",{}).get("FRED_API_KEY","")
except Exception: FRED=""
def _get(url,t=40):
    req=urllib.request.Request(url,headers={"User-Agent":"jh/1.0","Accept":"application/json"})
    with urllib.request.urlopen(req,timeout=t) as r: return json.loads(r.read())
def fred(sid):
    try:
        d=_get("https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s&file_type=json&sort_order=desc&limit=14"%(sid,FRED))
        o=[x for x in d.get("observations",[]) if x.get("value") not in(".","",None)]
        if not o: return {"empty":True}
        cur=float(o[0]["value"]); ya=float(o[12]["value"]) if len(o)>12 else None
        return {"latest":cur,"date":o[0]["date"],"yoy_pct":round((cur-ya)/abs(ya)*100,1) if ya else None}
    except Exception as e: return {"err":str(e)[:70]}
# FRED candidates: Chile exports (OECD MEI), Taiwan (likely empty), US semi IP, Chile IP
for sid in ["XTEXVA01CLM664S","XTEXVA01TWM664S","XTEXVA01KRM664S","IPG3344S","IPG3344SQ","CHLPROINDMISMEI","PCOPPUSDM"]:
    R.setdefault("fred",{})[sid]=fred(sid)
# DBnomics: list KOF provider datasets
def dbn(url):
    try: return _get("https://api.db.nomics.world/v22"+url)
    except Exception as e: return {"err":str(e)[:70]}
kof=dbn("/datasets/KOF")
R["dbnomics_KOF_datasets"]=[{"code":x.get("code"),"name":(x.get("name") or "")[:55]} for x in ((kof.get("datasets") or {}).get("docs") or [])][:12] if isinstance(kof,dict) and "err" not in kof else kof
# DBnomics: find Taiwan providers/series via search on provider names
tw=dbn("/search?"+urllib.parse.urlencode({"q":"Taiwan export orders manufacturing","limit":10}))
R["dbnomics_taiwan"]=[{"id":"%s/%s"%(x.get("provider_code"),x.get("code")),"name":(x.get("name") or "")[:55]} for x in ((tw.get("results") or {}).get("docs") or [])][:8] if isinstance(tw,dict) and "err" not in tw else tw
# WSTS semiconductor provider probe
w=dbn("/datasets/WSTS")
R["dbnomics_WSTS"]=("EXISTS: "+str([x.get("code") for x in ((w.get("datasets") or {}).get("docs") or [])][:6])) if isinstance(w,dict) and "err" not in w and w.get("datasets") else "no WSTS provider"
print(json.dumps(R,indent=1,default=str)[:3500])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2844_targeted_probe.json","w"),indent=1,default=str)
print("OPS 2844 COMPLETE")
