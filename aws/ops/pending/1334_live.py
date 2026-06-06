import json, urllib.request
out={}
def get(p):
    try:
        req=urllib.request.Request("https://justhodl.ai"+p+"?t=999",headers={"User-Agent":"Mozilla/5.0"})
        return urllib.request.urlopen(req,timeout=20).read().decode("utf-8","replace")
    except urllib.error.HTTPError as e: return f"HTTP {e.code}"
    except Exception as e: return "ERR:"+str(e)[:50]
fp=get("/funding-plumbing.html")
out["funding_plumbing_page"]={"served":"Funding" in fp and "plumbing_stress" in fp,"bytes":len(fp) if isinstance(fp,str) else fp}
rr=get("/regime-ribbon.js")
out["ribbon_has_plumbing"]="funding_plumbing" in rr if isinstance(rr,str) else rr
ix=get("/index.html")
out["homepage_has_ribbon"]="home-regime-ribbon" in ix if isinstance(ix,str) else ix
# the actual data file
d=get("/data/funding-plumbing.json")
try:
    j=json.loads(d); out["data"]={"regime":j.get("regime"),"bs":j.get("balance_sheet_direction"),"score":j.get("plumbing_stress_score")}
except: out["data"]=d[:80]
open("aws/ops/reports/1334_live.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
