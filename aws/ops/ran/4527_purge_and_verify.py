import json,time,urllib.request,os
from datetime import datetime,timezone
def get(u):
    req=urllib.request.Request(u,headers={"User-Agent":"ops4527"})
    with urllib.request.urlopen(req,timeout=25) as r:
        return json.loads(r.read()),dict(r.headers)
R={"at":datetime.now(timezone.utc).isoformat()}
try:
    clear,_=get("https://justhodl.ai/__cache-clear?paths=data/provider-catalog.json,data/providers/fred.json,data/providers/polygon.json,data/providers/nyfed.json")
    R["clear"]=clear
except Exception as e:
    R["clear_err"]=str(e)[:150]
time.sleep(3)
tries=[]
for i in range(3):
    try:
        d,h=get(f"https://justhodl.ai/data/provider-catalog.json?p={int(time.time())}-{i}")
        tries.append({"providers":d.get("totals",{}).get("providers"),
                       "keys":d.get("totals",{}).get("keys"),
                       "as_of":d.get("as_of"),
                       "cf_cache_status":h.get("CF-Cache-Status"),"age":h.get("Age")})
    except Exception as e:
        tries.append({"err":str(e)[:100]})
    time.sleep(2)
R["fetches"]=tries
try:
    fd,fh=get(f"https://justhodl.ai/data/providers/fred.json?p={int(time.time())}")
    R["fred"]={"n_keys":fd.get("n_keys"),"hot_feeds":fd.get("hot_feeds"),"cf_cache_status":fh.get("CF-Cache-Status")}
except Exception as e: R["fred_err"]=str(e)[:100]
os.makedirs("aws/ops/reports",exist_ok=True)
open("aws/ops/reports/4527_purge_verify.md","w").write("# 4527 — "+json.dumps(R,default=str)+"\n")
print(json.dumps(R,default=str)[:800])
