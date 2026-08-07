import json,time,urllib.request
from datetime import datetime,timezone
import os
def live(u):
    req=urllib.request.Request(u,headers={"User-Agent":"ops4525"})
    with urllib.request.urlopen(req,timeout=20) as r:
        return json.loads(r.read()),dict(r.headers)
R={"checked_at":datetime.now(timezone.utc).isoformat()}
for i in range(3):
    d,h=live(f"https://justhodl.ai/data/provider-catalog.json?probe={i}-{int(time.time())}")
    R[f"probe_{i}"]={"providers":d.get("totals",{}).get("providers"),
                     "keys":d.get("totals",{}).get("keys"),
                     "as_of":d.get("as_of"),
                     "cf_cache_status":h.get("CF-Cache-Status"),
                     "age":h.get("Age")}
    time.sleep(2)
fd,fh=live(f"https://justhodl.ai/data/providers/fred.json?probe={int(time.time())}")
R["fred"]={"n_keys":fd.get("n_keys"),"hot_feeds":fd.get("hot_feeds"),
           "cf_cache_status":fh.get("CF-Cache-Status")}
os.makedirs("aws/ops/reports",exist_ok=True)
open("aws/ops/reports/4525_proof.md","w").write("# 4525 proof — "+json.dumps(R,default=str)+"\n")
print(json.dumps(R,default=str))
