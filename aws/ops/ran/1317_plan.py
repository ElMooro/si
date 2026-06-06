import json, urllib.request
uid="9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"
try:
    req=urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/debug-plan-temp?uid="+uid,headers={"User-Agent":"Mozilla/5.0"})
    out=json.loads(urllib.request.urlopen(req,timeout=25).read().decode())
except Exception as e: out={"err":str(e)[:150]}
open("aws/ops/reports/1317_plan.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
