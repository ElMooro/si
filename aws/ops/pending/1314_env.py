import json, urllib.request
try:
    req=urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/debug-supabase-temp",headers={"User-Agent":"Mozilla/5.0"})
    out=json.loads(urllib.request.urlopen(req,timeout=25).read().decode())
except Exception as e: out={"err":str(e)[:150]}
open("aws/ops/reports/1314_env.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
