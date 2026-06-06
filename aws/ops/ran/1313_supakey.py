import json, urllib.request
out={}
try:
    req=urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/debug-supabase-temp",headers={"User-Agent":"Mozilla/5.0"})
    r=urllib.request.urlopen(req,timeout=25); out=json.loads(r.read().decode())
except urllib.error.HTTPError as e: out={"http":e.code,"body":e.read().decode()[:200]}
except Exception as e: out={"err":str(e)[:120]}
open("aws/ops/reports/1313_supakey.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
