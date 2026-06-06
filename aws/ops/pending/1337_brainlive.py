import json, urllib.request
out={}
def get(u):
    try:
        req=urllib.request.Request(u+"?t=999",headers={"User-Agent":"Mozilla/5.0"})
        return urllib.request.urlopen(req,timeout=20).read().decode("utf-8","replace")
    except urllib.error.HTTPError as e: return f"HTTP {e.code}"
    except Exception as e: return "ERR:"+str(e)[:50]
# page live
p=get("https://justhodl.ai/brain.html")
out["page_served"]="The Brain" in p if isinstance(p,str) else p
# worker GET /brain (read path — public)
b=get("https://justhodl-data-proxy.raafouis.workers.dev/brain")
try:
    j=json.loads(b); out["worker_get"]={"pin_set":j.get("pin_set"),"n_notes":len(j.get("notes",[]))}
except: out["worker_get"]=b[:100]
# homepage link
ix=get("https://justhodl.ai/index.html")
out["homepage_link"]="brain.html" in ix if isinstance(ix,str) else ix
open("aws/ops/reports/1337_bl.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
