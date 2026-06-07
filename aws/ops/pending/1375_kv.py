import json, urllib.request, time
out={}
UID="9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"
# try one small save via api.justhodl.ai
try:
    req=urllib.request.Request("https://api.justhodl.ai/brain?uid="+UID,
        data=json.dumps({"note":{"id":"kvcheck","cat":"rule","text":"kv reset check","created":int(time.time()*1000)}}).encode(),
        headers={"Content-Type":"text/plain","Origin":"https://justhodl.ai","User-Agent":"Mozilla/5.0"},method="PUT")
    r=urllib.request.urlopen(req,timeout=15); out["save"]={"status":r.getcode(),"body":r.read().decode()[:100]}
except urllib.error.HTTPError as e: out["save"]={"status":e.code,"body":e.read().decode()[:120]}
except Exception as e: out["save"]=str(e)[:80]
open("aws/ops/reports/1375_kv.json","w").write(json.dumps(out,indent=2,default=str));print("done")
