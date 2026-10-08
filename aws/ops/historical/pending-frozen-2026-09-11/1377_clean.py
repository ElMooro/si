import json, urllib.request, time
out={}
UID="9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"
# wipe all test notes → clean slate for real notes
try:
    req=urllib.request.Request("https://api.justhodl.ai/brain?uid="+UID,
        data=json.dumps({"notes":[]}).encode(),
        headers={"Content-Type":"text/plain","Origin":"https://justhodl.ai","User-Agent":"Mozilla/5.0"},method="PUT")
    r=urllib.request.urlopen(req,timeout=20); out["wipe"]=r.read().decode()[:60]
except Exception as e: out["wipe"]=str(e)[:60]
time.sleep(1)
req=urllib.request.Request("https://api.justhodl.ai/brain?uid="+UID+"&t=%d"%int(time.time()),headers={"Origin":"https://justhodl.ai","User-Agent":"Mozilla/5.0"})
out["final"]={"notes":len(json.loads(urllib.request.urlopen(req,timeout=12).read()).get("notes",[]))}
open("aws/ops/reports/1377_c.json","w").write(json.dumps(out,indent=2,default=str));print("done")
