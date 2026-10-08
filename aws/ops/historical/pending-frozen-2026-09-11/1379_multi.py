import json, urllib.request, time
out={}
UID="9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"
# simulate the bulk import save (what the page now sends for multi-file)
notes=[{"id":"m%d"%i,"cat":"thesis","text":"multi note %d — testing batch save"%i,"created":int(time.time()*1000)+i} for i in range(8)]
try:
    req=urllib.request.Request("https://api.justhodl.ai/brain?uid="+UID,
        data=json.dumps({"notes":notes}).encode(),
        headers={"Content-Type":"text/plain","Origin":"https://justhodl.ai","User-Agent":"Mozilla/5.0"},method="PUT")
    r=urllib.request.urlopen(req,timeout=20); out["bulk_save"]={"status":r.getcode(),"body":r.read().decode()[:80]}
except urllib.error.HTTPError as e: out["bulk_save"]={"status":e.code,"body":e.read().decode()[:100]}
time.sleep(2)
# read back — all 8 there?
g=urllib.request.urlopen(urllib.request.Request("https://api.justhodl.ai/brain?uid="+UID+"&t=%d"%int(time.time()),headers={"Origin":"https://justhodl.ai","User-Agent":"Mozilla/5.0"}),timeout=15)
d=json.loads(g.read());out["readback"]={"notes":len(d.get("notes",[])),"all_8":sum(1 for n in d.get("notes",[]) if n.get("id","").startswith("m"))}
open("aws/ops/reports/1379_m.json","w").write(json.dumps(out,indent=2,default=str));print("done")
