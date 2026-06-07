import json, urllib.request, time
out={}
UID="9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"
# bulk-replace with EMPTY notes → clears the index + any shards, gives a clean fast start.
# (the 9.5MB legacy blob was test junk, not real notes)
try:
    r=urllib.request.urlopen(urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/brain?uid="+UID,
        data=json.dumps({"notes":[]}).encode(),headers={"Content-Type":"text/plain","Origin":"https://justhodl.ai","User-Agent":"Mozilla/5.0"},method="PUT"),timeout=30)
    out["wipe"]={"status":r.getcode(),"body":r.read().decode()[:80]}
except urllib.error.HTTPError as e: out["wipe"]={"status":e.code,"body":e.read().decode()[:100]}
except Exception as e: out["wipe"]=str(e)[:100]
time.sleep(1)
# now GET should be fast + empty
try:
    g=urllib.request.urlopen(urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/brain?uid="+UID+"&t=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0"}),timeout=15)
    d=json.loads(g.read()); out["GET_clean"]={"notes":len(d.get("notes",[])),"sharded":d.get("sharded")}
except Exception as e: out["GET_clean"]=str(e)[:80]
open("aws/ops/reports/1367_w.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
