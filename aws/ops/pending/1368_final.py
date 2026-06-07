import json, urllib.request, time
out={}
UID="9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"
# clean wipe (now also deletes legacy)
try:
    r=urllib.request.urlopen(urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/brain?uid="+UID,
        data=json.dumps({"notes":[]}).encode(),headers={"Content-Type":"text/plain","User-Agent":"Mozilla/5.0"},method="PUT"),timeout=20)
    out["wipe"]=r.read().decode()[:60]
except Exception as e: out["wipe"]=str(e)[:60]
time.sleep(1)
# fast GET
try:
    t0=time.time();g=urllib.request.urlopen(urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/brain?uid="+UID+"&t=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0"}),timeout=15)
    d=json.loads(g.read());out["GET"]={"notes":len(d.get("notes",[])),"secs":round(time.time()-t0,1)}
except Exception as e: out["GET"]=str(e)[:60]
# add 3 notes individually (sharded), confirm all persist
for i in range(3):
    n={"id":"real%d"%i,"cat":"rule","text":"real note %d"%i,"created":int(time.time()*1000)+i}
    urllib.request.urlopen(urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/brain?uid="+UID,
        data=json.dumps({"note":n}).encode(),headers={"Content-Type":"text/plain","User-Agent":"Mozilla/5.0"},method="PUT"),timeout=15)
time.sleep(1)
g=urllib.request.urlopen(urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/brain?uid="+UID+"&t=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0"}),timeout=15)
out["after_3_adds"]=len(json.loads(g.read()).get("notes",[]))
open("aws/ops/reports/1368_f.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
