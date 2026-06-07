import json, urllib.request, time
out={}
UID="9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"
# bulk-replace with empty → clears all my test shards + index
for attempt in range(3):
    try:
        req=urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/brain?uid="+UID,
            data=json.dumps({"notes":[]}).encode(),
            headers={"Content-Type":"text/plain","Origin":"https://justhodl.ai","User-Agent":"Mozilla/5.0"},method="PUT")
        r=urllib.request.urlopen(req,timeout=25); out["wipe"]=r.read().decode()[:60]; break
    except urllib.error.HTTPError as e: out["wipe"]={"http":e.code,"body":e.read().decode()[:60]}; time.sleep(2)
    except Exception as e: out["wipe"]=str(e)[:60]; time.sleep(2)
time.sleep(2)
# GET should be fast + empty now
try:
    t0=time.time();g=urllib.request.urlopen(urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/brain?uid="+UID+"&t=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0"}),timeout=12)
    d=json.loads(g.read());out["GET"]={"notes":len(d.get("notes",[])),"secs":round(time.time()-t0,2)}
except Exception as e: out["GET"]=str(e)[:60]
open("aws/ops/reports/1380_w.json","w").write(json.dumps(out,indent=2,default=str));print("done")
