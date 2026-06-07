import json, urllib.request, time
out={}
UID="9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"
# GET first (triggers migration of the 9.5MB blob into shards)
try:
    g=urllib.request.urlopen(urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/brain?uid="+UID+"&t=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0"}),timeout=40)
    d=json.loads(g.read()); out["GET_after_migrate"]={"notes":len(d.get("notes",[])),"sharded":d.get("sharded")}
except Exception as e: out["GET_after_migrate"]=str(e)[:100]
# per-note upsert (the new tiny save)
note={"id":"shardtest1","cat":"rule","text":"sharded per-note save works","created":int(time.time()*1000),"pinned":False}
try:
    r=urllib.request.urlopen(urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/brain?uid="+UID,
        data=json.dumps({"note":note}).encode(),headers={"Content-Type":"text/plain","Origin":"https://justhodl.ai","User-Agent":"Mozilla/5.0"},method="PUT"),timeout=20)
    out["upsert"]={"status":r.getcode(),"body":r.read().decode()[:80]}
except urllib.error.HTTPError as e: out["upsert"]={"status":e.code,"body":e.read().decode()[:100]}
open("aws/ops/reports/1366_s.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
