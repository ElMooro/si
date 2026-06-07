import json, urllib.request, time
out={}
UID="9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"
# GET — should be fast + clean
try:
    t0=time.time()
    g=urllib.request.urlopen(urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/brain?uid="+UID+"&t=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0","Origin":"https://justhodl.ai"}),timeout=12)
    d=json.loads(g.read())
    out["GET"]={"status":g.getcode(),"acao":g.headers.get("Access-Control-Allow-Origin"),"notes":len(d.get("notes",[])),"sharded":d.get("sharded"),"secs":round(time.time()-t0,2)}
except Exception as e: out["GET"]=str(e)[:80]
open("aws/ops/reports/1370_h.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
