import json, urllib.request, time
out={}
UID="9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"  # your exact uid from the console
# 1) GET your brain — does it even read?
try:
    g=urllib.request.urlopen(urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/brain?uid="+UID+"&t=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0","Origin":"https://justhodl.ai"}),timeout=15)
    out["GET"]={"status":g.getcode(),"acao":g.headers.get("Access-Control-Allow-Origin"),"len":len(g.read())}
except urllib.error.HTTPError as e: out["GET"]={"status":e.code,"acao":e.headers.get("Access-Control-Allow-Origin"),"body":e.read().decode()[:100]}
except Exception as e: out["GET"]={"err":str(e)[:100]}
# 2) PUT a small note to your uid — capture status + whether ACAO header present
note={"notes":[{"id":"x1","cat":"rule","text":"test","created":int(time.time()*1000)}],"updated_at":"2026-06-06"}
try:
    req=urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/brain?uid="+UID,
        data=json.dumps(note).encode(),headers={"Content-Type":"text/plain","Origin":"https://justhodl.ai","User-Agent":"Mozilla/5.0"},method="PUT")
    r=urllib.request.urlopen(req,timeout=20)
    out["PUT"]={"status":r.getcode(),"acao":r.headers.get("Access-Control-Allow-Origin"),"body":r.read().decode()[:100]}
except urllib.error.HTTPError as e: out["PUT"]={"status":e.code,"acao":e.headers.get("Access-Control-Allow-Origin"),"body":e.read().decode()[:150]}
except Exception as e: out["PUT"]={"err":str(e)[:120]}
open("aws/ops/reports/1365_u.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
