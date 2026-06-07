import json, urllib.request, time
out={}
UID="9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"  # your real supabase id
def call(method,body=None):
    req=urllib.request.Request("https://api.justhodl.ai/brain?uid="+UID+"&t=%d"%int(time.time()),
        data=(json.dumps(body).encode() if body else None),
        headers={"Content-Type":"text/plain","Origin":"https://justhodl.ai","User-Agent":"Mozilla/5.0"},method=method)
    r=urllib.request.urlopen(req,timeout=15); return r.getcode(), r.read().decode()
s,b=call("PUT",{"note":{"id":"rt1","cat":"rule","text":"round-trip persistence test","created":int(time.time()*1000)}})
out["save"]=s
time.sleep(2)
s,b=call("GET"); d=json.loads(b)
out["load_same_uid"]={"status":s,"notes":len(d.get("notes",[])),"has_rt1":any(n.get("id")=="rt1" for n in d.get("notes",[]))}
open("aws/ops/reports/1378_rt.json","w").write(json.dumps(out,indent=2,default=str));print("done")
