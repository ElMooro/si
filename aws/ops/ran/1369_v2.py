import json, urllib.request, time
out={}
UID="9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"
def call(method,body=None,timeout=20):
    try:
        req=urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/brain?uid="+UID+"&t=%d"%int(time.time()),
            data=(json.dumps(body).encode() if body else None),
            headers={"Content-Type":"text/plain","User-Agent":"Mozilla/5.0"},method=method)
        r=urllib.request.urlopen(req,timeout=timeout); return r.getcode(), r.read().decode()
    except urllib.error.HTTPError as e: return e.code, e.read().decode()[:100]
    except Exception as e: return None, str(e)[:80]
# wipe (deletes legacy now)
s,b=call("PUT",{"notes":[]},timeout=25); out["wipe"]={"s":s,"b":b[:50]}
time.sleep(2)
# GET clean — should be fast now (legacy gone)
s,b=call("GET",timeout=15)
try: out["GET_clean"]={"s":s,"notes":len(json.loads(b).get("notes",[]))}
except: out["GET_clean"]={"s":s,"b":b[:50]}
# add one real note
s,b=call("PUT",{"note":{"id":"r0","cat":"rule","text":"first real note","created":int(time.time()*1000)}})
out["add"]={"s":s,"b":b[:50]}
time.sleep(1)
s,b=call("GET",timeout=12)
try: out["GET_after"]={"s":s,"notes":len(json.loads(b).get("notes",[]))}
except: out["GET_after"]={"s":s}
open("aws/ops/reports/1369_v.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
