import json, urllib.request, time
out={}
UID="9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"
# test the NEW path: api.justhodl.ai/brain (must send Origin to pass ai-proxy origin check)
def call(base,method,body=None):
    try:
        req=urllib.request.Request(base+"/brain?uid="+UID+"&t=%d"%int(time.time()),
            data=(json.dumps(body).encode() if body else None),
            headers={"Content-Type":"text/plain","Origin":"https://justhodl.ai","User-Agent":"Mozilla/5.0"},method=method)
        r=urllib.request.urlopen(req,timeout=15); return r.getcode(), r.read().decode()[:120]
    except urllib.error.HTTPError as e: return e.code, e.read().decode()[:120]
    except Exception as e: return None, str(e)[:80]
s,b=call("https://api.justhodl.ai","GET"); out["api_GET"]={"s":s,"b":b[:60]}
s,b=call("https://api.justhodl.ai","PUT",{"note":{"id":"apitest1","cat":"rule","text":"saved via api.justhodl.ai","created":int(time.time()*1000)}}); out["api_PUT"]={"s":s,"b":b[:80]}
s,b=call("https://api.justhodl.ai","GET"); 
try: out["api_GET_after"]={"s":s,"notes":len(json.loads(b).get("notes",[])) if b.startswith("{") else "?"}
except: out["api_GET_after"]={"s":s,"b":b[:60]}
open("aws/ops/reports/1374_a.json","w").write(json.dumps(out,indent=2,default=str));print("done")
