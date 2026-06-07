import json, urllib.request, time
out={}
UID="9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"
def call(method,body=None):
    try:
        req=urllib.request.Request("https://api.justhodl.ai/brain?uid="+UID+"&t=%d"%int(time.time()),
            data=(json.dumps(body).encode() if body else None),
            headers={"Content-Type":"text/plain","Origin":"https://justhodl.ai","User-Agent":"Mozilla/5.0"},method=method)
        r=urllib.request.urlopen(req,timeout=15); return r.getcode(), r.read().decode()
    except urllib.error.HTTPError as e: return e.code, e.read().decode()[:120]
    except Exception as e: return None, str(e)[:80]
# save a note
s,b=call("PUT",{"note":{"id":"paidcheck","cat":"rule","text":"saved after Workers Paid upgrade","created":int(time.time()*1000)}})
out["save"]={"status":s,"body":b[:90]}
time.sleep(1)
# read back
s,b=call("GET")
try: out["readback"]={"status":s,"notes":len(json.loads(b).get("notes",[])),"has_note":'paidcheck' in b}
except: out["readback"]={"status":s,"b":b[:60]}
open("aws/ops/reports/1376_p.json","w").write(json.dumps(out,indent=2,default=str));print("done")
