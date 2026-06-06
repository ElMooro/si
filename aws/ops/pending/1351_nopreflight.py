import json, urllib.request, time
out={}
def put(url,payload):
    try:
        req=urllib.request.Request(url,data=json.dumps(payload).encode(),
            headers={"Content-Type":"text/plain","Origin":"https://justhodl.ai","User-Agent":"Mozilla/5.0 Chrome/126"},method="PUT")
        r=urllib.request.urlopen(req,timeout=15); return r.status, r.read().decode()[:120]
    except urllib.error.HTTPError as e: return e.code, e.read().decode()[:150]
    except Exception as e: return None, str(e)[:100]
# owner brain save with pin in body
note={"notes":[{"id":"nf1","cat":"rule","text":"no-preflight save test","created":int(time.time()*1000),"pinned":False}],"updated_at":"2026-06-06","_pin":"savetest1234"}
s,b=put("https://justhodl-data-proxy.raafouis.workers.dev/brain",note)
out["brain_save"]={"status":s,"body":b}
# read back
try:
    g=json.loads(urllib.request.urlopen(urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/brain?t=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0"}),timeout=15).read().decode())
    out["readback_notes"]=len(g.get("notes",[]))
    out["note_visible"]=any(n.get("id")=="nf1" for n in g.get("notes",[]))
    out["pin_not_stored"]=not any("_pin" in n for n in g.get("notes",[])) and "_pin" not in g
except Exception as e: out["readback"]=str(e)[:80]
open("aws/ops/reports/1351_np.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
