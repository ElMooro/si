import json, urllib.request, time
out={}
# save a large note (~200KB of text) to confirm the cap is lifted
big="MACRO NOTE. "*18000  # ~216KB
note={"notes":[{"id":"big1","cat":"thesis","text":big[:200000],"created":int(time.time()*1000),"pinned":False}],"updated_at":"2026-06-06","_pin":"savetest1234"}
try:
    req=urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/brain",
        data=json.dumps(note).encode(),
        headers={"Content-Type":"text/plain","Origin":"https://justhodl.ai","User-Agent":"Mozilla/5.0 Chrome/126"},method="PUT")
    r=urllib.request.urlopen(req,timeout=20); out["big_save"]={"status":r.getcode(),"body":r.read().decode()[:80],"note_kb":round(len(big[:200000])/1024)}
except urllib.error.HTTPError as e: out["big_save"]={"status":e.code,"body":e.read().decode()[:80]}
except Exception as e: out["big_save"]=str(e)[:80]
# read back size
try:
    g=json.loads(urllib.request.urlopen(urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/brain?t=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0"}),timeout=15).read().decode())
    n=next((x for x in g.get("notes",[]) if x.get("id")=="big1"),None)
    out["readback_note_kb"]=round(len(n["text"])/1024) if n else "missing"
except Exception as e: out["readback"]=str(e)[:60]
open("aws/ops/reports/1355_bs.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
