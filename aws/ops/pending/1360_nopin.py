import json, urllib.request, time
out={}
# simulate a logged-in user (real UUID-length uid) saving WITHOUT any pin
uid="9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"  # khalid's real supabase id
note={"notes":[{"id":"nopin1","cat":"rule","text":"no-pin save test for logged-in user","created":int(time.time()*1000),"pinned":False}],"updated_at":"2026-06-06"}
try:
    req=urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/brain?uid="+uid,
        data=json.dumps(note).encode(),
        headers={"Content-Type":"text/plain","Origin":"https://justhodl.ai","User-Agent":"Mozilla/5.0 Chrome/126"},method="PUT")
    r=urllib.request.urlopen(req,timeout=15); out["authed_save_no_pin"]={"status":r.getcode(),"body":r.read().decode()[:80]}
except urllib.error.HTTPError as e: out["authed_save_no_pin"]={"status":e.code,"body":e.read().decode()[:80]}
# read it back for that user
try:
    g=json.loads(urllib.request.urlopen(urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/brain?t=%d&uid=%s"%(int(time.time()),uid),headers={"User-Agent":"Mozilla/5.0"}),timeout=15).read().decode())
    out["readback"]={"notes":len(g.get("notes",[])),"visible":any(n.get("id")=="nopin1" for n in g.get("notes",[]))}
except Exception as e: out["readback"]=str(e)[:60]
open("aws/ops/reports/1360_np.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
