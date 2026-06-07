import json, urllib.request, time
out={}
note={"notes":[{"id":"r1","cat":"rule","text":"repro test","created":int(time.time()*1000)}],"updated_at":"2026-06-06"}
# case A: no uid, no pin (logged-OUT page after the no-pin change)
try:
    req=urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/brain",
        data=json.dumps(note).encode(),headers={"Content-Type":"text/plain","Origin":"https://justhodl.ai","User-Agent":"Mozilla/5.0 Chrome/126"},method="PUT")
    r=urllib.request.urlopen(req,timeout=15); out["no_uid_no_pin"]={"status":r.getcode(),"body":r.read().decode()[:90]}
except urllib.error.HTTPError as e: out["no_uid_no_pin"]={"status":e.code,"body":e.read().decode()[:90]}
except Exception as e: out["no_uid_no_pin"]=str(e)[:60]
open("aws/ops/reports/1361_r.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
