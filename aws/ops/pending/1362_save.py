import json, urllib.request, time
out={}
# device-id style save (no pin, uid >=20 chars) — what the page now sends
dev="dev-"+("a"*36)
note={"notes":[{"id":"dev1","cat":"rule","text":"device save test","created":int(time.time()*1000)}],"updated_at":"2026-06-06"}
try:
    req=urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/brain?uid="+dev,
        data=json.dumps(note).encode(),headers={"Content-Type":"text/plain","Origin":"https://justhodl.ai","User-Agent":"Mozilla/5.0 Chrome/126"},method="PUT")
    r=urllib.request.urlopen(req,timeout=15); out["device_save"]={"status":r.getcode(),"body":r.read().decode()[:80]}
except urllib.error.HTTPError as e: out["device_save"]={"status":e.code,"body":e.read().decode()[:80]}
# page has device-id + regime panel?
try:
    h=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/brain.html?t=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0"}),timeout=15).read().decode()
    out["page"]={"deviceId":"deviceId" in h,"regime_read":"REGIME READ" in h,"no_pin_save":"_pin:PIN" not in h}
except Exception as e: out["page"]=str(e)[:50]
open("aws/ops/reports/1362_s.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
