import json, urllib.request, time
out={}
# fetch the LIVE brain.html and check which save code it actually contains
try:
    h=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/brain.html?t=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0","Cache-Control":"no-cache"}),timeout=20).read().decode()
    out["has_saveNote"]="saveNote" in h
    out["has_sharded_queue"]="_runQueue" in h
    out["has_uidQ"]="uidQ" in h
    out["has_deviceId"]="deviceId" in h
    out["loads_authjs"]="/auth.js" in h
    out["proxy_url"]=("workers.dev" in h)
    # extract the save function to inspect
    i=h.find("function saveNote")
    out["saveNote_snippet"]=h[i:i+200] if i>0 else "NOT FOUND"
    # does it still have the OLD broken bulk save anywhere?
    out["still_has_old_doSave"]="_doSave" in h
    out["bytes"]=len(h)
except Exception as e: out["err"]=str(e)[:100]
open("aws/ops/reports/1372_lc.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
