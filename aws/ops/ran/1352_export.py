import json, urllib.request, time
out={}
try:
    req=urllib.request.Request("https://justhodl.ai/brain.html?t=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0"})
    h=urllib.request.urlopen(req,timeout=20).read().decode("utf-8","replace")
    out["export_btn"]="export-btn" in h and "exportNotes" in h
    out["import_btn"]="import-btn" in h and "importNotes" in h
    out["save_text_plain"]="text/plain" in h
    out["bytes"]=len(h)
except Exception as e: out["err"]=str(e)[:80]
open("aws/ops/reports/1352_e.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
