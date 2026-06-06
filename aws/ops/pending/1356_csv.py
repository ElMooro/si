import json, urllib.request, time
out={}
try:
    req=urllib.request.Request("https://justhodl.ai/brain.html?t=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0"})
    h=urllib.request.urlopen(req,timeout=20).read().decode("utf-8","replace")
    out["csv_supported"]="ext==='csv'" in h
    out["lnk_friendly"]="shortcut, not a document" in h
    out["accept_has_csv"]=".csv" in h
except Exception as e: out["err"]=str(e)[:60]
open("aws/ops/reports/1356_csv.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
