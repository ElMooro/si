import json, urllib.request, time
out={}
try:
    req=urllib.request.Request("https://justhodl.ai/brain.html?t=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0","Cache-Control":"no-cache"})
    h=urllib.request.urlopen(req,timeout=20).read().decode("utf-8","replace")
    out["dedup"]="findDuplicates" in h and "Remove " in h
    out["junk_skip"]="JUNK=" in h
    out["csv"]="ext==='csv'" in h
    out["nocache_meta"]="Cache-Control" in h
except Exception as e: out["err"]=str(e)[:60]
open("aws/ops/reports/1357_d.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
