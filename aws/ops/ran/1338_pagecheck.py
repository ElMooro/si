import json, urllib.request
out={}
try:
    req=urllib.request.Request("https://justhodl.ai/brain.html?t=88",headers={"User-Agent":"Mozilla/5.0"})
    p=urllib.request.urlopen(req,timeout=20).read().decode("utf-8","replace")
    out["status"]="ok"; out["bytes"]=len(p)
    out["has_brain_title"]="The </span>" in p or "Brain" in p
    out["has_composer"]="note-input" in p
    out["has_brain_route"]="/brain" in p
except urllib.error.HTTPError as e: out["status"]=f"HTTP {e.code}"
except Exception as e: out["status"]="ERR:"+str(e)[:60]
open("aws/ops/reports/1338_pc.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
