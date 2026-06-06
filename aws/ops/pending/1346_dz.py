import json, urllib.request
out={}
try:
    req=urllib.request.Request("https://justhodl.ai/brain.html?t=77",headers={"User-Agent":"Mozilla/5.0"})
    h=urllib.request.urlopen(req,timeout=20).read().decode("utf-8","replace")
    out["served"]="dropzone" in h and "handleFiles" in h
    out["has_docx"]="mammoth" in h; out["has_pdf"]="pdf.min.js" in h or "pdfjsLib" in h
    out["bytes"]=len(h)
except Exception as e: out["err"]=str(e)[:80]
open("aws/ops/reports/1346_dz.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
