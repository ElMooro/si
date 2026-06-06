import json, urllib.request
out={}
def g(p):
    try:
        req=urllib.request.Request("https://justhodl.ai"+p+"?t=9",headers={"User-Agent":"Mozilla/5.0"})
        return urllib.request.urlopen(req,timeout=20).read().decode("utf-8","replace")
    except Exception as e: return "ERR:"+str(e)[:50]
c=g("/cockpit.html"); out["cockpit_served"]="Your" in c and "WHAT CHANGED" in c if isinstance(c,str) else c
b=g("/brain.html"); out["brain_ocr_paste"]=("ensureTesseract" in b and "paste" in b) if isinstance(b,str) else b
ix=g("/index.html"); out["homepage_cockpit_tab"]="cockpit.html" in ix if isinstance(ix,str) else ix
open("aws/ops/reports/1347_c.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
