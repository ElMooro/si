"""1297 — verify cmdk + welcome + og-image + catalog serve."""
import json, urllib.request
out={}
def chk(path,kind="text"):
    try:
        req=urllib.request.Request("https://justhodl.ai"+path,headers={"User-Agent":"Mozilla/5.0"})
        r=urllib.request.urlopen(req,timeout=20); b=r.read()
        return {"status":r.status,"bytes":len(b),"ct":r.headers.get("Content-Type","")}
    except urllib.error.HTTPError as e: return {"status":e.code}
    except Exception as e: return {"err":str(e)[:50]}
for p in ["/cmdk.js","/site-catalog.json","/welcome.html","/og-image.png","/glossary.html","/about.html","/status.html"]:
    out[p]=chk(p)
open("aws/ops/reports/1297_ux.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
