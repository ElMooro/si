"""1305 — confirm new nav links present on live pages."""
import json, urllib.request
out={}
def chk(path):
    try:
        req=urllib.request.Request("https://justhodl.ai"+path,headers={"User-Agent":"Mozilla/5.0"})
        h=urllib.request.urlopen(req,timeout=20).read().decode("utf-8","replace")
        return {"deep_value":"deep-value-overlap.html" in h,"backlog":"backlog.html" in h,
                "my_portfolio":"my-portfolio.html" in h,"ask":"ask.html" in h}
    except Exception as e: return {"err":str(e)[:50]}
for p in ["/dislocations.html","/capital-flow.html","/opportunities.html","/backlog.html","/deep-value-overlap.html","/glossary.html"]:
    out[p]=chk(p)
open("aws/ops/reports/1305_nav.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
