import json, urllib.request
out={}
def get(p):
    try:
        req=urllib.request.Request("https://justhodl.ai"+p,headers={"User-Agent":"Mozilla/5.0"})
        return urllib.request.urlopen(req,timeout=20).read().decode("utf-8","replace")
    except Exception as e: return "ERR:"+str(e)[:50]
j=get("/wl-lens.js")
out["wl_lens_js"]={"served":"WLLens" in j,"bytes":len(j)}
cp=get("/chart-pro.html")
out["chart_pro_badges"]={"has_getSymbolSignals_badges":"wl-badge" in cp,"loads_backlog":"data/backlog.json" in cp,"unicode_clean": "\\ud83" not in cp}
for p in ["/deep-value-overlap.html","/backlog.html","/dislocations.html"]:
    h=get(p); out[p]={"has_lens":"wl-lens.js" in h}
open("aws/ops/reports/1307_wl.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
