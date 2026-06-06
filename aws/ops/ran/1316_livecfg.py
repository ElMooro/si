import json, urllib.request
out={}
def get(p):
    try:
        req=urllib.request.Request("https://justhodl.ai"+p+"?t=999",headers={"User-Agent":"Mozilla/5.0"})
        return urllib.request.urlopen(req,timeout=20).read().decode("utf-8","replace")
    except Exception as e: return "ERR:"+str(e)[:60]
ac=get("/auth-config.js")
out["auth_config_has_pk"]="pk_test_51RKpTA" in ac
out["auth_config_has_price"]="price_1TfKrr" in ac
pr=get("/pricing.html")
out["pricing_has_guard"]="coming soon" in pr
out["pricing_has_startCheckout"]="create-checkout" in pr
open("aws/ops/reports/1316_livecfg.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
