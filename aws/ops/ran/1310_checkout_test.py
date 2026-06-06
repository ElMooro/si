"""1310 — verify /create-checkout talks to Stripe (real internet via GH runner)."""
import json, urllib.request
out={}
def post(path, body):
    try:
        req=urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev"+path,
            data=json.dumps(body).encode(),
            headers={"Content-Type":"application/json","Origin":"https://justhodl.ai","User-Agent":"Mozilla/5.0"},method="POST")
        r=urllib.request.urlopen(req,timeout=30)
        return r.status, r.read().decode()[:400]
    except urllib.error.HTTPError as e: return e.code, e.read().decode()[:400]
    except Exception as e: return None, str(e)[:120]
# fake user id (real Supabase uuid format) + the live price
s,b = post("/create-checkout", {"priceId":"price_1TfKrrQ0UPXfFGwHVUfVhyaA","userId":"00000000-0000-0000-0000-000000000001","email":"test@example.com","returnUrl":"https://justhodl.ai"})
out["create_checkout"]={"status":s,"body":b}
open("aws/ops/reports/1310_checkout.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
