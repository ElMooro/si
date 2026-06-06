import json, urllib.request
out={}
def post(path, body):
    try:
        req=urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev"+path,
            data=json.dumps(body).encode(),
            headers={"Content-Type":"application/json","Origin":"https://justhodl.ai","User-Agent":"Mozilla/5.0"},method="POST")
        r=urllib.request.urlopen(req,timeout=30); return r.status, r.read().decode()[:500]
    except urllib.error.HTTPError as e: return e.code, e.read().decode()[:500]
    except Exception as e: return None, str(e)[:120]
s,b = post("/create-checkout", {"priceId":"price_1TfKrrQ0UPXfFGwHVUfVhyaA","userId":"00000000-0000-0000-0000-000000000001","email":"test@example.com","returnUrl":"https://justhodl.ai"})
out["create_checkout"]={"status":s,"has_stripe_url":"checkout.stripe.com" in (b or ""),"body":b[:300]}
open("aws/ops/reports/1311_checkout.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
