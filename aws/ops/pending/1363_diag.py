import json, urllib.request, time
out={}
dev="dev-"+("b"*36)
# Test escalating payload sizes to find where it breaks (KV limit? worker limit?)
for kb in [1, 100, 1000, 5000, 12000]:
    big="X"*(kb*1024)
    note={"notes":[{"id":"d%d"%kb,"cat":"rule","text":big,"created":int(time.time()*1000)}],"updated_at":"2026-06-06"}
    try:
        req=urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/brain?uid="+dev,
            data=json.dumps(note).encode(),headers={"Content-Type":"text/plain","Origin":"https://justhodl.ai","User-Agent":"Mozilla/5.0 Chrome/126"},method="PUT")
        r=urllib.request.urlopen(req,timeout=25); out["%dKB"%kb]={"status":r.getcode(),"body":r.read().decode()[:60]}
    except urllib.error.HTTPError as e: out["%dKB"%kb]={"status":e.code,"body":e.read().decode()[:120]}
    except Exception as e: out["%dKB"%kb]={"err":str(e)[:100]}
open("aws/ops/reports/1363_diag.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
