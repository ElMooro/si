import json, urllib.request, time
out={}
dev="dev-"+("c"*36)
note={"notes":[{"id":"c1","cat":"rule","text":"cors check","created":int(time.time()*1000)}],"updated_at":"2026-06-06"}
try:
    req=urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/brain?uid="+dev,
        data=json.dumps(note).encode(),headers={"Content-Type":"text/plain","Origin":"https://justhodl.ai","User-Agent":"Mozilla/5.0 Chrome/126"},method="PUT")
    r=urllib.request.urlopen(req,timeout=20)
    out["status"]=r.getcode()
    out["resp_headers"]={k:v for k,v in r.headers.items() if k.lower().startswith("access-control") or k.lower()=="content-type"}
except urllib.error.HTTPError as e: out["err"]=e.code
# also: is the brain page maybe POSTing to a stale/wrong worker URL? check what URL the live page uses
try:
    h=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/brain.html?t=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0"}),timeout=15).read().decode()
    import re
    out["page_proxy_url"]=re.findall(r'const PROXY="([^"]+)"',h)[:1]
    out["page_save_fn"]="_doSave" in h
    out["page_backup_in_worker"]=None
    # check the page's save still references text/plain + uidQ
    out["save_uses_text_plain"]="text/plain" in h
except Exception as e: out["page"]=str(e)[:60]
open("aws/ops/reports/1364_cors.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
