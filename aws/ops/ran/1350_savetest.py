import json, urllib.request, time
out={}
# Simulate the browser's exact PUT (with browser-like headers + Origin)
def put_brain(payload,pin,uid="",extra_headers=None):
    url="https://justhodl-data-proxy.raafouis.workers.dev/brain"+("?uid="+uid if uid else "")
    h={"Content-Type":"application/json","X-Brain-Pin":pin,
       "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36",
       "Origin":"https://justhodl.ai","Referer":"https://justhodl.ai/brain.html","Accept":"*/*"}
    if extra_headers: h.update(extra_headers)
    try:
        req=urllib.request.Request(url,data=json.dumps(payload).encode(),headers=h,method="PUT")
        r=urllib.request.urlopen(req,timeout=15)
        return r.status, r.read().decode()[:150]
    except urllib.error.HTTPError as e: return e.code, e.read().decode()[:200]
    except Exception as e: return None, str(e)[:120]
note={"notes":[{"id":"t1","cat":"rule","text":"save path test","created":int(time.time()*1000),"pinned":False}],"updated_at":"2026-06-06"}
# owner brain (no uid) — first PUT bootstraps a pin
s,b=put_brain(note,"savetest1234")
out["owner_put"]={"status":s,"body":b}
# also test OPTIONS preflight (browsers send this before cross-origin PUT)
def options():
    try:
        req=urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/brain",
            headers={"Origin":"https://justhodl.ai","Access-Control-Request-Method":"PUT","Access-Control-Request-Headers":"content-type,x-brain-pin"},method="OPTIONS")
        r=urllib.request.urlopen(req,timeout=10)
        return r.status, dict(r.headers).get("Access-Control-Allow-Headers","(none)")
    except urllib.error.HTTPError as e: return e.code, e.read().decode()[:80]
    except Exception as e: return None, str(e)[:80]
os_,oh=options()
out["options_preflight"]={"status":os_,"allow_headers":oh}
open("aws/ops/reports/1350_st.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
