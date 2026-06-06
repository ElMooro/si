import json, urllib.request, time
out={}
def get(u):
    try:
        req=urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36","Origin":"https://justhodl.ai","Referer":"https://justhodl.ai/brain.html"})
        return urllib.request.urlopen(req,timeout=15).read().decode("utf-8","replace")
    except urllib.error.HTTPError as e: return f"HTTP {e.code}"
    except Exception as e: return "ERR:"+str(e)[:50]
# brain GET (read path)
b=get("https://justhodl-data-proxy.raafouis.workers.dev/brain?t=%d"%int(time.time()))
try: j=json.loads(b); out["brain_get"]={"ok":True,"notes":len(j.get("notes",[])),"pin_set":j.get("pin_set"),"scope":j.get("scope")}
except: out["brain_get"]=str(b)[:80]
# pages live with per-user wiring
p=get("https://justhodl.ai/brain.html?t=%d"%int(time.time()))
out["brain_page"]={"uid_wired":"uidQ" in p if isinstance(p,str) else p,"dropzone":"handleFiles" in p if isinstance(p,str) else p,"status_panel":"renderStatus" in p if isinstance(p,str) else p}
j2=get("https://justhodl.ai/journal.html?t=%d"%int(time.time()))
out["journal_page_uid"]="uidQ" in j2 if isinstance(j2,str) else j2
open("aws/ops/reports/1349_bf.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
