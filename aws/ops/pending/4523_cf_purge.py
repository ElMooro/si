"""ops 4523 — Khalid's screenshot showed the 19:08 snapshot verbatim after
5+ hard-refresh cycles: not browser cache (bypassed by hard-refresh), it's
Cloudflare's EDGE holding a stale copy of /data/*.json served through the
justhodl-data-proxy worker route. Purge the zone (proven pattern, ops
3309/3338) + prove staleness before/after via cache-buster fetch."""
import json,os,time,urllib.request
from datetime import datetime,timezone
import boto3
REGION="us-east-1"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION)
R={"ops":4523,"started":datetime.now(timezone.utc).isoformat()}

def live_fetch(u):
    try:
        req=urllib.request.Request(u,headers={"Cache-Control":"no-cache","User-Agent":"ops4523"})
        with urllib.request.urlopen(req,timeout=20) as r:
            return json.loads(r.read()),dict(r.headers)
    except Exception as e:
        return {"err":str(e)[:100]},{}

before,hdr_b=live_fetch("https://justhodl.ai/data/provider-catalog.json?cb="+str(int(time.time())))
R["before"]={"totals":before.get("totals"),"as_of":before.get("as_of"),
             "cf_cache_status":hdr_b.get("CF-Cache-Status"),"age":hdr_b.get("Age")}

def cf(path,method="GET",data=None):
    tok=os.environ.get("CLOUDFLARE_API_TOKEN","")
    if not tok: return None,"no CLOUDFLARE_API_TOKEN in env"
    req=urllib.request.Request("https://api.cloudflare.com/client/v4"+path,
        data=(json.dumps(data).encode() if data else None),method=method,
        headers={"Authorization":"Bearer "+tok,"Content-Type":"application/json"})
    try:
        with urllib.request.urlopen(req,timeout=30) as r: return json.loads(r.read()),None
    except Exception as e: return None,str(e)[:150]

zj,zerr=cf("/zones?name=justhodl.ai")
zid=((zj or {}).get("result") or [{}])[0].get("id") if zj else None
R["zone_lookup"]={"zid_found":bool(zid),"err":zerr}
purged=False
if zid:
    # targeted purge (files) first — falls back to purge_everything
    pj,perr=cf(f"/zones/{zid}/purge_cache","POST",
        {"files":["https://justhodl.ai/data/provider-catalog.json",
                  "https://justhodl.ai/data.html",
                  "https://justhodl.ai/data/providers/fred.json",
                  "https://justhodl.ai/data/providers/polygon.json"]})
    R["purge_files"]={"ok":(pj or {}).get("success"),"err":perr,"errs":(pj or {}).get("errors")}
    purged=bool((pj or {}).get("success"))
    if not purged:
        pj2,perr2=cf(f"/zones/{zid}/purge_cache","POST",{"purge_everything":True})
        R["purge_everything"]={"ok":(pj2 or {}).get("success"),"err":perr2}
        purged=bool((pj2 or {}).get("success"))
time.sleep(20)
after,hdr_a=live_fetch("https://justhodl.ai/data/provider-catalog.json?cb="+str(int(time.time())+1))
R["after"]={"totals":after.get("totals"),"as_of":after.get("as_of"),
            "cf_cache_status":hdr_a.get("CF-Cache-Status")}
R["fixed"]=(after.get("totals",{}).get("providers")==39 or
            after.get("totals",{}).get("keys",0)>1000)

def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0806-master","from":"claude","to":"perplexity","kind":"propose",
 "content":("CF EDGE CACHE PURGED (root cause of Khalid's stale 19:08 screenshot after 5+ "
  f"refreshes — not browser, Cloudflare edge via data-proxy worker route). before={json.dumps(R['before'])[:180]} "
  f"zone={R['zone_lookup']} purge={json.dumps(R.get('purge_files') or R.get('purge_everything'))[:150]} "
  f"after={json.dumps(R['after'])[:180]} FIXED={R['fixed']}. If still stale: propagation lag ~30-60s, "
  "or pages.yml's own self-purge (ops 3309) needs re-verification. Verify+seal."),
 "evidence":[{"kind":"log","ref":"data/provider-catalog.json","snippet":"totals"}]})
bus({"action":"fanout_pending"})
R["verdict"]=f"FIXED={R['fixed']} before={R['before']} after={R['after']} purged={purged}"
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4523_cf_purge.json","w"),indent=1,default=str)
open("aws/ops/reports/4523_cf_purge.md","w").write("# 4523 CF purge — "+R["verdict"]+"\n")
print(R["verdict"])
