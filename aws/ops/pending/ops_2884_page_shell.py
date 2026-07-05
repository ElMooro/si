"""ops 2884 — site-wide shell rollout finish: Function URL for click-generated AI, live config,
page-ai-manifest merge to all pages, end-to-end verification."""
import os, json, time, traceback, urllib.request, boto3
from datetime import datetime, timezone
from botocore.config import Config
REGION="us-east-1"; FN="justhodl-page-ai"; B="justhodl-dashboard-live"
R={"ops":2884,"ts":datetime.now(timezone.utc).isoformat(),"errors":{}}
def guard(name):
    def deco(fn):
        def run(*a,**k):
            try: return fn(*a,**k)
            except Exception:
                R["errors"][name]=traceback.format_exc()[-450:]; return None
        return run
    return deco
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=170,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
def sread(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception: return None
def swrite(k,d):
    s3.put_object(Bucket=B,Key=k,Body=json.dumps(d,ensure_ascii=False,default=str).encode(),ContentType="application/json",CacheControl="max-age=300")

@guard("wait_deploy")
def wait_deploy():
    for _ in range(45):
        c=lam.get_function_configuration(FunctionName=FN)
        if c.get("LastModified","")>R["ts"][:19] and c.get("LastUpdateStatus")=="Successful":
            R["fn_deployed"]=c.get("LastModified"); return True
        time.sleep(6)
    R["fn_deployed"]="TIMEOUT-continuing"; return True

@guard("function_url")
def function_url():
    try:
        R["live_url"]=lam.get_function_url_config(FunctionName=FN)["FunctionUrl"]; R["url_action"]="existing"
    except Exception:
        R["live_url"]=lam.create_function_url_config(FunctionName=FN,AuthType="NONE",
            Cors={"AllowOrigins":["*"],"AllowMethods":["GET"],"AllowHeaders":["*"],"MaxAge":86400})["FunctionUrl"]
        try: lam.add_permission(FunctionName=FN,StatementId="url-public",Action="lambda:InvokeFunctionUrl",Principal="*",FunctionUrlAuthType="NONE")
        except Exception as e:
            if "ResourceConflict" not in str(e): raise
        R["url_action"]="created"
    swrite("data/page-ai-live.json",{"url":R["live_url"],"updated":R["ts"]})
    return True

@guard("manifest_merge")
def manifest_merge():
    scan=json.load(open("aws/ops/pending/_page_feeds_scan.json"))
    man=sread("data/page-ai-manifest.json") or {}
    before=len(man); added=0
    for page,info in scan.items():
        if page in man: continue
        dfs=[f[5:-5] for f in (info.get("feeds") or []) if f.startswith("data/") and f.endswith(".json")][:6]
        man[page]={"title":info.get("title") or page,"data_files":dfs,"engines":dfs}
        added+=1
    swrite("data/page-ai-manifest.json",man)
    R["page_ai_manifest"]={"before":before,"added":added,"total":len(man)}
    return True

@guard("live_test")
def live_test():
    u=R.get("live_url")
    if not u: return None
    req=urllib.request.Request(u.rstrip("/")+"?mode=live&page=canaries",headers={"User-Agent":"Mozilla/5.0"})
    d=json.loads(urllib.request.urlopen(req,timeout=120).read())
    R["live_test"]={"page":d.get("page"),"on_click":d.get("generated_on_click"),
        "has_explain":bool(d.get("what_it_is")),"has_outlook":bool(d.get("outlook")),
        "keys":list(d.keys())[:10]}
    return True

@guard("site_checks")
def site_checks():
    def get(u):
        req=urllib.request.Request(u+"?t=%d"%time.time(),headers={"User-Agent":"Mozilla/5.0"})
        return urllib.request.urlopen(req,timeout=20).read().decode("utf-8","ignore")
    for _ in range(24):
        try:
            man=json.loads(get("https://justhodl.ai/nav-manifest.json"))
            dj=get("https://justhodl.ai/jh-nav-drawer.js")
            pj=get("https://justhodl.ai/jh-page-ai.js")
            pg=get("https://justhodl.ai/brain-compiler.html")
            R["site"]={"nav_n_pages":man.get("n_pages"),"drawer_has_favs":("jh_favs" in dj),
                "panel_has_gen":("jhpai-gen" in pj),
                "injected_page_ok":("jh-nav-drawer.js" in pg and "jh-page-ai.js" in pg)}
            if man.get("n_pages")==366 and R["site"]["drawer_has_favs"] and R["site"]["panel_has_gen"] and R["site"]["injected_page_ok"]:
                return True
        except Exception: pass
        time.sleep(8)
    return True

wait_deploy(); function_url(); manifest_merge(); live_test(); site_checks()
s=R.get("site") or {}
R["status"]="LIVE" if (not R["errors"] and s.get("nav_n_pages")==366 and s.get("drawer_has_favs") and s.get("panel_has_gen") and (R.get("live_test") or {}).get("page")) else "PARTIAL"
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:3200])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2884_page_shell.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2884 COMPLETE")
