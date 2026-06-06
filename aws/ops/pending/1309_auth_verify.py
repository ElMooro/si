"""1309 — verify auth-config wired live + settings page serves + Supabase reachable."""
import json, urllib.request
out={}
def get(p, host="justhodl.ai"):
    try:
        req=urllib.request.Request(("https://"+host+p) if not p.startswith("http") else p, headers={"User-Agent":"Mozilla/5.0"})
        return urllib.request.urlopen(req,timeout=20).read().decode("utf-8","replace")
    except urllib.error.HTTPError as e: return f"HTTP {e.code}"
    except Exception as e: return "ERR:"+str(e)[:60]
ac=get("/auth-config.js")
out["auth_config"]={"enabled_true":"enabled: true" in ac, "has_url":"bdmjenqcyvzouusfcgow" in ac, "has_key":"sb_publishable" in ac}
st=get("/settings.html")
out["settings"]={"served":"Account &" in st or "data-auth-slot" in st}
# Supabase project reachable (health)
sb=get("https://bdmjenqcyvzouusfcgow.supabase.co/auth/v1/health")
out["supabase_health"]=sb[:80]
ix=get("/index.html")
out["homepage_auth"]={"loads_auth":"auth.js" in ix, "has_slot":"data-auth-slot" in ix}
open("aws/ops/reports/1309_auth.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
