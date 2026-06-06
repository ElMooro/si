import json, urllib.request
out={}
try:
    req=urllib.request.Request("https://justhodl.ai/settings.html?t=77",headers={"User-Agent":"Mozilla/5.0"})
    h=urllib.request.urlopen(req,timeout=20).read().decode("utf-8","replace")
    out["bytes"]=len(h)
    out["has_authconfig"]="auth-config.js" in h
    out["has_supabase_cdn"]="supabase-js" in h
    out["has_authjs"]="/auth.js" in h
    out["has_safety_timeout"]="safety" in h
    out["has_v3"]="?v=3" in h
    # extract the inline init script tail
    i=h.rfind("DOMContentLoaded")
    out["init_snippet"]=h[i:i+400] if i>0 else "NOT FOUND"
except Exception as e: out["err"]=str(e)[:150]
open("aws/ops/reports/1319_raw.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
