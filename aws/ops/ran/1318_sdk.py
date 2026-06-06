import json, urllib.request
out={}
def get(u):
    try:
        req=urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0"})
        r=urllib.request.urlopen(req,timeout=20); return r.status, r.read().decode("utf-8","replace")[:200]
    except urllib.error.HTTPError as e: return e.code, str(e)[:100]
    except Exception as e: return None, str(e)[:100]
# does the SDK URL resolve?
s,b=get("https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2")
out["sdk_cdn"]={"status":s,"head":b[:120]}
# settings page script tags
s2,b2=get("https://justhodl.ai/settings.html?t=9")
import re
out["settings_scripts"]=re.findall(r'<script src="[^"]*(?:supabase|auth)[^"]*"',b2) if isinstance(b2,str) else b2[:100]
open("aws/ops/reports/1318_sdk.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
