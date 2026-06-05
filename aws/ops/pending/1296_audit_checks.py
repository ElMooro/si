"""1296 — verify screener live + sovereign-stress status."""
import json, urllib.request, boto3
from botocore.config import Config
out={}
def get(url):
    try:
        req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0"})
        r=urllib.request.urlopen(req,timeout=20); b=r.read().decode("utf-8","replace")
        return r.status, ("Unicorns" in b or "There isn't a GitHub Pages site" in b or "404" in b[:500]), len(b)
    except urllib.error.HTTPError as e: return e.code, True, 0
    except Exception as e: return None, True, str(e)[:60]
for path in ["/screener/","/screener/index.html","/my-portfolio.html","/ask.html","/track-public.html"]:
    s,bad,ln=get("https://justhodl.ai"+path)
    out[path]={"status":s,"looks_404":bad,"bytes":ln}
# sovereign-stress deployed?
try:
    lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=60))
    try: c=lam.get_function_configuration(FunctionName="justhodl-sovereign-stress"); out["sovereign_stress"]={"deployed":True,"last":c.get("LastModified")}
    except Exception: out["sovereign_stress"]={"deployed":False}
except Exception as e: out["sovereign_stress"]=str(e)[:80]
open("aws/ops/reports/1296_audit.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
