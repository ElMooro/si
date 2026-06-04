"""1279 — test the worker brief route from AWS (real internet) post-fix."""
import json, urllib.request, boto3
from botocore.config import Config
out={}
def get(url):
    try:
        req=urllib.request.Request(url+"?t=1",headers={"User-Agent":"Mozilla/5.0","Origin":"https://justhodl.ai"})
        with urllib.request.urlopen(req,timeout=15) as r:
            b=r.read().decode()[:200]
            return {"status":r.status,"len":len(b),"preview":b[:120]}
    except urllib.error.HTTPError as e: return {"status":e.code,"err":"HTTPError"}
    except Exception as e: return {"err":str(e)[:120]}
W="https://justhodl-data-proxy.raafouis.workers.dev"
out["bare_slug"]=get(f"{W}/bond-vol-decisive-call.json")
out["data_path"]=get(f"{W}/data/bond-vol-decisive-call.json")
out["dislocations_bare"]=get(f"{W}/dislocations.json")
# We run this via a quick Lambda using boto3? No — ops runs in GH Actions runner (has internet).
open("aws/ops/reports/1279_brief_test.json","w").write(json.dumps(out,indent=2,default=str))
print(json.dumps(out,indent=2))
