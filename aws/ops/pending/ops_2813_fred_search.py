"""ops 2813 — FRED search for valid crude-stocks / production / natgas-storage series."""
import os, json, urllib.request, urllib.parse
from datetime import datetime, timezone
import boto3
lam=boto3.client("lambda",region_name="us-east-1")
R={"ops":2813,"ts":datetime.now(timezone.utc).isoformat()}
FRED=""
for eng in ("justhodl-china-liquidity","justhodl-macro-leads"):
    env=lam.get_function_configuration(FunctionName=eng).get("Environment",{}).get("Variables",{})
    FRED=env.get("FRED_API_KEY") or env.get("FRED_KEY") or FRED
    if FRED: break
def search(q):
    try:
        u=("https://api.stlouisfed.org/fred/series/search?search_text=%s&api_key=%s&file_type=json"
           "&limit=6&order_by=popularity&sort_order=desc"%(urllib.parse.quote(q),FRED))
        d=json.loads(urllib.request.urlopen(u,timeout=20).read())
        return [{"id":s["id"],"title":s["title"][:60],"freq":s.get("frequency_short"),"end":s.get("observation_end")} for s in d.get("seriess",[])]
    except Exception as e: return "ERR "+str(e)[:60]
R["search"]={
 "crude_stocks":search("weekly ending stocks crude oil excluding SPR"),
 "crude_production":search("field production crude oil weekly"),
 "natgas_storage":search("working gas underground storage lower 48"),
 "gasoline_stocks":search("ending stocks total gasoline"),
}
print(json.dumps(R["search"],indent=1))
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/2813_fred_search.json","w"),indent=1,default=str)
print("OPS 2813 COMPLETE")
