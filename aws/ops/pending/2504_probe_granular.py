import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def g(k): return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
print("===== finviz-groups INDUSTRIES (unused on page) =====")
fg=g("data/finviz-groups.json")
ind=fg.get("industries") or []
print("n_industries:",len(ind))
if ind:
    s=sorted(ind,key=lambda x:-(x.get("rel_volume") or 0))[:5]
    for x in s: print("  %-30s perf_w=%-6s perf_m=%-6s rel_vol=%-5s change=%s"%(x.get("name"),x.get("perf_w"),x.get("perf_m"),x.get("rel_volume"),x.get("change")))
print("countries:",len(fg.get("countries") or []),"| mktcaps:",len(fg.get("mktcaps") or []))
print("\n===== universe.json (per-ticker sector+industry for rollup) =====")
u=g("data/universe.json"); st=u.get("stocks") or []
print("n_stocks:",len(st))
wsec=[x for x in st if x.get("sector")]; wind=[x for x in st if x.get("industry")]
print("with sector:",len(wsec),"| with industry:",len(wind))
if st: print("sample:",json.dumps({k:st[0].get(k) for k in ('symbol','sector','industry','market_cap')}))
print("\n===== Polygon grouped-daily: confirm shape (1 call = whole market) =====")
import urllib.request
POLY="zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
from datetime import date,timedelta
d=(date.today()-timedelta(days=4)).isoformat()
url=f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{d}?adjusted=true&apiKey={POLY}"
try:
    j=json.loads(urllib.request.urlopen(url,timeout=30).read())
    res=j.get("results") or []
    print(f"grouped {d}: status={j.get('status')} n_tickers={len(res)} sample={json.dumps(res[0]) if res else 'none'}")
except Exception as e: print("grouped ERR",str(e)[:80])
print("DONE 2504")
