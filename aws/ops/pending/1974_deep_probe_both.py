"""1974 — enumerate the FULL data surface of Benzinga Earnings + ETF Constituents
to find everything untapped. Probes: full fields, forward calendar breadth,
share-count deltas, additions/deletions, query params."""
import os, json, urllib.request, urllib.error, boto3, datetime
def key():
    k=os.environ.get("MASSIVE_API_KEY")
    if not k:
        try:k=boto3.client("ssm","us-east-1").get_parameter(Name="/justhodl/massive-api-key",WithDecryption=True)["Parameter"]["Value"]
        except Exception:k=""
    return k
K=key(); B="https://api.polygon.io"
def get(url):
    try:
        return json.loads(urllib.request.urlopen(urllib.request.Request(url+f"&apiKey={K}",headers={"User-Agent":"jh/1"}),timeout=30).read())
    except urllib.error.HTTPError as e: return {"_http":e.code,"_body":e.read().decode()[:160]}
    except Exception as e: return {"_err":f"{type(e).__name__}:{e}"}
today=datetime.date.today(); d14=(today+datetime.timedelta(days=14)).isoformat()

print("="*66); print("BENZINGA EARNINGS — full surface"); print("="*66)
# full field dump on a reported row
j=get(f"{B}/benzinga/v1/earnings?ticker=NVDA&date.lte={today}&order=desc&sort=date&limit=1")
r=(j.get("results") or [{}])[0]
print("ALL fields on a reported row:")
for k,v in r.items(): print(f"   {k}: {v}")
# forward CALENDAR breadth (no ticker, next 14d) — untapped market-wide
cal=get(f"{B}/benzinga/v1/earnings?date.gte={today}&date.lte={d14}&order=asc&sort=date&limit=1000")
res=cal.get("results") or []
print(f"\nForward 14d CALENDAR (no ticker filter): {len(res)} rows, next_url={'yes' if cal.get('next_url') else 'no'}")
if res:
    from collections import Counter
    imp=Counter(x.get('importance') for x in res)
    print("  importance distribution:", dict(sorted(imp.items(),key=lambda x:str(x[0]))))
    print("  sample upcoming high-importance:", [(x.get('ticker'),x.get('date'),x.get('importance'),x.get('estimated_eps')) for x in res if (x.get('importance') or 0)>=4][:6])
# updated-since param (revisions feed)
us=get(f"{B}/benzinga/v1/earnings?ticker=AAPL&updated.gte={today}&limit=2")
print("\nupdated.gte supported?", "results" in us, us.get("_http",""))

print("\n"+"="*66); print("ETF CONSTITUENTS — full surface"); print("="*66)
j=get(f"{B}/etf-global/v1/constituents?composite_ticker=XLK&processed_date.gte={(today-datetime.timedelta(days=30)).isoformat()}&order=desc&sort=processed_date&limit=1")
r=(j.get("results") or [{}])[0]
print("ALL fields on a constituent row:")
for k,v in r.items(): print(f"   {k}: {v}")
# share-count DELTAS: compare two snapshots ~20d apart for XLK
def snap(etf, gte, lte):
    jj=get(f"{B}/etf-global/v1/constituents?composite_ticker={etf}&processed_date.gte={gte}&processed_date.lte={lte}&limit=1000")
    rr=jj.get("results") or []
    if not rr: return None,{}
    pd=max(x.get("processed_date","") for x in rr)
    return pd,{x["constituent_ticker"]:x for x in rr if x.get("processed_date")==pd and x.get("constituent_ticker")}
pd_new,new=snap("XLK",(today-datetime.timedelta(days=5)).isoformat(),today.isoformat())
pd_old,old=snap("XLK",(today-datetime.timedelta(days=35)).isoformat(),(today-datetime.timedelta(days=25)).isoformat())
print(f"\nXLK snapshots: new={pd_new}({len(new)}) vs old={pd_old}({len(old)})")
if new and old:
    adds=[t for t in new if t not in old]; dels=[t for t in old if t not in new]
    print("  ADDITIONS (new holdings):", adds[:10])
    print("  DELETIONS (dropped):", dels[:10])
    deltas=[]
    for t in new:
        if t in old:
            sn=new[t].get("shares_held"); so=old[t].get("shares_held")
            if isinstance(sn,(int,float)) and isinstance(so,(int,float)) and so:
                deltas.append((t,round((sn-so)/so*100,1),sn-so))
    deltas.sort(key=lambda x:abs(x[1]),reverse=True)
    print("  largest SHARE-COUNT changes (ETF actually bought/sold the name):")
    for t,pct,d in deltas[:8]: print(f"    {t:<6} {pct:+.1f}% shares ({d:+,.0f})")
print("\nDONE 1974")
