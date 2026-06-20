"""1958 — re-probe with CORRECT params:
 (A) ETF Global Constituents via composite_ticker + latest processed_date (SPY/XLK/QQQ)
 (B) Benzinga reported earnings (confirm actual eps + surprise fields exist)
"""
import os, json, urllib.request, urllib.error, boto3, datetime
def key():
    k=os.environ.get("MASSIVE_API_KEY")
    if not k:
        try: k=boto3.client("ssm","us-east-1").get_parameter(Name="/justhodl/massive-api-key",WithDecryption=True)["Parameter"]["Value"]
        except Exception: k=""
    return k
K=key(); B="https://api.polygon.io"
def get(url):
    u=url+f"&apiKey={K}"
    try:
        req=urllib.request.Request(u,headers={"User-Agent":"jh-probe/1.0"})
        with urllib.request.urlopen(req,timeout=25) as r: return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e: return e.code, e.read().decode()[:200]
    except Exception as e: return None, f"{type(e).__name__}: {e}"

print("="*64); print("(A) CONSTITUENTS via composite_ticker"); print("="*64)
for etf in ["SPY","XLK","QQQ"]:
    s,j=get(f"{B}/etf-global/v1/constituents?composite_ticker={etf}&order=desc&sort=processed_date&limit=600")
    if s!=200: print(f"\n  {etf}: HTTP {s} {j}"); continue
    res=j.get("results") or []
    if not res: print(f"\n  {etf}: 200 but empty (status={j.get('status')})"); continue
    # latest processed_date slice
    latest_pd=max(r.get("processed_date","") for r in res)
    snap=[r for r in res if r.get("processed_date")==latest_pd]
    snap=sorted(snap,key=lambda x:(x.get("constituent_rank") or 999))
    print(f"\n  {etf}: latest processed_date={latest_pd}  rows_total={len(res)}  holdings_in_snapshot={len(snap)}")
    for r in snap[:6]:
        print(f"     #{r.get('constituent_rank')}: {r.get('constituent_ticker'):<12} {str(r.get('constituent_name'))[:26]:<26} mv={r.get('market_value')} sh={r.get('shares_held')}")

print("\n"+"="*64); print("(B) BENZINGA REPORTED EARNINGS (actual+surprise)"); print("="*64)
today=datetime.date.today().isoformat()
for url in [f"{B}/benzinga/v1/earnings?ticker=AAPL&date.lte={today}&order=desc&sort=date&limit=3",
            f"{B}/benzinga/v1/earnings?ticker=NVDA&date.lte={today}&order=desc&sort=date&limit=2"]:
    s,j=get(url)
    print(f"\n  {url.split('?')[1].split('&apiKey')[0]} -> HTTP {s}")
    if s==200:
        for r in (j.get("results") or [])[:3]:
            print("    fields:", list(r.keys()))
            print("    rec:", json.dumps({k:r.get(k) for k in ("ticker","date","date_status","eps","eps_est","eps_surprise_percent","estimated_eps","revenue","revenue_surprise_percent","fiscal_period","fiscal_year")}))
print("\nDONE 1958")
