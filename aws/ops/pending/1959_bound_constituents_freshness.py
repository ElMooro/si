"""1959 — bound ETF Global Constituents freshness: query explicit recent date
windows (default ASC ignores order=desc). Find the newest available holdings."""
import os,json,urllib.request,urllib.error,boto3
def key():
    k=os.environ.get("MASSIVE_API_KEY")
    if not k:
        try:k=boto3.client("ssm","us-east-1").get_parameter(Name="/justhodl/massive-api-key",WithDecryption=True)["Parameter"]["Value"]
        except Exception:k=""
    return k
K=key();B="https://api.polygon.io"
def get(url):
    try:
        req=urllib.request.Request(url+f"&apiKey={K}",headers={"User-Agent":"jh/1.0"})
        with urllib.request.urlopen(req,timeout=25) as r:return r.status,json.loads(r.read())
    except urllib.error.HTTPError as e:return e.code,e.read().decode()[:150]
    except Exception as e:return None,f"{type(e).__name__}:{e}"

print("=== bound freshness: newest processed_date in each recent window (SPY) ===")
for gte,lte in [("2026-05-01","2026-06-20"),("2026-01-01","2026-06-20"),
                ("2025-01-01","2025-12-31"),("2023-01-01","2023-12-31"),
                ("2020-01-01","2020-12-31"),("2018-01-01","2018-12-31")]:
    s,j=get(f"{B}/etf-global/v1/constituents?composite_ticker=SPY&processed_date.gte={gte}&processed_date.lte={lte}&limit=1000")
    if s!=200:print(f"  {gte}..{lte}: HTTP {s} {j}");continue
    res=j.get("results") or []
    if not res:print(f"  {gte}..{lte}: 0 rows");continue
    pds=[r.get('processed_date','') for r in res]
    print(f"  {gte}..{lte}: rows={len(res)} min_pd={min(pds)} max_pd={max(pds)}")

print("\n=== if recent window has data, show CURRENT top SPY holdings ===")
s,j=get(f"{B}/etf-global/v1/constituents?composite_ticker=SPY&processed_date.gte=2026-01-01&processed_date.lte=2026-06-20&limit=1000")
if s==200 and (j.get("results")):
    res=j["results"];latest=max(r.get("processed_date","") for r in res)
    snap=sorted([r for r in res if r.get("processed_date")==latest],key=lambda x:(x.get("constituent_rank") or 9999))
    print(f"  newest snapshot {latest}: {len(snap)} holdings")
    for r in snap[:10]:print(f"    #{r.get('constituent_rank')}: {r.get('constituent_ticker'):<8} {str(r.get('constituent_name'))[:24]:<24} mv={r.get('market_value')}")
else:
    print("  no 2026 data — entitlement is historical-only")
print("\nDONE 1959")
