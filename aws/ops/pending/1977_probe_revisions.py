"""1977 — probe the Benzinga estimate-REVISION surface (updated.gte) + check for
any native consensus/estimate-history endpoint in the entitlement, to design a
revision-momentum engine correctly."""
import os, json, urllib.request, urllib.error, boto3, datetime
def key():
    k=os.environ.get("MASSIVE_API_KEY")
    if not k:
        try:k=boto3.client("ssm","us-east-1").get_parameter(Name="/justhodl/massive-api-key",WithDecryption=True)["Parameter"]["Value"]
        except Exception:k=""
    return k
K=key(); B="https://api.polygon.io"
def get(url):
    try: return json.loads(urllib.request.urlopen(urllib.request.Request(url+f"&apiKey={K}",headers={"User-Agent":"jh/1"}),timeout=30).read())
    except urllib.error.HTTPError as e: return {"_http":e.code,"_body":e.read().decode()[:120]}
    except Exception as e: return {"_err":f"{type(e).__name__}:{e}"}
today=datetime.date.today()
wk=(today-datetime.timedelta(days=7)).isoformat()
d30=(today-datetime.timedelta(days=30)).isoformat()

print("=== updated.gte = recently-revised rows (last 7d), sorted by last_updated ===")
j=get(f"{B}/benzinga/v1/earnings?updated.gte={wk}&order=desc&sort=last_updated&limit=12")
res=j.get("results") or []
print(f"rows={len(res)} next_url={'y' if j.get('next_url') else 'n'} {j.get('_http','')}")
for r in res[:10]:
    print(f"  {r.get('ticker'):<6} upd={r.get('last_updated','')[:16]} date={r.get('date')} imp{r.get('importance')} estEPS={r.get('estimated_eps')} status={r.get('date_status')} fp={r.get('fiscal_period')}{r.get('fiscal_year')}")

print("\n=== multiple rows for ONE ticker+period? (estimate history within feed) ===")
j2=get(f"{B}/benzinga/v1/earnings?ticker=NVDA&order=desc&sort=last_updated&limit=8")
for r in (j2.get('results') or [])[:8]:
    print(f"  NVDA fp={r.get('fiscal_period')}{r.get('fiscal_year')} date={r.get('date')} estEPS={r.get('estimated_eps')} actual={r.get('actual_eps')} upd={r.get('last_updated','')[:16]}")

print("\n=== probe candidate native consensus/estimate endpoints ===")
for path in ["benzinga/v1/consensus-eps","benzinga/v1/estimates","benzinga/v1/consensus",
             "benzinga/v1/analyst-insights","benzinga/v1/ratings","benzinga/v1/guidance"]:
    r=get(f"{B}/{path}?limit=1&x=1")
    tag = "OK results" if isinstance(r,dict) and "results" in r else (f"HTTP{r.get('_http')}" if r.get('_http') else r.get('_err','?'))
    print(f"  /{path:<28} -> {tag}")
print("DONE 1977")
