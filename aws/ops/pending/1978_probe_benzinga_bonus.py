"""1978 — confirm whether ratings / guidance / analyst-insights are ENTITLED +
populated (real recent data), and dump their full field surface. If yes, these
are far higher-value than an estimate-revision differ (PT changes, guidance cuts)."""
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
    except urllib.error.HTTPError as e: return {"_http":e.code,"_body":e.read().decode()[:160]}
    except Exception as e: return {"_err":f"{type(e).__name__}:{e}"}
today=datetime.date.today(); wk=(today-datetime.timedelta(days=7)).isoformat()

for name,path,extra in [
    ("RATINGS (analyst up/downgrades + PT changes)","benzinga/v1/ratings",f"date.gte={wk}&order=desc&sort=date"),
    ("GUIDANCE (company forward guidance raises/cuts)","benzinga/v1/guidance",f"date.gte={wk}&order=desc&sort=date"),
    ("ANALYST-INSIGHTS","benzinga/v1/analyst-insights",f"order=desc&sort=date"),
]:
    print("="*64); print(name); print("="*64)
    j=get(f"{B}/{path}?{extra}&limit=5")
    if "_http" in j: print("  BLOCKED/err:",j.get("_http"),j.get("_body")); print(); continue
    res=j.get("results") or []
    print(f"  results={len(res)} next_url={'y' if j.get('next_url') else 'n'}")
    if res:
        print("  ALL fields on row 0:")
        for k,v in res[0].items(): print(f"    {k}: {v}")
        print("  recency (date of latest 5):",[r.get('date') for r in res[:5]])
        print("  sample tickers:",[r.get('ticker') for r in res[:5]])
    print()
# also test ticker-filtered to confirm usable
print("=== ratings for AAPL (confirm ticker filter works) ===")
j=get(f"{B}/benzinga/v1/ratings?ticker=AAPL&order=desc&sort=date&limit=3")
for r in (j.get('results') or [])[:3]:
    print(f"  {r.get('date')} {r.get('ticker')} {r.get('action_company','')}/{r.get('action_pt','')} rating {r.get('rating_prior','?')}->{r.get('rating_current','?')} PT {r.get('pt_prior','?')}->{r.get('pt_current','?')} [{r.get('analyst_name') or r.get('firm','')}]")
print("DONE 1978")
