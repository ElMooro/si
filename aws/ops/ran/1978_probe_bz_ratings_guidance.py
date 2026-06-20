"""1978 — confirm entitlement + full surface of Benzinga ratings / guidance /
analyst-insights. Entitled = returns recent (last ~7d) populated rows, not stubs."""
import os, json, urllib.request, urllib.error, boto3, datetime
from collections import Counter
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
today=datetime.date.today()
wk=(today-datetime.timedelta(days=7)).isoformat()
d3=(today-datetime.timedelta(days=3)).isoformat()

def dump(path, params):
    print("\n"+"="*68); print(path, params); print("="*68)
    q="&".join(f"{k}={v}" for k,v in params.items())
    j=get(f"{B}/{path}?{q}")
    if j.get("_http") or j.get("_err"):
        print("  NOT ENTITLED / error:", j.get("_http"), j.get("_err"), j.get("_body","")); return
    res=j.get("results") or []
    print(f"  rows={len(res)} next_url={'y' if j.get('next_url') else 'n'} status={j.get('status')}")
    if not res: print("  (empty — entitled endpoint but no rows for these params)"); return
    r=res[0]
    print("  ALL fields on row[0]:")
    for k,v in r.items():
        sv=str(v); print(f"    {k}: {sv[:90]}")
    # recency check
    dates=[x.get("date") or x.get("updated") or x.get("last_updated") or "" for x in res]
    dates=[d for d in dates if d]
    if dates: print(f"  recency: newest={max(dates)[:16]} oldest={min(dates)[:16]}")
    return res

# RATINGS — analyst rating actions + price targets
r=dump("benzinga/v1/ratings", {"date.gte":wk,"order":"desc","sort":"date","limit":"8"})
if r:
    acts=Counter((x.get("action_company") or x.get("action") or x.get("rating_action") or "?") for x in r)
    print("  action types:", dict(acts))
    print("  sample PT changes:", [(x.get("ticker"),x.get("pt_prior"),x.get("pt_current"),x.get("rating_current")) for x in r[:5]])

# GUIDANCE — company forward guidance raises/cuts
g=dump("benzinga/v1/guidance", {"date.gte":wk,"order":"desc","sort":"date","limit":"8"})
if g:
    print("  sample guidance:", [(x.get("ticker"),x.get("period"),x.get("eps_guidance_est"),x.get("revenue_guidance_est")) for x in g[:5]])

# ANALYST INSIGHTS
ai=dump("benzinga/v1/analyst-insights", {"date.gte":wk,"order":"desc","sort":"date","limit":"5"})

# also test ticker-filter works on ratings (needed for watchlist joins)
print("\n--- ticker filter test (ratings?ticker=NVDA) ---")
jt=get(f"{B}/benzinga/v1/ratings?ticker=NVDA&order=desc&sort=date&limit=3")
for x in (jt.get('results') or [])[:3]:
    print(f"  NVDA {x.get('date')} {x.get('analyst_firm') or x.get('analyst')} {x.get('rating_prior')}->{x.get('rating_current')} PT {x.get('pt_prior')}->{x.get('pt_current')}")
print("DONE 1978")
