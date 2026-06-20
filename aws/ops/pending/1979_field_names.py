"""1979 — lock exact field names for PT (ratings) + EPS guidance (guidance) by
finding rows that actually carry them."""
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
    except urllib.error.HTTPError as e: return {"_http":e.code}
    except Exception as e: return {"_err":str(e)}
wk=(datetime.date.today()-datetime.timedelta(days=10)).isoformat()

print("=== RATINGS rows WITH a price_target_action — find PT numeric fields ===")
j=get(f"{B}/benzinga/v1/ratings?date.gte={wk}&order=desc&sort=date&limit=60")
pt_rows=[r for r in (j.get('results') or []) if r.get('price_target_action')]
print(f"  {len(pt_rows)} of {len(j.get('results') or [])} rows have price_target_action")
seen=set()
for r in pt_rows[:4]:
    print(f"  --- {r.get('ticker')} {r.get('price_target_action')} ({r.get('firm')}) ---")
    for k,v in r.items():
        if 'target' in k.lower() or 'pt' in k.lower() or 'price' in k.lower():
            print(f"      {k}: {v}")
    seen.update(r.keys())
print("  all PT-related keys seen:", sorted(k for k in seen if 'target' in k.lower() or k.lower().startswith('pt') or 'price' in k.lower()))

print("\n=== GUIDANCE rows — find EPS guidance field names ===")
j2=get(f"{B}/benzinga/v1/guidance?date.gte={wk}&order=desc&sort=date&limit=60")
allkeys=set()
eps_rows=[]
for r in (j2.get('results') or []):
    allkeys.update(r.keys())
    if any('eps' in k.lower() and r.get(k) is not None for k in r):
        eps_rows.append(r)
print("  all guidance keys:", sorted(allkeys))
print(f"  {len(eps_rows)} rows carry non-null EPS guidance")
for r in eps_rows[:3]:
    print(f"  --- {r.get('ticker')} {r.get('fiscal_period')}{r.get('fiscal_year')} ---")
    for k,v in r.items():
        if 'eps' in k.lower(): print(f"      {k}: {v}")
print("DONE 1979")
