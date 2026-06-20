"""1986 — empirical FMP vs Benzinga comparison: history depth + coverage + fields,
for earnings surprises, analyst estimates, ratings/PT. Decides consolidation."""
import os, json, urllib.request, urllib.error, boto3, datetime
def ssm(n):
    try: return boto3.client("ssm","us-east-1").get_parameter(Name=n,WithDecryption=True)["Parameter"]["Value"]
    except Exception: return ""
MASS=os.environ.get("MASSIVE_API_KEY") or ssm("/justhodl/massive-api-key")
FMP=os.environ.get("FMP_KEY") or "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
def get(url):
    try: return json.loads(urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"jh/1"}),timeout=30).read())
    except urllib.error.HTTPError as e: return {"_http":e.code,"_b":e.read().decode()[:120]}
    except Exception as e: return {"_err":f"{type(e).__name__}:{e}"}
def span(dates):
    ds=sorted([d for d in dates if d])
    if not ds: return ("—","—",0)
    try:
        y=(datetime.date.fromisoformat(ds[-1][:10])-datetime.date.fromisoformat(ds[0][:10])).days/365.25
    except Exception: y=0
    return (ds[0][:10],ds[-1][:10],round(y,1))
TK="AAPL"; FB="https://financialmodelingprep.com/stable"; BZ="https://api.polygon.io/benzinga/v1"

print("="*70); print(f"EARNINGS SURPRISE HISTORY ({TK})"); print("="*70)
f=get(f"{FB}/earnings?symbol={TK}&limit=400&apikey={FMP}")
if isinstance(f,list):
    withest=[r for r in f if r.get("epsEstimated") is not None]
    o,n,y=span([r.get("date") for r in f])
    print(f"  FMP:      rows={len(f)} (w/estimate={len(withest)})  span {o} → {n}  ({y}y)")
    print(f"    fields: {sorted(f[0].keys()) if f else '—'}")
else: print("  FMP earnings:",f)
b=get(f"{BZ}/earnings?ticker={TK}&order=asc&sort=date&limit=400&apiKey={MASS}")
br=b.get("results") if isinstance(b,dict) else None
if br is not None:
    rep=[r for r in br if r.get("actual_eps") is not None]
    o,n,y=span([r.get("date") for r in br])
    print(f"  BENZINGA: rows={len(br)} (reported={len(rep)})  span {o} → {n}  ({y}y)  next_url={'y' if b.get('next_url') else 'n'}")
else: print("  Benzinga earnings:",b)

print("\n"+"="*70); print(f"ANALYST ESTIMATES / REVISIONS ({TK})"); print("="*70)
f=get(f"{FB}/analyst-estimates?symbol={TK}&period=annual&limit=60&apikey={FMP}")
if isinstance(f,list) and f:
    o,n,y=span([r.get("date") for r in f])
    print(f"  FMP analyst-estimates: rows={len(f)}  span {o} → {n} ({y}y)")
    print(f"    fields: {sorted(f[0].keys())}")
else: print("  FMP analyst-estimates:",f if not isinstance(f,list) else "empty")
print("  BENZINGA: no historical consensus endpoint — current estimate only per period")
print("            (revision history must be self-built via daily snapshots = my estimate-revisions)")

print("\n"+"="*70); print(f"ANALYST RATINGS / PRICE TARGETS ({TK})"); print("="*70)
for ep in ["grades-historical","grades","price-target-summary"]:
    f=get(f"{FB}/{ep}?symbol={TK}&limit=600&apikey={FMP}")
    if isinstance(f,list) and f:
        o,n,y=span([r.get("date") for r in f])
        print(f"  FMP /{ep}: rows={len(f)} span {o} → {n} ({y}y)  fields={sorted(f[0].keys())[:8]}")
    else: print(f"  FMP /{ep}: {f if not isinstance(f,list) else 'empty'}")
b=get(f"{BZ}/ratings?ticker={TK}&order=asc&sort=date&limit=400&apiKey={MASS}")
br=b.get("results") if isinstance(b,dict) else None
if br is not None:
    o,n,y=span([r.get("date") for r in br])
    print(f"  BENZINGA ratings: rows={len(br)} span {o} → {n} ({y}y) next_url={'y' if b.get('next_url') else 'n'}")
else: print("  Benzinga ratings:",b)

print("\n"+"="*70); print("COVERAGE BREADTH (mkt-wide calendar, next 14d)"); print("="*70)
today=datetime.date.today(); d14=(today+datetime.timedelta(days=14)).isoformat()
fc=get(f"{FB}/earnings-calendar?from={today}&to={d14}&apikey={FMP}")
print(f"  FMP earnings-calendar rows={len(fc) if isinstance(fc,list) else fc}")
bc=get(f"{BZ}/earnings?date.gte={today}&date.lte={d14}&order=asc&limit=1000&apiKey={MASS}")
print(f"  BENZINGA earnings rows={len(bc.get('results',[])) if isinstance(bc,dict) else bc}")
print("DONE 1986")
