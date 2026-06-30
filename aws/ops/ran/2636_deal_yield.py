"""ops 2636 — current deals + source reliability (FMP pages, Polygon, Benzinga)."""
import urllib.request, json, boto3, time
FMP="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
def g(url,t=25):
    try: return json.loads(urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"jh"}),timeout=t).read())
    except Exception as e: return {"__err__":str(e)[:120]}
s3=boto3.client("s3",region_name="us-east-1")

print("=== CURRENT FEED DEALS ===")
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/deal-scanner.json")["Body"].read())
s=j.get("summary",{})
print("summary:",{k:s.get(k) for k in ['n_prs_scanned','n_deals','n_green','n_yellow','n_ai','n_ai_mega','n_small_cap']})
for d in (j.get("deals") or [])[:20]:
    print(f"  {d['symbol']:6s} hl={str(d.get('highlight')):6s} ai={int(bool(d.get('ai_relevant')))} ${d.get('deal_value_str') or '?':>8s} vsMC={d.get('vs_market_cap_pct')} | {d['title'][:70]}")

print("\n=== FMP press-release pages 0-7 (sequential, deal-language yield) ===")
STRONG=["awarded a contract","wins contract","secures contract","secures order","purchase order","supply agreement","supply contract","contract worth","contract valued","order worth","design win","new contract","contract award","wins deal","lands contract","selected to supply","framework agreement","wins $","awarded $"]
tot=0; deal=0
for p in range(8):
    d=g(f"https://financialmodelingprep.com/stable/news/press-releases-latest?page={p}&limit=100&apikey={FMP}")
    n=len(d) if isinstance(d,list) else 0
    dl=sum(1 for it in (d if isinstance(d,list) else []) if any(k in ((it.get('title') or '')+' '+(it.get('text') or '')[:300]).lower() for k in STRONG))
    tot+=n; deal+=dl
    print(f"  page {p}: {n} items, {dl} deal-language" + ("" if isinstance(d,list) else f" ERR {d}"))
print(f"  TOTAL: {tot} items, {deal} with strong deal-language")

print("\n=== Polygon + Benzinga key health ===")
POLY="zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
pj=g(f"https://api.polygon.io/v2/reference/news?limit=5&apiKey={POLY}")
print("  polygon:", len(pj.get("results",[])) if isinstance(pj,dict) and "results" in pj else pj)
# benzinga key from engine
import re
src=open("aws/lambdas/justhodl-deal-scanner/source/lambda_function.py").read()
bz=re.search(r'BENZINGA_KEY\s*=\s*"([^"]*)"',src)
bzk=bz.group(1) if bz else ""
print("  benzinga key set:", bool(bzk and bzk!="YOUR_KEY" and len(bzk)>5))
if bzk and len(bzk)>5:
    bj=g(f"https://api.benzinga.com/api/v2/news?token={bzk}&pagesize=3&displayOutput=full")
    print("  benzinga resp type:", type(bj).__name__, (len(bj) if isinstance(bj,list) else str(bj)[:80]))
print("DONE 2636")
