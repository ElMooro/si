import boto3, json, urllib.request, time
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
FMP="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
def gj(u,tries=2):
    for _ in range(tries):
        try:
            with urllib.request.urlopen(u,timeout=25) as r: return r.read()
        except Exception as e: time.sleep(1); err=str(e)[:60]
    return None

print("=== data/ files that could be a bulk pre-filter (universe/finviz/screener/financials) ===")
keys=[o["Key"] for o in s3.list_objects_v2(Bucket=B,Prefix="data/").get("Contents",[])]
for k in sorted(keys):
    if any(x in k.lower() for x in ("universe","finviz","screen","bulk","fundamental","financ","valuation","metrics","profile")):
        try:
            sz=s3.head_object(Bucket=B,Key=k)["ContentLength"]; print(f"  {k}  ({sz//1024}KB)")
        except Exception: print(f"  {k}")

def peek(k):
    try:
        d=json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
        if isinstance(d,dict):
            # find the main list
            lst=next(((kk,v) for kk,v in d.items() if isinstance(v,list) and v and isinstance(v[0],dict)),None)
            print(f"\n[{k}] top keys: {list(d.keys())[:10]}")
            if lst:
                kk,v=lst; print(f"  list '{kk}' n={len(v)}; item fields: {list(v[0].keys())[:30]}")
                # does an item have margin/cap/sector?
                it=v[0]; rel={f:it.get(f) for f in it if any(x in f.lower() for x in ('margin','marketcap','market_cap','sector','industry','symbol','ticker','grossprofit','revenue'))}
                print(f"  sample relevant: {json.dumps(rel,default=str)[:300]}")
        elif isinstance(d,list) and d:
            print(f"\n[{k}] LIST n={len(d)}; item fields: {list(d[0].keys())[:30]}")
    except Exception as e: print(f"\n[{k}] err: {str(e)[:60]}")

for k in ["data/universe.json","data/finviz-universe.json","data/finviz-screener.json","data/finviz.json","data/stock-valuations.json"]:
    if k in keys: peek(k)

print("\n\n=== FMP BULK endpoint probe (one call -> margins for thousands?) ===")
for url,label in [
    (f"https://financialmodelingprep.com/stable/profile-bulk?part=0&apikey={FMP}","stable/profile-bulk"),
    (f"https://financialmodelingprep.com/api/v4/ratios-bulk?year=2025&period=annual&apikey={FMP}","v4/ratios-bulk"),
    (f"https://financialmodelingprep.com/api/v4/key-metrics-bulk?year=2025&period=annual&apikey={FMP}","v4/key-metrics-bulk"),
    (f"https://financialmodelingprep.com/stable/company-screener?marketCapMoreThan=300000000&marketCapLowerThan=50000000000&limit=20&apikey={FMP}","stable/company-screener"),
]:
    r=gj(url)
    if r is None: print(f"  {label}: FAILED/empty"); continue
    head=r[:200].decode("utf-8","replace").replace("\n"," ")
    print(f"  {label}: {len(r)} bytes | head: {head[:160]}")
print("DONE 2119")
