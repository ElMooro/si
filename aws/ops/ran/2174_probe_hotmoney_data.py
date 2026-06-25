import boto3, json, urllib.request
s3=boto3.client("s3","us-east-1"); FMP="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
def s3get(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:40]}
def shape(name,k,listhint=None):
    d=s3get(k)
    if "_err" in d: print(f"{name}: MISSING {d['_err']}"); return
    print(f"\n{name} [{k}] keys: {list(d.keys())[:12]}")
    for key,v in d.items():
        if isinstance(v,list) and v and isinstance(v[0],dict):
            print(f"   list '{key}' n={len(v)} item-keys={list(v[0].keys())[:11]}")
        elif isinstance(v,dict) and v:
            fk=list(v.keys())[0]
            if isinstance(v.get(fk),dict): print(f"   map '{key}' n={len(v)} sample-key={fk} val-keys={list(v[fk].keys())[:9]}")
shape("etf-fund-flows","data/etf-fund-flows.json")
shape("etf-true-flows","data/etf-true-flows.json")
shape("global-markets","data/global-markets.json")
shape("fx-regime","data/fx-regime.json")
shape("fx-regime2","data/polygon-fx-regime.json")
# FMP /stable/ ETF drill-down probes
def fmp(path):
    try:
        u=f"https://financialmodelingprep.com/stable/{path}{'&' if '?' in path else '?'}apikey={FMP}"
        return json.loads(urllib.request.urlopen(u,timeout=30).read())
    except Exception as e: return {"_err":str(e)[:50]}
print("\n--- FMP /stable/ ETF drill-down (EWZ Brazil) ---")
for p in ["etf/holdings?symbol=EWZ","etf-holdings?symbol=EWZ","etf/sector-weightings?symbol=EWZ","etf/info?symbol=EWZ","etf/country-weightings?symbol=EWZ"]:
    r=fmp(p)
    if isinstance(r,dict) and "_err" in r: print(f"  {p}: ERR {r['_err']}")
    elif isinstance(r,list): print(f"  {p}: OK list n={len(r)} keys={list(r[0].keys())[:8] if r else '[]'}")
    else: print(f"  {p}: OK {type(r).__name__} keys={list(r.keys())[:8] if isinstance(r,dict) else ''}")
print("DONE 2174")
