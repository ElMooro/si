import boto3, json
s3=boto3.client("s3","us-east-1")
def g(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:50]}
def shape(name,k):
    d=g(k)
    if "_err" in d: print(f"{name}: MISSING {d['_err']}"); return
    print(f"\n{name} [{k}] top-keys:",list(d.keys())[:14] if isinstance(d,dict) else type(d).__name__)
    if isinstance(d,dict):
        for key,v in d.items():
            if isinstance(v,list) and v:
                s=v[0]
                if isinstance(s,dict): print(f"   list '{key}' n={len(v)} item-keys={list(s.keys())[:10]}")
                else: print(f"   list '{key}' n={len(v)} of {type(s).__name__}: {str(v[:3])[:80]}")
            elif isinstance(v,dict) and v:
                ik=list(v.keys())[:5]
                # is it a {ticker: {...}} map?
                fk=ik[0] if ik else None
                print(f"   dict '{key}' n={len(v)} sample-keys={ik}"+(f" (map? first-val-keys={list(v[fk].keys())[:6]})" if isinstance(v.get(fk),dict) else ""))
for n,k in [("options-analytics","data/options-analytics.json"),("dealer-gex","data/dealer-gex.json"),
            ("put-call-extreme","data/put-call-extreme.json"),("options-flow","data/options-flow.json"),
            ("volatility-squeeze","data/volatility-squeeze.json"),("polygon-options-flow","data/polygon-options-flow.json"),
            ("options-gamma","data/options-gamma.json"),("catalyst-skew-premove","data/catalyst-skew-premove.json")]:
    shape(n,k)
print("\nDONE 2148")
