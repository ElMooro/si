import boto3, json, re
s3=boto3.client("s3","us-east-1")
def probe(f, note=""):
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=f"data/{f}.json")["Body"].read())
        print(f"\n{f} {note}: keys={list(d.keys())[:12]}")
        head={k:v for k,v in d.items() if isinstance(v,(int,float,str,bool)) and re.search(r'score|signal|regime|state|level|carry|stress',k,re.I)}
        if head: print(f"    headline={json.dumps(head)[:130]}")
        for k,v in d.items():
            if isinstance(v,list) and v and isinstance(v[0],dict) and ("ticker" in v[0] or "symbol" in v[0] or "currency" in v[0] or "country" in v[0] or "pair" in v[0]):
                ik=list(v[0].keys())
                cand=[x for x in ik if any(w in x.lower() for w in ("score","yield","buyback","carry","si","short","dtc","squeeze","rate","real","return","pct"))]
                print(f"    '{k}' n={len(v)} keys={ik[:8]} scoreish={cand[:6]}")
            if isinstance(v,dict) and v and "carry" in f:
                print(f"    dict '{k}' subkeys={list(v.keys())[:8]}")
    except Exception as e: print(f"{f}: ERR {str(e)[:45]}")
probe("buyback-yield-ranking","(flow-confluence buyback)")
probe("earnings-iv-crush","(options-confluence)")
probe("carry-surface","(hot-money carry axis)")
probe("short-interest","(accumulation-radar squeeze fuel)")
print("\nDONE 2207")
