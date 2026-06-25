import boto3, json
s3=boto3.client("s3","us-east-1")
def probe(f):
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=f"data/{f}.json")["Body"].read())
        print(f"\n{f}: keys={list(d.keys())[:12]}")
        for k,v in d.items():
            if isinstance(v,list) and v and isinstance(v[0],dict) and ("ticker" in v[0] or "symbol" in v[0]):
                print(f"    list '{k}' n={len(v)} item-keys={list(v[0].keys())[:8]}")
            if isinstance(v,dict) and any(isinstance(x,list) for x in v.values()):
                subs={sk:(len(sv) if isinstance(sv,list) else type(sv).__name__) for sk,sv in v.items()}
                print(f"    dict '{k}' subs={list(subs.items())[:6]}")
        # classification values for convexity
        if "convex" in f:
            cls=set(r.get("classification") for r in (d.get("scores") or [])[:80])
            print(f"    classification values: {cls}")
    except Exception as e: print(f"{f}: ERR {str(e)[:45]}")
for f in ["convexity-scores","consensus-bottom","capitulation","cta-trend-exhaust"]:
    probe(f)
print("\nDONE 2210")
