import boto3, json
s3=boto3.client("s3","us-east-1")
def probe(f):
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=f"data/{f}.json")["Body"].read())
        print(f"\n{f}: top-keys={[k for k in d.keys()][:12]}")
        # find list-of-dict fields that look like ticker books
        for k,v in d.items():
            if isinstance(v,list) and v and isinstance(v[0],dict) and ("ticker" in v[0] or "symbol" in v[0]):
                keys=list(v[0].keys())
                print(f"    list '{k}' (n={len(v)}) item-keys={keys[:10]}")
    except Exception as e: print(f"{f}: ERR {str(e)[:50]}")
for f in ["quality-on-sale","deep-value","deep-value-overlap","insider-clusters",
          "insider-buyback-confluence","consensus-bottom","convexity-scores"]:
    probe(f)
print("\nDONE 2194")
