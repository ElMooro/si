import boto3, json
s3=boto3.client("s3","us-east-1")
for f in ["options-confluence","flow-confluence"]:
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=f"data/{f}.json")["Body"].read())
        print(f"\n{f}: keys={list(d.keys())[:14]}")
        for k,v in d.items():
            if isinstance(v,list) and v and isinstance(v[0],dict) and ("ticker" in v[0] or "symbol" in v[0]):
                ik=list(v[0].keys())
                cand=[x for x in ik if any(w in x.lower() for w in ("score","posture","composite","conviction","strength","signal","n_"))]
                print(f"    '{k}' n={len(v)} keys={ik[:8]} scoreish={cand[:5]}")
    except Exception as e: print(f"{f}: ERR {str(e)[:40]}")
print("DONE 2200")
