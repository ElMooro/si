import boto3, json
s3=boto3.client("s3","us-east-1")
def gj(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception as e: return {"_ERR":str(e)[:50]}
for key in ["data/yield-curve.json","data/treasury-curve.json","data/rates-curve.json","data/yields.json"]:
    d=gj(key)
    if "_ERR" in d: print(key,"MISSING"); continue
    print("\n===",key,"=== keys:",list(d.keys())[:20])
    for k in ("nominal_yields","yields","curve","curve_points","tenors","spreads_bps","real_yields","inflation_expectations","term_premium_proxy_bps","regime","inversion","signal","as_of"):
        if k in d: print(f"  {k}: {json.dumps(d[k])[:240]}")
print("DONE 2358")
