import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
for k in ["data/vol-surface.json","data/skew-tail-hedging.json","data/catalyst-skew-premove.json"]:
    try:
        d=json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
        print("\n%s\n  keys: %s"%(k,list(d.keys())[:22]))
        for kk in ["composite_stress_score","stress_score","regime","vix","iv_rank","iv_percentile","term_structure",
                   "skew","put_skew","skew_25d","tail_signal","verdict","summary","signals","front_iv","spot_vol"]:
            if kk in d: print("   %s = %s"%(kk, json.dumps(d[kk])[:160]))
    except Exception as e: print(k,"ERR",str(e)[:60])
# gamma regime already known; confirm market_composite
g=json.loads(s3.get_object(Bucket=B,Key="data/dealer-gex.json")["Body"].read()).get("market_composite",{})
print("\ndealer-gex market_composite:",json.dumps(g)[:300])
