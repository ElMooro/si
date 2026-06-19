import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
for k in ["data/polygon-options-flow.json","data/dealer-gex.json","data/options-flow.json"]:
    try:
        d=json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
        print("\n%s top-level keys: %s"%(k,list(d.keys())[:20]))
        if k.endswith("polygon-options-flow.json"):
            for kk in ["extreme","bullish","notable","results","alerts","tickers","flows"]:
                v=d.get(kk)
                if isinstance(v,list): print("   %s: %d items; sample=%s"%(kk,len(v), (v[0] if v else {})))
        if k.endswith("dealer-gex.json"):
            sc=d.get("squeeze_candidates",[]); print("   squeeze_candidates: %d"%len(sc)); 
            if sc: print("   sample:",sc[0])
            print("   market_composite keys:",list((d.get("market_composite") or {}).keys())[:12])
    except Exception as e: print(k,"ERR",str(e)[:50])
