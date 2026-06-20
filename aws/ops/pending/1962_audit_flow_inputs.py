"""1962 — audit flow input schemas for the look-through engine."""
import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
for key in ["data/etf-fund-flows.json","data/capital-flow-radar.json"]:
    try:
        j=json.loads(s3.get_object(Bucket=B,Key=key)["Body"].read())
    except Exception as e:
        print(key,"ERR",e); continue
    print("\n"+"="*60); print(key); print("="*60)
    print("top keys:", list(j.keys())[:15] if isinstance(j,dict) else type(j))
    # find the per-ETF flow list
    for k,v in (j.items() if isinstance(j,dict) else []):
        if isinstance(v,list) and v and isinstance(v[0],dict):
            f0=v[0]
            if any(x in f0 for x in ("ticker","composite_ticker","fund_flow_5d_usd","daily_flow_usd","flow")):
                print(f"  list '{k}' len={len(v)} sample fields:", list(f0.keys())[:14])
                print("   sample:", json.dumps({kk:f0.get(kk) for kk in list(f0.keys())[:10]})[:300])
                break
print("\nDONE 1962")
