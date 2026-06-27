import boto3, json
s3=boto3.client("s3","us-east-1")
def gj(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception as e: return {"_ERR":str(e)[:50]}
print("=== crypto-funding.json ===")
d=gj("data/crypto-funding.json")
print("keys:",[k for k in d.keys()][:25])
for k in ("regime","signal","verdict","aggregate","vw_funding_pct","median_funding_pct","funding_dispersion","total_oi_usd","summary","headline","squeeze_risk","posture","market"):
    if k in d: print(f"  {k}: {json.dumps(d[k])[:200]}")
# look for a coins/by_coin list
for k in d.keys():
    if isinstance(d[k],list) and d[k] and isinstance(d[k][0],dict):
        print(f"  list '{k}'[0]:",json.dumps(d[k][0])[:200]); break
print("\n=== crypto-liquidity.json ===")
d=gj("data/crypto-liquidity.json")
print("keys:",[k for k in d.keys()][:25])
for k in ("regime","verdict","ssr","stablecoin_supply_ratio","stablecoin_dominance","fear_greed","fng","posture","headline","signal","composite"):
    if k in d: print(f"  {k}: {json.dumps(d[k])[:240]}")
print("DONE 2363")
