import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def g(k): return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
pt=g("data/political-trades.json")
print("gen:",pt.get("generated_at"),"stats:",json.dumps(pt.get("stats"))[:200])
for fld in ["clusters_top_10","large_trades_top_15","trades_recent_50","high_watch_recent_15"]:
    v=pt.get(fld)
    if isinstance(v,list) and v: print(f"{fld}[0]:",json.dumps(v[0])[:320])
ps=g("data/political-stocks.json"); cg=ps.get("congress")
print("\npolitical-stocks.congress type:",type(cg).__name__)
if isinstance(cg,list) and cg: print(" congress[0]:",json.dumps(cg[0])[:300])
elif isinstance(cg,dict): 
    print(" congress keys:",list(cg.keys())[:10])
    for kk in list(cg.keys())[:2]:
        print("  ",kk,"->",json.dumps(cg[kk])[:200])
print("DONE 2511")
