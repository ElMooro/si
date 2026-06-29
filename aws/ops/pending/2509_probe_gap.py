import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def g(k): return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
print("===== political-stocks.json (congress) =====")
try:
    p=g("data/political-stocks.json"); print("keys:",list(p.keys())[:14])
    for fld in ["recent_buys","recent_sells","top_buys","top_sells","by_ticker","trades","most_bought","most_sold","leaderboard","tickers"]:
        v=p.get(fld)
        if isinstance(v,list) and v: print(f" .{fld}[0]:",json.dumps(v[0])[:300]); 
        elif isinstance(v,dict) and v:
            k0=list(v.keys())[:1]; print(f" .{fld}{{}}:",json.dumps({k0[0]:v[k0[0]]})[:240])
except Exception as e: print("ERR",str(e)[:80])
print("\n===== political-trades.json =====")
try:
    pt=g("data/political-trades.json"); print("keys:",list(pt.keys())[:12])
    for fld in ["trades","recent","by_ticker","top"]:
        v=pt.get(fld)
        if isinstance(v,list) and v: print(f" .{fld}[0]:",json.dumps(v[0])[:280]);break
except Exception as e: print("ERR",str(e)[:80])
print("DONE 2509")
