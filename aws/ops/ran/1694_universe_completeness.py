import json, boto3
s3=boto3.client("s3",region_name="us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/finviz-universe.json")["Body"].read())
bt=d.get("by_ticker",{})
print("tickers:",len(bt))
# field coverage across universe
from collections import Counter
cov=Counter()
for r in bt.values():
    for k in r: cov[k]+=1
print("distinct fields:",len(cov))
key=["short_float","short_ratio","float_shares","recom","analyst_recom","target_price","roe","roic","roa","rsi","sma200","sma50","sma20","off_52w_high_pct","perf_m","perf_y","rel_volume","avg_volume","beta","atr","inst_own","insider_own","profit_margin","debt_equity","current_ratio","pe","fwd_pe","peg","eps_g_ny","earnings_date"]
n=len(bt)
print("\nfield coverage (have non-null / total):")
for k in key:
    c=sum(1 for r in bt.values() if r.get(k) is not None)
    print(f"  {k:20} {c:6}/{n}  ({100*c//max(n,1)}%)") if c else print(f"  {k:20} MISSING")
print("\nsample AAPL:", json.dumps(bt.get("AAPL",{}),default=str)[:600])
