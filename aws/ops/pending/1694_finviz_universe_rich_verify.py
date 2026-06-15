import json, boto3, time
s3=boto3.client("s3",region_name="us-east-1"); lam=boto3.client("lambda",region_name="us-east-1")
t=time.time()
r=lam.invoke(FunctionName="justhodl-finviz-universe", InvocationType="RequestResponse")
pl=json.loads(r["Payload"].read().decode())
print("invoke body:", pl.get("body"), "| wall=%.1fs"%(time.time()-t))
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/finviz-universe.json")["Body"].read())
bt=d.get("by_ticker",{})
print("universe tickers:", len(bt))
fields=["roe","roic","profit_margin","gross_margin","analyst_recom","target_price","rsi","sma200_pct","sma50_pct","perf_m","perf_y","off_52w_high_pct","rel_volume","short_float_pct","eps_growth_ny","debt_eq","div_yield","earnings_date"]
for f in fields:
    print(f"  {f:18} {sum(1 for r in bt.values() if r.get(f) is not None):>6} / {len(bt)}")
for tk in ["NVDA","AAPL","MU"]:
    r=bt.get(tk,{})
    print(f"\n{tk}: recom={r.get('analyst_recom')} tgt={r.get('target_price')} px={r.get('price')} rsi={r.get('rsi')} sma200%={r.get('sma200_pct')} perfM={r.get('perf_m')} roe={r.get('roe')} pmargin={r.get('profit_margin')} short={r.get('short_float_pct')}")
