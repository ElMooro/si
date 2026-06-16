import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=180,retries={"max_attempts":0}))
print("invoking finviz-universe (full 151-col pull)...")
r=lam.invoke(FunctionName="justhodl-finviz-universe",InvocationType="RequestResponse")
print("invoke:",r["StatusCode"],r["Payload"].read().decode()[:160])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/finviz-universe.json")["Body"].read())
bt=d.get("by_ticker",{}); n=len(bt)
keys=["index_membership","eps_surprise","rev_surprise","ev_ebitda","ev_sales","float_pct","off_ath_pct","income","sales","employees","book_sh","div_ttm","eps_yoy_ttm","aum","expense_ratio","flows_1m","n_holdings","ret_1y","asset_type"]
print(f"tickers={n} | new-field coverage:")
for k in keys:
    c=sum(1 for r in bt.values() if r.get(k) is not None)
    print(f"  {k:18} {c:6}/{n}  ({100*c//max(n,1)}%)")
print("\nAAPL index:", bt.get("AAPL",{}).get("index_membership"), "| eps_surprise:", bt.get("AAPL",{}).get("eps_surprise"), "| ev_ebitda:", bt.get("AAPL",{}).get("ev_ebitda"))
# an ETF sample for flows/aum
for etf in ["SPY","XLK","QQQ"]:
    e=bt.get(etf,{}); print(f"{etf}: aum={e.get('aum')} expense={e.get('expense_ratio')} flows_1m={e.get('flows_1m')} holdings={e.get('n_holdings')} type={e.get('asset_type')}")
