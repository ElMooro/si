import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=180,retries={"max_attempts":0}))
lam.invoke(FunctionName="justhodl-finviz-universe",InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/finviz-etf-flows.json")["Body"].read())
print(f"n_etfs={d.get('n_etfs')}")
print("SECTOR ETF net flows (1M, $):")
for x in d.get("sector_etfs",[]):
    f=x.get('flows_1m'); print(f"  {x['ticker']} {x['sector']:22} 1M={'+' if (f or 0)>=0 else ''}{(f or 0)/1e9:.2f}B  AUM={x.get('aum',0)/1e9:.0f}B  YTD={(x.get('flows_ytd') or 0)/1e9:+.1f}B")
print("\nTOP 5 inflows:", [(x['ticker'],round((x['flows_1m'] or 0)/1e9,1)) for x in d.get("top_inflows",[])[:5]])
print("TOP 5 outflows:", [(x['ticker'],round((x['flows_1m'] or 0)/1e9,1)) for x in d.get("top_outflows",[])[:5]])
