import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=90,retries={"max_attempts":0}))
lam.invoke(FunctionName="justhodl-finviz-news",InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/finviz-news.json")["Body"].read())
print(f"news={d.get('n_news')} blogs={d.get('n_blogs')} tickers={d.get('n_tickers')}")
print("top mentioned:", ", ".join(f"{x['ticker']}({x['n']})" for x in d.get("top_tickers",[])[:10]))
print("\nsample headlines:")
for it in d.get("news",[])[:4]:
    print(f"  [{it.get('ticker') or 'mkt'}] {it.get('title','')[:80]} — {it.get('source')}")
