import json, boto3
s3=boto3.client("s3",region_name="us-east-1")
uni=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/finviz-universe.json")["Body"].read()).get("by_ticker",{})
for t in ["AAPL","MSFT","F"]:
    r=uni.get(t,{}); print(f"  {t}: market_cap={r.get('market_cap')}  income={r.get('income')}  sales={r.get('sales')}")
