import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=180,retries={"max_attempts":0}))
lam.invoke(FunctionName="justhodl-finviz-universe",InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/finviz-earnings-surprise.json")["Body"].read())
print(f"n={d.get('n')}")
print("TOP BEATS (rev surp / eps surp):", [(x['ticker'],x.get('rev_surprise'),x.get('eps_surprise')) for x in d.get("top_beats",[])[:6]])
print("TOP MISSES:", [(x['ticker'],x.get('rev_surprise')) for x in d.get("top_misses",[])[:6]])
