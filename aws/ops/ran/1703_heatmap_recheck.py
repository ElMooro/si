import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=120,retries={"max_attempts":0}))
lam.invoke(FunctionName="justhodl-finviz-universe",InvocationType="RequestResponse")
h=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/finviz-heatmap.json")["Body"].read())
print("cap-weighted sector heatmap (sane now?):")
for x in h.get("sectors",[]):
    print(f"  {x['sector']:24} n={x['n']:4} 1M(capW)={x['avg_perf_m']:+6.2f}%  median={x.get('median_perf_m')}  ${x['total_mktcap_b']/1000:.1f}T")
