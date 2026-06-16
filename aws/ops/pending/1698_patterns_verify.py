import json, boto3
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=240,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name="us-east-1")
r=lam.invoke(FunctionName="justhodl-finviz-signals",InvocationType="RequestResponse")
print("invoke:",r["StatusCode"])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/finviz-signals.json")["Body"].read())
c=d.get("counts",{})
print("total screens:",len(c))
for k in ["double_bottom","double_top","inverse_hs","head_shoulders","multiple_bottom","multiple_top","channel_up","channel_down"]:
    print(f"  {k:16} {c.get(k,'MISSING')}")
db=d["signals"].get("double_bottom",[])[:5]
print("double_bottom sample:", [(x.get("ticker"),x.get("perf_m"),x.get("rel_volume")) for x in db])
dt=d["signals"].get("double_top",[])[:5]
print("double_top sample:", [(x.get("ticker"),x.get("perf_m")) for x in dt])
