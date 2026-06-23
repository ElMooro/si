import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=480,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
for _ in range(20):
    c=lam.get_function(FunctionName="justhodl-market-internals")["Configuration"]
    if c.get("LastUpdateStatus")=="Successful": break
    time.sleep(3)
# confirm SPY/QQQ exist in grouped feed (debug)
import urllib.request
KEY="zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
lam.invoke(FunctionName="justhodl-market-internals",InvocationType="RequestResponse")
mi=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/market-internals.json")["Body"].read())
print("=== MARKET-INTERNALS (after refresh fix) ===")
print("latest:",json.dumps(mi.get("latest",{})))
print("volume:",json.dumps({k:mi.get("volume",{}).get(k) for k in ('up_dollar_vol','down_dollar_vol','ratio','funded_advance')}))
print("rotation:",json.dumps({k:mi.get("rotation",{}).get(k) for k in ('state','spy_ret_pct','qqq_ret_pct','ad_ratio','pct_advancers','vol_ratio','funded')}))
print("note:",mi.get("rotation",{}).get("note"))
print("DONE 2157")
