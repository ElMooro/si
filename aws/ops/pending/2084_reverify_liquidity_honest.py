import boto3, json, time, urllib.request
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
for _ in range(20):
    c=lam.get_function(FunctionName="justhodl-crypto-liquidity")["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(3)
lam.invoke(FunctionName="justhodl-crypto-liquidity",InvocationType="RequestResponse")["Payload"].read()
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/crypto-liquidity.json")["Body"].read())
print("regime:",d["regime"],"score",d["liquidity_score"])
print("forecast_supported:",d["forecast_supported"])
print("directional_read:",d["directional_read"],"| top_picks:",d["top_picks"])
print("forecast_support:",d["forecast_support"][:220])
# page live?
def get(u):
    for _ in range(4):
        try:
            with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=20) as r:
                return r.getcode(),r.read().decode("utf-8","replace")
        except Exception: time.sleep(12)
    return None,""
code,b=get("https://justhodl.ai/crypto-liquidity.html?t="+str(int(time.time())))
print("\ncrypto-liquidity.html:",code,"| reads feed:",'data/crypto-liquidity.json' in b)
print("DONE 2084")
