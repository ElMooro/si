import boto3, json, time, urllib.request
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=400,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
for _ in range(30):
    c=lam.get_function(FunctionName="justhodl-crypto-ma200")["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
lam.invoke(FunctionName="justhodl-crypto-ma200",InvocationType="RequestResponse")
cm=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-ma200.json")["Body"].read())
print("crypto-ma200 alpha_status:",json.dumps(cm.get("alpha_status",{})))
print("signals_logged:",cm.get("signals_logged"),"counts:",json.dumps(cm.get("counts",{})))
# page live + shows grade status
time.sleep(110)
try:
    r=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/ma200-radar.html",headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=15)
    b=r.read().decode("utf-8","ignore")
    print("page:",r.getcode(),"has excess-vs-BTC grade line:", "excess-vs-BTC" in b)
except Exception as e: print("page:",str(e)[:50])
print("DONE 2176")
