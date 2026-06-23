import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=600,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
def get(k): return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
# show a real thesis record shape
th=get("data/kill-theses.json").get("theses",[])
print("thesis[0] keys:",list(th[0].keys()) if th else "none","| symbols:",[t.get("symbol") for t in th[:8]])
for _ in range(30):
    c=lam.get_function(FunctionName="justhodl-best-setups")["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(3)
lam.invoke(FunctionName="justhodl-best-setups",InvocationType="RequestResponse")
b=get("data/best-setups.json")
print("meta_intelligence:",json.dumps(b.get("meta_intelligence",{})))
for s in b.get("picks_with_kill_thesis",[])[:6]:
    print(f"   KILL {s['ticker']:<6} conv={s.get('conviction')}: {(s.get('failure_mode') or '')[:95]}")
print("DONE 2129")
