import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=600,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
def get(k): return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
for _ in range(30):
    c=lam.get_function(FunctionName="justhodl-premortem-engine")["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(3)
t=time.time(); r=lam.invoke(FunctionName="justhodl-premortem-engine",InvocationType="RequestResponse")
print("premortem (GLM):",r["Payload"].read().decode()[:160],f"({time.time()-t:.0f}s)")
kt=get("data/kill-theses.json"); th=kt.get("theses",[])
ok=[t for t in th if not t.get("error")]
print(f"kill-theses: {len(th)} total, {len(ok)} REAL (no error)")
for t in ok[:5]:
    print(f"   {t.get('symbol'):<6} {(t.get('thesis_summary') or '')[:95]}")
# re-run best-setups to pick up real theses
for _ in range(20):
    c=lam.get_function(FunctionName="justhodl-best-setups")["Configuration"]
    if c.get("State")=="Active": break
    time.sleep(2)
lam.invoke(FunctionName="justhodl-best-setups",InvocationType="RequestResponse")
b=get("data/best-setups.json")
print("\nbest-setups meta_intelligence:",json.dumps(b.get("meta_intelligence",{})))
for s in b.get("picks_with_kill_thesis",[])[:6]:
    print(f"   KILL {s['ticker']:<6} conv={s.get('conviction')}: {(s.get('failure_mode') or '')[:90]}")
print("DONE 2130")
