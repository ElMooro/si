import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=120,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
def get(k): return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
for _ in range(30):
    c=lam.get_function(FunctionName="justhodl-premortem-engine")["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(3)
lam.invoke(FunctionName="justhodl-premortem-engine",InvocationType="Event")
print("async invoke sent; polling for real theses (up to ~280s)...")
real=0
for w in range(28):
    time.sleep(10)
    try:
        kt=get("data/kill-theses.json"); th=kt.get("theses",[]); real=sum(1 for t in th if not t.get("error"))
        if w%3==0 or real>0: print(f"  t+{(w+1)*10}s: real={real}/{len(th)} gen={kt.get('generated_at','')[:19]}")
        if real>=3: break
    except Exception as e: print("  err",str(e)[:30])
kt=get("data/kill-theses.json"); th=[t for t in kt.get("theses",[]) if not t.get("error")]
print(f"\nREAL kill-theses: {len(th)} (gen {kt.get('generated_at','')[:19]})")
for t in th[:6]: print(f"   {t.get('symbol'):<6} {(t.get('thesis_summary') or '')[:92]}")
if th:
    boto3.client("lambda","us-east-1",config=Config(read_timeout=300)).invoke(FunctionName="justhodl-best-setups",InvocationType="RequestResponse")
    b=get("data/best-setups.json")
    print("\nbest-setups meta_intelligence:",json.dumps(b.get("meta_intelligence",{})))
    for s in b.get("picks_with_kill_thesis",[])[:6]:
        print(f"   KILL {s['ticker']:<6} conv={s.get('conviction')}: {(s.get('failure_mode') or '')[:82]}")
print("DONE 2133")
