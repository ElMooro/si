import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
for _ in range(25):
    c=lam.get_function(FunctionName="justhodl-best-setups")["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(3)
# are the orphan feeds populated?
for f in ["data/equity-confluence.json","data/resilience.json","data/strategist.json"]:
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=f)["Body"].read())
        n=len(d.get("confluence_book") or d.get("top_picks") or d.get("picks") or [])
        print(f"  FEED {f}: book_size={n} mode={d.get('mode','-')}")
    except Exception as e: print(f"  FEED {f}: {str(e)[:40]}")
lam.invoke(FunctionName="justhodl-best-setups",InvocationType="RequestResponse")
b=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/best-setups.json")["Body"].read())
om=b.get("orphan_meta_wiring",{})
print("\norphan_meta_wiring:",json.dumps(om))
for s in b.get("meta_confluence_book",[])[:5]:
    print(f"   CONFLUENCE {s['ticker']:<6} conv={s.get('conviction')} {s.get('meta_confluence')}")
for s in b.get("resilient_setups",[])[:5]:
    print(f"   RESILIENT  {s['ticker']:<6} conv={s.get('conviction')} flags={s.get('resilience',{}).get('flags')}")
print("DONE 2146")
