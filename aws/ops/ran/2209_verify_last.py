import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=890,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
def ready(fn):
    for _ in range(28):
        c=lam.get_function(FunctionName=fn)["Configuration"]
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): return
        time.sleep(3)
ready("justhodl-options-confluence"); lam.invoke(FunctionName="justhodl-options-confluence",InvocationType="RequestResponse")
o=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/options-confluence.json")["Body"].read())
iv=[r for r in (o.get("multi_engine_confluence") or []) if any("earnings" in t for t in (r.get("tags") or []))]
print(f"options-confluence earnings-IV tagged: {len(iv)} (QUIET off-season expected)")
ready("justhodl-hot-money"); lam.invoke(FunctionName="justhodl-hot-money",InvocationType="RequestResponse")
h=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/hot-money.json")["Body"].read())
wc=[c for c in (h.get("all_countries") or []) if c.get("carry_pct") is not None]
print(f"hot-money countries with carry: {len(wc)}")
for c in sorted(wc,key=lambda x:-(x.get('carry_pct') or 0))[:8]:
    print(f"  {c['country']:<13} carry {c.get('carry_pct')}% {c.get('carry_signal')} | conviction {c.get('conviction')}")
print("DONE 2209")
