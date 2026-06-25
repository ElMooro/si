import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=400,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
for _ in range(25):
    c=lam.get_function(FunctionName="justhodl-flow-confluence")["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
lam.invoke(FunctionName="justhodl-flow-confluence",InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/flow-confluence.json")["Body"].read())
book=d.get("multi_engine_confluence") or []
newsrc={"insider","buyback","insider-buyback"}
n_ins=sum(1 for r in book if any(e in newsrc for e in (r.get("engines") or [])))
print(f"flow-confluence names={len(book)} | with insider/buyback flow: {n_ins}")
for r in book:
    eng=r.get("engines") or []
    if any(e in newsrc for e in eng):
        print(f"  {r['ticker']:<6} score {r.get('score')} posture {r.get('posture')} engines={eng}")
        if n_ins and len([x for x in book if any(e in newsrc for e in (x.get('engines') or []))][:6])>=6: break
print("DONE 2204")
