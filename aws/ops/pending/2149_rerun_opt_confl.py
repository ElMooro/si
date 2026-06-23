import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=200,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); FN="justhodl-options-confluence"
for _ in range(25):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(3)
lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/options-confluence.json")["Body"].read())
print("counts:",json.dumps(d.get("counts",{})))
print("\nMULTI-ENGINE CONFLUENCE (>=2 options engines agree):")
for b in d.get("multi_engine_confluence",[])[:12]:
    print(f"   {b['ticker']:<6} {b['posture']:<13} score={b['score']:+.2f} engines={b['n_engines']} ({','.join(b['engines'])}) {b.get('tags')}")
print("\nby_posture sample:")
for p,v in d.get("by_posture",{}).items():
    if v: print(f"   {p}: {[x['ticker'] for x in v[:8]]}")
print("DONE 2149")
