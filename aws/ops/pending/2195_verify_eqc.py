import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
for _ in range(25):
    c=lam.get_function(FunctionName="justhodl-equity-confluence")["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
lam.invoke(FunctionName="justhodl-equity-confluence",InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/equity-confluence.json")["Body"].read())
es=d.get("engines_seen") or d.get("sources") or []
fams=set()
for e in es:
    if isinstance(e,dict): fams.add(e.get("family"))
print("engines_seen families:", sorted(f for f in fams if f))
print("has value family:", "value" in fams, "| has insider family:", "insider" in fams)
# count names lit by value / insider
board=d.get("research_book") or d.get("board") or d.get("provisional_book") or d.get("names") or []
def cnt(famname):
    n=0
    for r in board:
        f=r.get("families") or r.get("families_lit") or {}
        keys=f.keys() if isinstance(f,dict) else (f if isinstance(f,list) else [])
        if famname in keys: n+=1
    return n
print("board size:", len(board), "| names with value:", cnt("value"), "| names with insider:", cnt("insider"))
print("diag tail:", (d.get("diag") or [])[-6:])
print("DONE 2195")
