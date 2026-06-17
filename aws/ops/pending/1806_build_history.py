import json, boto3
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name="us-east-1")
lam.invoke(FunctionName="justhodl-settlement-fails",InvocationType="RequestResponse")
print("plumbing invoke:", lam.invoke(FunctionName="justhodl-plumbing-aggregator",InvocationType="RequestResponse")["Payload"].read().decode()[:120])
h=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/plumbing-history.json")["Body"].read())
inds=h["indicators"]
print("history indicators:",len(inds),"| crises:",len(h["crises"]),"| bytes:",s3.head_object(Bucket="justhodl-dashboard-live",Key="data/plumbing-history.json")["ContentLength"])
for k,v in sorted(inds.items(), key=lambda x:x[1]["start"]):
    print(f"  {k:18} {v['layer']:3} n={v['n']:4} {v['start']}..{v['end']}  {(v['label'] or '')[:32]}")
sf=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/settlement-fails.json")["Body"].read())
he=next(c for c in sf["classes"] if c["key"]=="ust_ex_tips")
print("\nsettlement-fails ust ftd n:",len(he["ftd"]),"range:",he["ftd"][0][0],"..",he["ftd"][-1][0])
