import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=180,retries={"max_attempts":0}))
lam.invoke(FunctionName="justhodl-finviz-universe",InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/finviz-index-membership.json")["Body"].read())
m=d.get("members",{}); c=d.get("changes",{})
print("MEMBERS:", {k:len(v) for k,v in m.items()})
print("CHANGES (first run = baseline, expect empty):")
for k,v in c.items(): print(f"  {k:12} +{len(v['additions'])} / -{len(v['deletions'])}")
print("sample sp500:", sorted(m.get('sp500',[]))[:8])
print("sample russell2000:", sorted(m.get('russell2000',[]))[:6], "...")
# confirm index-inclusion engine still runs and report its member source
lam.invoke(FunctionName="justhodl-index-inclusion",InvocationType="RequestResponse")
ii=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/index-inclusion.json")["Body"].read())
print("\nindex-inclusion availability:", ii.get("availability") or ii.get("avail"), "| candidates:", len(ii.get("watch") or ii.get("candidates") or []))
# SLA
K="data/_freshness-manifest.json"
man=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=K)["Body"].read())
man.setdefault("key_overrides",{})["data/finviz-index-membership.json"]=14
s3.put_object(Bucket="justhodl-dashboard-live",Key=K,Body=json.dumps(man,indent=2).encode(),ContentType="application/json")
print("index-membership SLA=14h | total finviz overrides:", len([k for k in man['key_overrides'] if 'finviz' in k]))
