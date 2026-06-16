import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=120,retries={"max_attempts":0}))
lam.invoke(FunctionName="justhodl-finviz-groups",InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/finviz-groups.json")["Body"].read())
print("counts:", d.get("counts"))
print("\nSECTORS (perf + valuation):")
for x in d.get("sectors",[]):
    print(f"  {x['name']:24} 1M={x.get('perf_m')}% YTD={x.get('perf_ytd')}% | P/E={x.get('pe')} PEG={x.get('peg')} P/S={x.get('ps')}")
ind=d.get("industries",[])
print(f"\nTOP 6 INDUSTRIES (1M): "+", ".join(f"{x['name']}={x.get('perf_m')}%" for x in ind[:6]))
print(f"BOTTOM 4 INDUSTRIES: "+", ".join(f"{x['name']}={x.get('perf_m')}%" for x in ind[-4:]))
co=d.get("countries",[])
print(f"\nTOP 5 COUNTRIES (1M): "+", ".join(f"{x['name']}={x.get('perf_m')}%" for x in co[:5]))
print("MKTCAP buckets:", ", ".join(f"{x['name']}={x.get('perf_m')}%" for x in d.get("mktcaps",[])))
