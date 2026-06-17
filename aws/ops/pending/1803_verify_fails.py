import json, boto3
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=180,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name="us-east-1")
print("invoke:", lam.invoke(FunctionName="justhodl-settlement-fails",InvocationType="RequestResponse")["Payload"].read().decode()[:300])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/settlement-fails.json")["Body"].read())
print("\nas_of:",d["as_of"],"| signal:",d["signal"]["regime"],d["signal"]["score"])
for x in d["signal"]["drivers"]: print("  -",x)
print("\nheadline:",{k:d["headline"][k] for k in ("ftd_bn","ftr_bn","combined_bn","z","pctile","max_bn")})
print("classes:")
for c in d["classes"]: print(f"  {c['label']:26} FTD={c['ftd_latest']} FTR={c['ftr_latest']} comb={c['stats'].get('latest')} {c['stats'].get('pctile')}%ile z{c['stats'].get('z')} n={c['stats'].get('n_obs')} {c['stats'].get('start')}..{c['stats'].get('as_of')}")
print("totals combined latest:", d["totals"]["combined"][-1] if d["totals"]["combined"] else None, "| n:", len(d["totals"]["combined"]))
