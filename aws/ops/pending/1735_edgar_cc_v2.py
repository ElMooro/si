import json, time, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
before=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/edgar-authority.json")["Body"].read()).get("generated_at")
lam.invoke(FunctionName="justhodl-edgar-authority",InvocationType="Event")
d=None
for i in range(13):
    time.sleep(20)
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/edgar-authority.json")["Body"].read())
    if d.get("generated_at")!=before: break
cc=d.get("crosscheck",{})
print(f"elapsed={d.get('elapsed_s')}s | net-nets credible={d.get('n_net_nets')} classic={d.get('n_classic_net_nets')}")
print(f"CROSS-CHECK checked={cc.get('n_checked')} clean={cc.get('n_clean')} CONFIRMED-flagged={cc.get('n_flagged')} unverified={cc.get('n_unverified')}")
print("CONFIRMED FMP divergences (worth a 10-K look):")
for c in cc.get("flagged",[]):
    print(f"  {c['ticker']:6} fy{c['fy']} {c['flags']}  filing_ni=${(c['filing']['net_income'] or 0)/1e6:.0f}M fmp_ni=${(c['fmp']['net_income'] or 0)/1e6:.0f}M")
print("unverified (concept/period mismatch, not errors):", [u["ticker"] for u in cc.get("unverified",[])])
print("clean sample:", cc.get("sample_clean",[])[:14])
