import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
print("invoke:", lam.invoke(FunctionName="justhodl-ecb-history",InvocationType="RequestResponse")["Payload"].read().decode()[:120])
ok=miss=0
for sid in ["fx_claims_nonea","ilm_usd_claims","unemp_ea_youth","unemp_de","unemp_fr","unemp_it","unemp_es","unemp_gr","unemp_fi","indprod_intermediate","indprod_capital","indprod_durable","indprod_nondurable","indprod_energy","manuf_turnover","retail_turnover"]:
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=f"data/ecb-hist/{sid}.json")["Body"].read())
        print(f"  {sid:22} freq={d.get('freq'):8} n={d.get('n_points'):5} latest={d.get('latest')} {d.get('latest_date')} pctile={d.get('percentile')}"); ok+=1
    except Exception: print(f"  {sid:22} MISSING (likely 404 code)"); miss+=1
print(f"\nOK={ok} MISSING={miss}")
