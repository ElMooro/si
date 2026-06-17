import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
print("invoke:", lam.invoke(FunctionName="justhodl-ecb-history",InvocationType="RequestResponse")["Payload"].read().decode()[:140])
man=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ecb-hist/_manifest.json")["Body"].read())
ids={m["id"] for m in man["series"]}
print("manifest count:", man["n"])
new=["conf_esi","conf_industrial","conf_services","conf_consumer","conf_retail","conf_construction",
     "indprod_total_yoy","indprod_core_yoy","indprod_intermediate_yoy","indprod_capital_yoy",
     "indprod_durable_yoy","indprod_nondurable_yoy","indprod_energy_yoy","real_m1_growth"]
for sid in new:
    m=next((x for x in man["series"] if x["id"]==sid),None)
    print(f"  {sid:26} {'OK '+str(m['latest'])+' ('+m['latest_date']+') n='+str(m['n_points'])+' '+m['first_date'][:4] if m else 'MISSING'}")
