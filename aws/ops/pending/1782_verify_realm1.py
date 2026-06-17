import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
print("invoke:", lam.invoke(FunctionName="justhodl-ecb-history",InvocationType="RequestResponse")["Payload"].read().decode()[:120])
man=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ecb-hist/_manifest.json")["Body"].read())
print("manifest n:", man["n"])
for sid in ["real_m1_growth","conf_esi","indprod_total_yoy"]:
    m=next((x for x in man["series"] if x["id"]==sid),None)
    print(f"  {sid:18}", f"OK {m['latest']} ({m['latest_date']}) n={m['n_points']} {m['first_date'][:4]}" if m else "MISSING")
# confirm the SIGHIST targets all exist in manifest
ids={m["id"] for m in man["series"]}
targets=["ciss_ea","euribor_ois_bp","estr","nfc_loans_yoy","wages_negotiated","spf_longterm","t2_de_minus_it","ilm_mp_lending","ilm_usd_claims","it_de_10y_bp","bank_rate_nfc","esi","unemployment_ea","indprod_total_yoy","conf_esi","real_m1_growth"]
miss=[t for t in targets if t not in ids]
print("SIGHIST targets missing from manifest:", miss or "NONE — all dump signals now chartable")
