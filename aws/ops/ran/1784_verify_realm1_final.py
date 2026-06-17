import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
print("invoke:", lam.invoke(FunctionName="justhodl-ecb-history",InvocationType="RequestResponse")["Payload"].read().decode()[:120])
man=json.loads(s3.get_object(Bucket=B,Key="data/ecb-hist/_manifest.json")["Body"].read())
ids={m["id"] for m in man["series"]}
print("manifest n:", man["n"], "| real_m1_growth in manifest:", "real_m1_growth" in ids)
try:
    d=json.loads(s3.get_object(Bucket=B,Key="data/ecb-hist/real_m1_growth.json")["Body"].read())
    p=d["points"]; print(f"  real_m1_growth FILE OK: {d['first_date']}→{d['latest_date']} n={d['n_points']} latest={d['latest']}")
    print("  head:",p[:2],"tail:",p[-2:])
except Exception as e: print("  real_m1_growth file:", type(e).__name__)
# final: confirm ALL 19 dump-signal SIGHIST targets resolve to a present manifest id
SIG={"ciss_acceleration":"ciss_ea","euribor_ois_stress":"euribor_ois_bp","estr_dfr_dislocation":"estr","credit_contraction":"nfc_loans_yoy","wage_persistence":"wages_negotiated","expectations_deanchoring":"spf_longterm","target2_imbalance":"t2_de_minus_it","bank_funding_stress":"ilm_mp_lending","eu_us_liquidity_divergence":"ilm_usd_claims","fragmentation_stress":"it_de_10y_bp","bls_credit_standards":"nfc_loans_yoy","bank_pass_through_premium":"bank_rate_nfc","usd_funding_stress_composite":"ilm_usd_claims","eurodollar_stress_index":"esi","ea_unemployment":"unemployment_ea","ea_industrial_production":"indprod_total_yoy","ea_confidence":"conf_esi","real_m1_growth":"real_m1_growth","country_unemployment":"unemployment_ea"}
miss={k:t for k,t in SIG.items() if t not in ids}
print("\nDUMP SIGNALS still un-chartable:", miss or "NONE — all 19 now resolve to a live history series")
