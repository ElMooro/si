import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=600,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
t=time.time()
r=lam.invoke(FunctionName="justhodl-resilience",InvocationType="RequestResponse")
print(f"invoke ({time.time()-t:.0f}s):",r["Payload"].read().decode()[:240])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/resilience.json")["Body"].read())
print("\ndiagnostics:")
for x in d.get("diagnostics",[]): print("  ",x)
print("\ncounts:",d.get("counts"),"| universe",d.get("universe_size"))
print("\n🚀 ABOUT TO BOOM (coiled/igniting):")
for r in d.get("about_to_boom",[])[:14]:
    print(f"  {r['ticker']:<6} {r['stage']:<9} res {r['resilience']} | +{r['mean_abnormal_on_adverse_pct']}%/adv ({r['adverse_hit_rate_pct']}% held, n{r['n_adverse_days']}) | asym {r['beta_asymmetry']} | {r['pct_of_60d_high']}% of 60d hi | 20d {r['ret_20d_pct']}% | cats {r['negative_catalysts_shrugged']}")
print("\ntop ABSORBING:")
for r in [x for x in d.get("all_resilient",[]) if x["stage"]=="ABSORBING"][:8]:
    print(f"  {r['ticker']:<6} res {r['resilience']} +{r['mean_abnormal_on_adverse_pct']}%/adv n{r['n_adverse_days']} asym {r['beta_asymmetry']}")
print("\ntop_picks→harvester:",[p["ticker"] for p in d.get("top_picks",[])])
print("DONE 2092")
