import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=600,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
try:
    for _ in range(20):
        c=lam.get_function(FunctionName="justhodl-resilience")["Configuration"]
        if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
        time.sleep(3)
    print("fn timeout",c.get("Timeout"),"mem",c.get("MemorySize"))
    t=time.time()
    r=lam.invoke(FunctionName="justhodl-resilience",InvocationType="RequestResponse")
    print(f"invoke ({time.time()-t:.0f}s):",r["Payload"].read().decode()[:240])
    time.sleep(2)
    d=json.loads(s3.get_object(Bucket=B,Key="data/resilience.json")["Body"].read())
    print("\ndiagnostics:")
    for x in d.get("diagnostics",[]): print("  ",x)
    print("\ncounts:",d.get("counts"))
    print("\n🚀 ABOUT TO BOOM (coiled/igniting):")
    for r in d.get("about_to_boom",[])[:12]:
        print(f"  {r['ticker']:<6} {r['stage']:<9} res {r['resilience']} | +{r['mean_abnormal_on_adverse_pct']}%/adv ({r['adverse_hit_rate_pct']}% held, n{r['n_adverse_days']}) | asym {r['beta_asymmetry']} | {r['pct_of_60d_high']}% of 60d hi | volz {r['adverse_volume_z']} | cats {r['negative_catalysts_shrugged']}")
    print("\ntop_picks logged for grading:",[p["ticker"] for p in d.get("top_picks",[])])
    print("DONE 2090")
except lam.exceptions.ResourceNotFoundException:
    print("NOT_CREATED")
