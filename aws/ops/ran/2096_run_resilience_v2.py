import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=600,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
for _ in range(20):
    c=lam.get_function(FunctionName="justhodl-resilience")["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(3)
t=time.time()
print("invoke:",lam.invoke(FunctionName="justhodl-resilience",InvocationType="RequestResponse")["Payload"].read().decode()[:220],f"({time.time()-t:.0f}s)")
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/resilience.json")["Body"].read())
print("version:",d.get("version"),"| basis:",d.get("abnormal_basis"))
print("diagnostics:")
for x in d.get("diagnostics",[]): print("  ",x)
print("\ncounts:",d.get("counts"))
print("\n🚀 ABOUT TO BOOM (idio-evidence first):")
for r in d.get("about_to_boom",[])[:14]:
    star="⭐IDIO" if r.get("has_idiosyncratic_evidence") else ""
    print(f"  {r['ticker']:<6} {r['stage']:<9} res {r['resilience']} | {r['abnormal_basis']:<10} +{r['mean_abnormal_on_adverse_pct']}%/adv ({r['adverse_hit_rate_pct']}% held,n{r['n_adverse_days']}) | dom {r['dominant_adverse_type']} {star}")
    for ev in r.get("events_shrugged",[])[:3]:
        print(f"         shrugged {ev['type']} {ev['date']}: day {ev['day_return_pct']}% (abn +{ev['abnormal_pct']}%)")
print("\nnames WITH idiosyncratic evidence (held through real catalyst):")
idio=[r for r in d.get("all_resilient",[]) if r.get("has_idiosyncratic_evidence")]
for r in idio[:12]:
    types=",".join(f"{k}:{v['n']}({v['held_pct']}%held)" for k,v in r.get("adverse_by_type",{}).items() if k in ("EARNINGS_MISS","DOWNGRADE","PT_CUT","GUIDANCE_CUT"))
    print(f"  {r['ticker']:<6} res {r['resilience']} {r['stage']:<9} | {types}")
print("\ntop_picks:",[(p["ticker"],p["dominant_adverse_type"],"idio" if p["has_idiosyncratic_evidence"] else "") for p in d.get("top_picks",[])])
print("DONE 2096")
