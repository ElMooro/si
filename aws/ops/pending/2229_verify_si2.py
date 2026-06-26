import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=890,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
for _ in range(28):
    c=lam.get_function(FunctionName="justhodl-supply-inflection-scanner")["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
lam.invoke(FunctionName="justhodl-supply-inflection-scanner",InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/supply-inflection.json")["Body"].read())
fs=d.get("fetch_stats") or {}
print("fetch_stats:", json.dumps(fs))
sigs=d.get("signals") or {}
new=["SEMI_PPI","MEMORY_STORAGE_PPI","COPPER_SPOT","URANIUM_SPOT","NICKEL_SPOT","ALUMINUM_SPOT","IRON_ORE_SPOT","STEEL_PPI","NATGAS_SPOT"]
print("new real-data signals now scoring:")
for n in new:
    s=sigs.get(n)
    print(f"  {n:<18} "+(f"score={s.get('score')} flag={s.get('flag')}" if isinstance(s,dict) else "STILL MISSING"))
# top tightening themes (real data now feeds these)
bt=d.get("by_theme") or {}
ranked=sorted(((k,v.get('composite_inflection_score')) for k,v in bt.items() if isinstance(v,dict)),key=lambda x:-(x[1] or 0))[:8]
print("top tightening themes:", ranked)
print("DONE 2229")
