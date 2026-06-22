import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=600,retries={"max_attempts":0}))
for _ in range(25):
    c=lam.get_function(FunctionName="justhodl-cyclical-bagger")["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(3)
t=time.time()
r=lam.invoke(FunctionName="justhodl-cyclical-bagger",InvocationType="RequestResponse")
print("invoke:",r["Payload"].read().decode()[:200],f"({time.time()-t:.0f}s)")
d=json.loads(boto3.client("s3","us-east-1").get_object(Bucket="justhodl-dashboard-live",Key="data/cyclical-bagger.json")["Body"].read())
print("stats:",d["stats"],"| mode:",d["mode"])
print("\n🌀 20x-SHAPE BOOK (clean — real businesses at deep troughs):")
print(f"  {'sym':<6}{'stage':<11}{'score':>6}{'cap':>8}  op-margin trough→now (swing)   eps_n2p  run  themes")
for r in d["twenty_x_shape_book"]:
    print(f"  {r['ticker']:<6}{r['stage']:<11}{r['cyclical_20x_score']:>6}{str(r['cap_bucket']):>8}  {r['om_trough']}%→{r['om_now']}% (+{r['om_swing_pp']}pp)  {str(r['eps_neg_to_pos']):<6} {r['run_from_trough_x']}x  {r.get('secular_themes')[:2]}")
print("\nharvester top_picks (forward-graded):",[(p['ticker'],p['score'],p['stage']) for p in d.get('top_picks',[])])
print("DONE 2115")
