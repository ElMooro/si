import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=600,retries={"max_attempts":0}))
for _ in range(20):
    c=lam.get_function(FunctionName="justhodl-cyclical-bagger")["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(3)
t=time.time()
r=lam.invoke(FunctionName="justhodl-cyclical-bagger",InvocationType="RequestResponse")
print("invoke:",r["Payload"].read().decode()[:200],f"({time.time()-t:.0f}s)")
d=json.loads(boto3.client("s3","us-east-1").get_object(Bucket="justhodl-dashboard-live",Key="data/cyclical-bagger.json")["Body"].read())
print("stats:",d["stats"],"mode:",d["mode"])
print("\n20x-SHAPE BOOK (early/confirming — the real hunt):")
for r in d["twenty_x_shape_book"][:12]:
    print(f"  {r['ticker']:<6}{r['stage']:<11}score={r['cyclical_20x_score']:<6}om {r['om_trough']}%→{r['om_now']}% (+{r['om_swing_pp']}pp) eps_n2p={r['eps_neg_to_pos']} run={r['run_from_trough_x']}x cap={r['cap_bucket']} {r.get('secular_themes')[:2]}")
print("\nTOP 12 ALL-RANKED (incl partials):")
for r in d["all_ranked"][:12]:
    print(f"  {r['ticker']:<6}{r['stage']:<11}score={r['cyclical_20x_score']:<6}shape={str(r['twenty_x_shape']):<5}swing=+{r['om_swing_pp']}pp coil={r['coil_score']} viol={r['violence_score']} cap={r['cap_bucket']}")
print("\nMU/SNDK (should be LATE/mega, correctly retired):")
for r in d["all_ranked"]+[x for x in d.get('all_ranked',[])]:
    pass
import urllib.request
def get(u):
    for _ in range(3):
        try:
            with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=20) as r:return r.getcode()
        except Exception:time.sleep(10)
    return None
print("page:",get("https://justhodl.ai/equity-cyclical-bagger.html?t="+str(int(time.time()))))
print("DONE 2114")
