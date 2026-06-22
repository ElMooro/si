import boto3, json, time, urllib.request
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
print("\n=== FULL 20x book (shape + survives + secular overlay) — the real ones ===")
fb=d.get("full_20x_book",[])
if fb:
    for r in fb:
        print(f"  {r['ticker']:<6}{r['stage']:<11}score={r['cyclical_20x_score']:<6}om+{r['om_swing_pp']}pp survived={r.get('survived')} secular={r.get('secular_class')} cap={r['cap_bucket']} {r.get('survival_detail')}")
else:
    print("  (empty — honest: no full MU-shaped confluence right now)")
print("\n=== CYCLICAL-ONLY book (deep snapback, no secular -> ~2-5x not 20x) ===")
for r in d.get("cyclical_only_book",[]):
    print(f"  {r['ticker']:<6}{r['stage']:<11}score={r['cyclical_20x_score']:<6}om {r['om_trough']}%->{r['om_now']}% (+{r['om_swing_pp']}pp) survived={r.get('survived')} secular={r.get('secular_class')} {r.get('survival_detail')}")
print("\nharvester top_picks:",[(p['ticker'],p['score'],p.get('secular_class')) for p in d.get('top_picks',[])])
def chk(u):
    for _ in range(3):
        try:
            with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=20) as x:return x.getcode()
        except Exception:time.sleep(8)
    return None
print("page:",chk("https://justhodl.ai/equity-cyclical-bagger.html?t="+str(int(time.time()))))
print("DONE 2116")
