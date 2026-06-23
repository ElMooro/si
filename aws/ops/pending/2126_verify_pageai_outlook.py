import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=600,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
def get(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception: return None
for _ in range(30):
    c=lam.get_function(FunctionName="justhodl-page-ai")["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(3)
sc=get("data/signal-scorecard.json") or {}
rows=sc.get("scorecard") or []
graded=[(r["signal_type"],r.get("alpha_status"),r.get("alpha_mean_excess_pct"),round((r.get('hit_rate') or 0)*100,1),r.get("alpha_n")) for r in rows if r.get("alpha_status")]
print("scorecard graded signals:",len(graded))
for st,al,me,hr,n in graded[:40]: print(f"   {st:<28} {al:<16} excess={me}% hit={hr}% n={n}")
# run 2 waves
for w in range(2):
    t=time.time(); r=lam.invoke(FunctionName="justhodl-page-ai",InvocationType="RequestResponse")
    print(f"\nwave {w+1}:",r["Payload"].read().decode()[:200],f"({time.time()-t:.0f}s)")
# scan generated page-ai files for ones with a real grounded outlook
paths=[o["Key"] for o in s3.get_paginator("list_objects_v2").paginate(Bucket="justhodl-dashboard-live",Prefix="data/page-ai/").search("Contents") if o]
n_files=len(paths); n_out=0; examples=[]
for k in paths:
    d=get(k); 
    if not d: continue
    o=d.get("outlook",{})
    if o.get("mean_excess_vs_spy_pct") is not None:
        n_out+=1
        if len(examples)<6: examples.append((d["page"],o))
print(f"\npage-ai files generated: {n_files} | with REAL grounded outlook (scorecard-matched): {n_out}")
for pg,o in examples:
    print(f"   {pg:<22} {o['alpha_status']:<14} mean_excess_vs_SPY={o['mean_excess_vs_spy_pct']}% hit={o.get('hit_rate_pct')}% n={o.get('n_graded')} [{o.get('matched_key')}]")
import urllib.request
def chk(u):
    for _ in range(2):
        try:
            with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=15) as x:return x.getcode()
        except Exception: time.sleep(5)
    return None
print("\njh-page-ai.js live:",chk("https://justhodl.ai/jh-page-ai.js"))
print("DONE 2126")
