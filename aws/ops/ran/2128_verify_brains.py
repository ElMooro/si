import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=600,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
def get(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception as e: return {"_missing":str(e)[:40]}
# are the brain feeds populated?
for f in ["data/kill-theses.json","data/engine-conflicts.json","data/lead-lag-graph.json","data/signal-orthogonality.json"]:
    d=get(f)
    if "_missing" in d: print(f"  FEED {f}: MISSING/empty"); continue
    n=len(d.get("theses") or d.get("conflicts") or d.get("live_predictions") or [])
    print(f"  FEED {f}: theses/conflicts/predictions={n} mode={d.get('mode','-')} gsi={d.get('gsi_total') or d.get('gsi','-')}")
for _ in range(30):
    c=lam.get_function(FunctionName="justhodl-best-setups")["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(3)
t=time.time(); r=lam.invoke(FunctionName="justhodl-best-setups",InvocationType="RequestResponse")
print("\nbest-setups invoke:",r["Payload"].read().decode()[:120],f"({time.time()-t:.0f}s)")
b=get("data/best-setups.json")
mi=b.get("meta_intelligence",{})
print("meta_intelligence:",json.dumps(mi))
print("contested_picks:",len(b.get("contested_picks",[])),"| with_kill_thesis:",len(b.get("picks_with_kill_thesis",[])),"| lead_lag_tailwinds:",len(b.get("lead_lag_tailwinds",[])))
for s in b.get("picks_with_kill_thesis",[])[:4]:
    print(f"   KILL {s['ticker']:<6} conv={s.get('conviction')}: {(s.get('failure_mode') or '')[:90]}")
for s in b.get("contested_picks",[])[:3]:
    c=s.get("conflict",{}); print(f"   CONTESTED {s['ticker']:<6} [{c.get('type')}] bear: {(c.get('bear') or '')[:80]}")
for s in b.get("lead_lag_tailwinds",[])[:3]:
    ll=s.get("lead_lag",{}); print(f"   TAILWIND {s['ticker']:<6} leader {ll.get('leader')} moved {ll.get('leader_move_2d_pct')}%")
print("DONE 2128")
