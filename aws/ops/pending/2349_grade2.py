import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
def doc(): return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/cycle-clock.json")["Body"].read())
b4=doc().get("generated_at")
lam.invoke(FunctionName="justhodl-cycle-clock",InvocationType="Event",Payload=b"{}")
d=None
for i in range(16):
    time.sleep(12); cur=doc()
    if cur.get("generated_at")!=b4: d=cur; break
if not d: d=doc()
qb=(d.get("track_record") or {}).get("quadrant_backtest") or {}
print("v",d.get("version"),"| current quadrant:",(d.get("track_record") or {}).get("current_quadrant"))
print("lookback:",qb.get("lookback_years"),"y |",qb.get("n_months"),"coord months")
for q in ["GOLDILOCKS","OVERHEAT","STAGFLATION","DOWNTURN"]:
    v=(qb.get("by_quadrant") or {}).get(q)
    if v: print(f"  {q:12s} n={v['n']:2d}  fwd1m {v['avg_fwd_1m']:+}%  fwd3m {v['avg_fwd_3m']}%  pos1m {v['pct_pos_1m']}%")
    else: print(f"  {q:12s} (none)")
print("DONE 2349")
