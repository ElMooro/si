import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
def doc(): return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/cycle-clock.json")["Body"].read())
b4=doc().get("generated_at")
lam.invoke(FunctionName="justhodl-cycle-clock",InvocationType="Event",Payload=b"{}")
d=None
for i in range(18):
    time.sleep(12); cur=doc()
    if cur.get("generated_at")!=b4 and cur.get("version")=="2.7": d=cur; print(f"wrote v2.7 dur {cur.get('duration_s')}s"); break
if not d: print("NO v2.7:",doc().get("version")); print("DONE 2348"); raise SystemExit
tr=d.get("track_record") or {}
qb=tr.get("quadrant_backtest") or {}; pg=tr.get("posture_grade") or {}
print("\n=== QUADRANT BACKTEST (current quadrant:",tr.get("current_quadrant"),") ===")
print("  lookback:",qb.get("lookback_years"),"y |",qb.get("n_months"),"months")
for q,v in (qb.get("by_quadrant") or {}).items():
    print(f"  {q:12s} n={v['n']:2d}  fwd1m {v['avg_fwd_1m']:+}%  fwd3m {v['avg_fwd_3m']}%  pos1m {v['pct_pos_1m']}%")
print("\n=== POSTURE FORWARD GRADE ===")
print("  status:",pg.get("status"),"| n_graded:",pg.get("n_graded"),"| hit_rate:",pg.get("hit_rate"),"| avg fwd riskoff:",pg.get("avg_fwd_when_riskoff"),"riskon:",pg.get("avg_fwd_when_riskon"))
print("DONE 2348")
