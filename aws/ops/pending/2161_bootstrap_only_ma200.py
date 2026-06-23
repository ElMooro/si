import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=890,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
FN="justhodl-ma200-reclaim"
for _ in range(40):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
sess=0
for k in range(3):
    lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ma200-reclaim.json")["Body"].read())
    sess=d.get("buffer_sessions",0)
    print(f"invoke {k+1}: buffer_sessions={sess} status={d.get('status','READY')} fetched={d.get('fetched_this_run')} dur={d.get('duration_s')}s")
    if sess>=205: break
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ma200-reclaim.json")["Body"].read())
print("\nuniverse:",d.get("universe"),"| counts:",json.dumps(d.get("counts",{})))
for bk,lbl in [("retest_held","RETEST-HELD (reclaimed→pulled back→held)"),("fresh_breakouts_above","FRESH BREAKOUTS ABOVE"),("fresh_breakdowns_below","FRESH BREAKDOWNS BELOW")]:
    arr=d.get(bk,[])
    if arr:
        print(f"\n{lbl}: {len(arr)}")
        for r in arr[:8]:
            print(f"   {r['ticker']:<6} {r.get('state',''):<16} px {r['price']} vs ma200 {r['ma200']} ({r['dist_pct']:+}%) slope {r.get('ma200_slope_pct')}% gc={r.get('ma50_above_ma200')} bars={r.get('bars_since_cross')}")
print("\nsignals_logged:",d.get("signals_logged"))
print("DONE 2161")
