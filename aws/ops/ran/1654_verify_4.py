import json, time, boto3
from datetime import datetime, timezone
s3=boto3.client("s3",region_name="us-east-1"); lam=boto3.client("lambda",region_name="us-east-1")
B="justhodl-dashboard-live"; K="data/bottleneck-boom-research.json"
n0=datetime.now(timezone.utc)
lam.invoke(FunctionName="justhodl-bottleneck-research", InvocationType="Event"); print("invoked")
for i in range(11):
    time.sleep(20)
    try:
        d=json.loads(s3.get_object(Bucket=B,Key=K)["Body"].read())
        if datetime.fromisoformat(d.get("generated_at"))>=n0.replace(microsecond=0):
            print(f"READY {(i+1)*20}s dur={d.get('duration_s')}s")
            print("#1 track_record:", json.dumps(d.get("track_record")))
            print("#2 concentration:", json.dumps(d.get("concentration")))
            print("#3 changes:", json.dumps(d.get("changes")))
            pp=d.get("pressure_pctiles") or {}
            print("#4 pressure_pctiles:", json.dumps(pp))
            bt=d.get("by_ticker",{})
            print("   per-stock pctile coverage:", sum(1 for v in bt.values() if v.get("pressure_pctile") is not None),"/",len(bt))
            break
    except Exception as e: pass
    print(f"  ...{(i+1)*20}s")
else: print("not ready")
