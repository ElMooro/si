import json, time, boto3
from datetime import datetime, timezone
s3=boto3.client("s3",region_name="us-east-1"); lam=boto3.client("lambda",region_name="us-east-1")
B="justhodl-dashboard-live"; K="data/bottleneck-boom-research.json"
n0=datetime.now(timezone.utc)
lam.invoke(FunctionName="justhodl-bottleneck-research", InvocationType="Event"); print("invoked")
for i in range(8):
    time.sleep(20)
    try:
        d=json.loads(s3.get_object(Bucket=B,Key=K)["Body"].read())
        if datetime.fromisoformat(d.get("generated_at"))>=n0.replace(microsecond=0):
            bt=d.get("by_ticker",{})
            def c(f): return sum(1 for v in bt.values() if v.get(f) is not None)
            print(f"READY {(i+1)*20}s: int_cov {c('int_cov')} | ev_ebitda {c('ev_ebitda')} | peg {c('peg')}")
            v=bt.get("DELL",{})
            print(f"DELL: int_cov {v.get('int_cov')} | ev/ebitda {v.get('ev_ebitda')} | peg {v.get('peg')} | nde {v.get('net_debt_ebitda')}")
            break
    except Exception: pass
    print(f"  ...{(i+1)*20}s")
else: print("not ready")
