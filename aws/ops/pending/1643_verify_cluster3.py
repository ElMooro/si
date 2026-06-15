import json, time, boto3
from datetime import datetime, timezone
s3=boto3.client("s3",region_name="us-east-1"); lam=boto3.client("lambda",region_name="us-east-1")
B="justhodl-dashboard-live"; K="data/bottleneck-boom-research.json"
n0=datetime.now(timezone.utc)
lam.invoke(FunctionName="justhodl-bottleneck-research", InvocationType="Event"); print("invoked")
for i in range(10):
    time.sleep(20)
    try:
        d=json.loads(s3.get_object(Bucket=B,Key=K)["Body"].read())
        if datetime.fromisoformat(d.get("generated_at"))>=n0.replace(microsecond=0):
            bt=d.get("by_ticker",{})
            wsh=sum(1 for v in bt.values() if v.get("short_pct") is not None)
            wsm=sum(1 for v in bt.values() if v.get("sm_funds") is not None)
            wfw=sum(1 for v in bt.values() if v.get("fwd_rev_growth") is not None)
            wch=sum(1 for v in bt.values() if v.get("chain"))
            print(f"READY {(i+1)*20}s new={d.get('new_theses')} dur={d.get('duration_s')}s")
            print(f"confirmation coverage: short {wsh}/{len(bt)} | smart-money {wsm}/{len(bt)} | fwd-growth {wfw}/{len(bt)} | value-chain {wch}/{len(bt)}")
            for t in ("VST","MU","CEG"):
                v=bt.get(t,{})
                print(f"\n{t}: short={v.get('short_pct')}%/{v.get('short_signal')} | funds={v.get('sm_funds')} net_adding={v.get('sm_net')} new={v.get('sm_new')} val={v.get('sm_value')} | fwd_growth={v.get('fwd_rev_growth')} | chain={v.get('chain')}")
            break
    except Exception: pass
    print(f"  ...{(i+1)*20}s")
else: print("not ready")
