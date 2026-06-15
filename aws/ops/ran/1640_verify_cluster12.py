import json, time, boto3
from datetime import datetime, timezone
s3=boto3.client("s3",region_name="us-east-1"); lam=boto3.client("lambda",region_name="us-east-1")
B="justhodl-dashboard-live"; K="data/bottleneck-boom-research.json"
n0=datetime.now(timezone.utc)
lam.invoke(FunctionName="justhodl-bottleneck-research", InvocationType="Event"); print("invoked")
for i in range(12):
    time.sleep(20)
    try:
        d=json.loads(s3.get_object(Bucket=B,Key=K)["Body"].read())
        if datetime.fromisoformat(d.get("generated_at"))>=n0.replace(microsecond=0) and d.get("new_theses",0)>0:
            bt=d.get("by_ticker",{})
            wfcf=sum(1 for v in bt.values() if any(f.get("fcf") is not None for f in (v.get("financials") or [])))
            wsh=sum(1 for v in bt.values() if any(f.get("shares") for f in (v.get("financials") or [])))
            wearn=sum(1 for v in bt.values() if v.get("next_earnings"))
            w52=sum(1 for v in bt.values() if v.get("off_52w_high") is not None)
            wgmt=sum(1 for v in bt.values() if v.get("gm_trend") is not None)
            from collections import Counter
            traps=Counter(v.get("trap") for v in bt.values())
            print(f"READY {(i+1)*20}s new={d.get('new_theses')} dur={d.get('duration_s')}s")
            print(f"coverage: fcf {wfcf}/{len(bt)} | shares {wsh}/{len(bt)} | earnings {wearn}/{len(bt)} | 52w {w52}/{len(bt)} | gm_trend {wgmt}/{len(bt)}")
            print("trap-check distribution:", dict(traps))
            v=bt.get("MU",{}); f=(v.get("financials") or [])
            print(f"\nMU: 52w off-high {v.get('off_52w_high')}% off-low {v.get('off_52w_low')}% | next earnings {v.get('next_earnings')} | gm_trend {v.get('gm_trend')}pp | trap={v.get('trap')} | share_chg {v.get('share_chg_pct')}%")
            if f: print("MU latest yr fin:", {k2:f[-1].get(k2) for k2 in ('year','revenue','gm','om','nm','fcf','fcfm','shares')})
            print("\nMU thesis:", (v.get('thesis') or '')[:400])
            break
    except Exception as e: pass
    print(f"  ...{(i+1)*20}s")
else: print("not ready")
