import json, time, boto3
from datetime import datetime, timezone
s3 = boto3.client("s3", region_name="us-east-1"); lam = boto3.client("lambda", region_name="us-east-1")
B="justhodl-dashboard-live"; K="data/bottleneck-boom-research.json"
now0=datetime.now(timezone.utc)
try: lam.invoke(FunctionName="justhodl-bottleneck-research", InvocationType="Event"); print("invoked")
except Exception as e: print("err",str(e)[:120])
for i in range(11):
    time.sleep(20)
    try:
        d=json.loads(s3.get_object(Bucket=B,Key=K)["Body"].read())
        ga=d.get("generated_at","")
        if ga and datetime.fromisoformat(ga) >= now0.replace(microsecond=0):
            bt=d.get("by_ticker",{})
            wfin=sum(1 for v in bt.values() if v.get("financials"))
            wth=sum(1 for v in bt.values() if v.get("thesis"))
            wpe=sum(1 for v in bt.values() if v.get("industry_pe") is not None)
            print(f"READY ~{(i+1)*20}s: n={d.get('n')} new_theses={d.get('new_theses')} dur={d.get('duration_s')}s")
            print(f"coverage: financials {wfin}/{len(bt)} | thesis {wth}/{len(bt)} | industry_pe {wpe}/{len(bt)}")
            samp=next((t for t,v in bt.items() if v.get("is_top_call") and v.get("thesis")), None) or next(iter(bt))
            v=bt[samp]; fins=v.get("financials") or []
            print(f"\nSAMPLE {samp} ({v.get('name')}): P/E {v.get('pe')} vs ind {v.get('industry_pe')} | P/S {v.get('ps')} | {len(fins)}yr fins {[f.get('year') for f in fins][:12]}")
            print("desc:",(v.get('desc') or '')[:140])
            print("THESIS:",v.get('thesis'))
            break
    except Exception: pass
    print(f"  ...{(i+1)*20}s")
else: print("not ready")
