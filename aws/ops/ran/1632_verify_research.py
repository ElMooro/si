"""Invoke bottleneck-research, verify financials + industry P/E + AI thesis populate."""
import json, time, boto3
from datetime import datetime, timezone
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
B = "justhodl-dashboard-live"; K = "data/bottleneck-boom-research.json"
today = datetime.now(timezone.utc).date().isoformat()
try:
    lam.invoke(FunctionName="justhodl-bottleneck-research", InvocationType="Event")
    print("invoked bottleneck-research (async)")
except Exception as e:
    print("invoke err:", str(e)[:160])

for i in range(14):  # up to ~4.5 min
    time.sleep(20)
    try:
        d = json.loads(s3.get_object(Bucket=B, Key=K)["Body"].read())
        if (d.get("generated_at","") >= today):
            bt = d.get("by_ticker", {})
            print(f"\nresearch READY after ~{(i+1)*20}s: n={d.get('n')} new_theses={d.get('new_theses')} dur={d.get('duration_s')}s")
            # coverage stats
            wfin = sum(1 for v in bt.values() if v.get("financials"))
            wth = sum(1 for v in bt.values() if v.get("thesis"))
            wpe = sum(1 for v in bt.values() if v.get("industry_pe") is not None)
            print(f"coverage: financials {wfin}/{len(bt)} | thesis {wth}/{len(bt)} | industry_pe {wpe}/{len(bt)}")
            # sample one top call
            samp = next((t for t,v in bt.items() if v.get("is_top_call") and v.get("thesis")), next(iter(bt)))
            v = bt[samp]
            print(f"\n=== SAMPLE: {samp} ({v.get('name')}) ===")
            print("desc:", (v.get('desc') or '')[:160])
            print(f"P/E {v.get('pe')} vs industry P/E {v.get('industry_pe')} | P/S {v.get('ps')} | mktcap {v.get('mkt_cap')}")
            fins = v.get('financials') or []
            print(f"financials years: {len(fins)} ", [f.get('year') for f in fins])
            if fins: print("  latest yr:", {k2:fins[-1].get(k2) for k2 in ('year','revenue','netIncome','nm')})
            print("THESIS:", v.get('thesis'))
            break
    except Exception:
        pass
    print(f"  ...waiting ({(i+1)*20}s)")
else:
    print("research not ready in time (may still be generating theses)")
