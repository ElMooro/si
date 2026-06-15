import json, time, boto3
from datetime import datetime, timezone
lam=boto3.client("lambda",region_name="us-east-1"); s3=boto3.client("s3",region_name="us-east-1")
B="justhodl-dashboard-live"; K="data/opportunities-research.json"; TGT="justhodl-opportunities-research"
key=None
for src in ("justhodl-research-critique","justhodl-alpha-research","justhodl-bottleneck-research"):
    try:
        env=lam.get_function_configuration(FunctionName=src).get("Environment",{}).get("Variables",{})
        if env.get("ANTHROPIC_API_KEY"): key=env["ANTHROPIC_API_KEY"]; print(f"key from {src} (len {len(key)})"); break
    except Exception as e: print(src,str(e)[:40])
if key:
    cur=lam.get_function_configuration(FunctionName=TGT).get("Environment",{}).get("Variables",{})
    cur["ANTHROPIC_API_KEY"]=key
    lam.update_function_configuration(FunctionName=TGT, Environment={"Variables":cur})
    for _ in range(15):
        time.sleep(4)
        if lam.get_function_configuration(FunctionName=TGT).get("LastUpdateStatus")=="Successful": print("key patched"); break
n0=datetime.now(timezone.utc)
lam.invoke(FunctionName=TGT, InvocationType="Event"); print("invoked")
for i in range(16):
    time.sleep(20)
    try:
        d=json.loads(s3.get_object(Bucket=B,Key=K)["Body"].read())
        if datetime.fromisoformat(d.get("generated_at"))>=n0.replace(microsecond=0):
            bt=d.get("by_ticker",{})
            cl=lambda f: sum(1 for v in bt.values() if v.get(f))
            c=lambda f: sum(1 for v in bt.values() if v.get(f) is not None)
            print(f"\nREADY {(i+1)*20}s n={len(bt)} new_theses={d.get('new_theses')} dur={d.get('duration_s')}s verdicts={d.get('enriched_verdicts')}")
            print(f"coverage: thesis {cl('thesis')} bear {cl('bear')} financials {cl('financials')} pe {c('pe')} pe_pctile {c('pe_pctile')} cash_conv {c('cash_conv')} scorecard {c('score_bull')} 13F {c('sm_funds')}")
            # pick a name with a thesis
            t=next((k for k,v in bt.items() if v.get("thesis")), next(iter(bt)))
            v=bt[t]
            print(f"\nSAMPLE {t} ({v.get('name')}) — {v.get('verdict')} score {v.get('opportunity_score')}")
            print(f"  scorecard {v.get('score_bull')}bull/{v.get('score_bear')}bear | P/E {v.get('pe') and round(v['pe'],1)} vs ind {v.get('industry_pe')} (own {v.get('pe_pctile')}pctile) | {len(v.get('financials') or [])}yr fins | cashconv {v.get('cash_conv')}%")
            print("  bull:", v.get('flags_bull'))
            print("  bear:", v.get('flags_bear'))
            print("  THESIS:", (v.get('thesis') or '')[:300])
            print("  BEAR:", (v.get('bear') or '')[:230])
            break
    except Exception: pass
    print(f"  ...{(i+1)*20}s")
else: print("no key")
