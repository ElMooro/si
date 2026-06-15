import json, time, boto3
from datetime import datetime, timezone
lam=boto3.client("lambda",region_name="us-east-1"); s3=boto3.client("s3",region_name="us-east-1")
B="justhodl-dashboard-live"; K="data/alpha-scoreboard-research.json"; TGT="justhodl-alpha-research"
# patch anthropic key from an existing claude engine
key=None
for src in ("justhodl-research-critique","justhodl-bottleneck-research","justhodl-khalid-metrics"):
    try:
        env=lam.get_function_configuration(FunctionName=src).get("Environment",{}).get("Variables",{})
        if env.get("ANTHROPIC_API_KEY"): key=env["ANTHROPIC_API_KEY"]; print(f"key from {src} (len {len(key)})"); break
    except Exception as e: print(src,str(e)[:50])
if key:
    cur=lam.get_function_configuration(FunctionName=TGT).get("Environment",{}).get("Variables",{})
    cur["ANTHROPIC_API_KEY"]=key
    lam.update_function_configuration(FunctionName=TGT, Environment={"Variables":cur})
    for _ in range(15):
        time.sleep(4)
        if lam.get_function_configuration(FunctionName=TGT).get("LastUpdateStatus")=="Successful": print("key patched"); break
n0=datetime.now(timezone.utc)
lam.invoke(FunctionName=TGT, InvocationType="Event"); print("invoked")
for i in range(14):
    time.sleep(20)
    try:
        d=json.loads(s3.get_object(Bucket=B,Key=K)["Body"].read())
        if datetime.fromisoformat(d.get("generated_at"))>=n0.replace(microsecond=0):
            bt=d.get("by_ticker",{})
            def c(f): return sum(1 for v in bt.values() if v.get(f) is not None)
            def cl(f): return sum(1 for v in bt.values() if v.get(f))
            print(f"\nREADY {(i+1)*20}s n={len(bt)} new_theses={d.get('new_theses')} logged={d.get('signals_logged')} dur={d.get('duration_s')}s")
            print(f"coverage: thesis {cl('thesis')} bear {cl('bear')} financials {cl('financials')} pe {c('pe')} pe_pctile {c('pe_pctile')} cash_conv {c('cash_conv')} scorecard {c('score_bull')}")
            print("concentration:", json.dumps(d.get("concentration")))
            print("track_record:", json.dumps(d.get("track_record")))
            t=next(iter(bt)); v=bt[t]
            print(f"\nSAMPLE {t} ({v.get('name')}): {v.get('n_systems')} systems {v.get('systems')}")
            print(f"  score {v.get('score_bull')}bull/{v.get('score_bear')}bear | P/E {v.get('pe') and round(v['pe'],1)} vs ind {v.get('industry_pe')} (own {v.get('pe_pctile')}pctile) | {len(v.get('financials') or [])}yr fins")
            print("  bull:", v.get('flags_bull'))
            print("  bear:", v.get('flags_bear'))
            print("  THESIS:", (v.get('thesis') or '')[:280])
            print("  BEAR:", (v.get('bear') or '')[:220])
            break
    except Exception: pass
    print(f"  ...{(i+1)*20}s")
else: print("not ready")
