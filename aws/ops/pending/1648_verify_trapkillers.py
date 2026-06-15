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
            def c(f): return sum(1 for v in bt.values() if v.get(f) is not None)
            def cl(f): return sum(1 for v in bt.values() if v.get(f))
            print(f"READY {(i+1)*20}s new={d.get('new_theses')} dur={d.get('duration_s')}s")
            print(f"#1 cash:   cash_conv {c('cash_conv')} | accruals {c('accruals')}")
            print(f"#2 solv:   net_debt_ebitda {c('net_debt_ebitda')} | cur_ratio {c('cur_ratio')} | int_cov {c('int_cov')}")
            print(f"#3 insider:{c('insider_sig')} | #4 pressure_trend {c('pressure_trend')}")
            print(f"#5 val:    peg {c('peg')} | ev_ebitda {c('ev_ebitda')}")
            print(f"#6 revqual:acq_pct {c('acq_pct')} | seg_conc {c('seg_conc')}")
            print(f"#7 bear:   {cl('bear')} | #9 sector_mom {c('sector_mom')} | #10 scorecard {c('score_bull')}")
            v=bt.get("MU",{})
            print(f"\nMU: cash_conv {v.get('cash_conv')}% accruals {v.get('accruals')} | nde {v.get('net_debt_ebitda')} cur {v.get('cur_ratio')} intcov {v.get('int_cov')}")
            print(f"  peg {v.get('peg')} ev/ebitda {v.get('ev_ebitda')} | insider {v.get('insider_sig')} ({v.get('insider_buys')}b/{v.get('insider_sells')}s) | pressure_trend {v.get('pressure_trend')}")
            print(f"  scorecard: {v.get('score_bull')} bull / {v.get('score_bear')} bear")
            print(f"  bull: {v.get('flags_bull')}")
            print(f"  bear: {v.get('flags_bear')}")
            print(f"  BEAR CASE: {v.get('bear')}")
            break
    except Exception as e: pass
    print(f"  ...{(i+1)*20}s")
else: print("not ready")
