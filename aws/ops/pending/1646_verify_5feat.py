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
            def cov(f): return sum(1 for v in bt.values() if v.get(f) is not None)
            def covl(f): return sum(1 for v in bt.values() if v.get(f))
            print(f"READY {(i+1)*20}s dur={d.get('duration_s')}s")
            print(f"price: ret_1m {cov('ret_1m')} | ret_3m {cov('ret_3m')} | sparkline {covl('price_spark')}")
            print(f"valuation hist: pe_low {cov('pe_low')} | pe_high {cov('pe_high')} | pe_pctile {cov('pe_pctile')}")
            print(f"earnings: beat_rate {cov('beat_rate')} | nq_eps_est {cov('nq_eps_est')} | nq_rev_est {cov('nq_rev_est')}")
            for t in ("MU","DELL"):
                v=bt.get(t,{})
                print(f"\n{t}: price ${v.get('price')} | 1m {v.get('ret_1m')}% 3m {v.get('ret_3m')}% | spark_pts {len(v.get('price_spark') or [])}")
                print(f"   P/E {v.get('pe') and round(v['pe'],1)} | own range {v.get('pe_low')}-{v.get('pe_high')} ({v.get('pe_pctile')}th pctile)")
                print(f"   beats {v.get('beat_rate')}% of last {v.get('beats_n')} | next-Q est EPS {v.get('nq_eps_est')} rev {v.get('nq_rev_est')}")
            break
    except Exception: pass
    print(f"  ...{(i+1)*20}s")
else: print("not ready")
