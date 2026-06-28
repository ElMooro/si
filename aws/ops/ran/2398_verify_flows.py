import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=150,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
try: lam.get_function_configuration(FunctionName="justhodl-crypto-exchange-flows"); print("function OK")
except Exception as e: print("MISSING",str(e)[:60]); print("DONE 2398"); raise SystemExit
r=lam.invoke(FunctionName="justhodl-crypto-exchange-flows",InvocationType="RequestResponse",Payload=b"{}")
print("FunctionError:",r.get("FunctionError"),"| resp:",r["Payload"].read().decode()[:170])
time.sleep(3)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-exchange-flows.json")["Body"].read())
for a in ("btc","eth"):
    s=d.get(a) or {}
    if s.get("_err"): print(f"{a.upper()}: ERR {s['_err']}"); continue
    print(f"\n{a.upper()}: netflow today {s.get('netflow_today')} | 7d {s.get('cum_7d')} | 30d {s.get('cum_30d')} ({s.get('cum_30d_pctile')}th) | {s.get('regime')} | {s.get('trend')}")
    print(f"  in/out today: {s.get('flow_in_today')}/{s.get('flow_out_today')} | price ${s.get('price')}")
    es=s.get("event_study") or {}
    print(f"  event study {es.get('verdict')} ({es.get('standing')}, n_days {es.get('n_days')}): {es.get('hypothesis')}")
    for h in ("fwd30d","fwd90d"):
        v=es.get(h) or {}
        print(f"     {h}: outflow {v.get('outflow_mean')}% vs inflow {v.get('inflow_mean')}% | edge {v.get('edge_outflow_minus_inflow_pp')}pp (n {v.get('n_outflow')}/{v.get('n_inflow')})")
print("\nread:",d.get("interpretation"),"| history_n:",d.get("history_n"),"| _diag:",d.get("_diag"))
print("DONE 2398")
