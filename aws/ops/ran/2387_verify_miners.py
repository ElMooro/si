import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=150,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
try:
    cfg=lam.get_function_configuration(FunctionName="justhodl-crypto-miners")
    print("function created OK | timeout",cfg["Timeout"])
except Exception as e:
    print("MISSING:",str(e)[:80]); print("DONE 2387"); raise SystemExit
r=lam.invoke(FunctionName="justhodl-crypto-miners",InvocationType="RequestResponse",Payload=b"{}")
print("FunctionError:",r.get("FunctionError"),"| resp:",r["Payload"].read().decode()[:160])
time.sleep(3)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-miners.json")["Body"].read())
hr=d.get("hash_ribbons") or {}; pu=d.get("puell") or {}; es=d.get("event_study") or {}
print("\nHASH RIBBONS:",hr.get("state"),"| hash",hr.get("hash_rate_eh"),"EH/s | ma30",hr.get("ma30_eh"),"ma60",hr.get("ma60_eh"))
print("  last cross:",hr.get("last_cross"))
print("PUELL:",pu.get("value"),"(",pu.get("zone"),") | rev $",pu.get("revenue_usd_m"),"M/d | 365dMA $",pu.get("ma365_usd_m"),"M")
print("DIFFICULTY 14d:",(d.get("difficulty") or {}).get("chg_14d_pct"),"%")
print("\nEVENT STUDY — hash ribbon (fwd BTC after buy crossups):",(es.get("hash_ribbon") or {}).get("verdict"))
for h in ("fwd30d","fwd90d","fwd180d"):
    v=(es.get("hash_ribbon") or {}).get(h) or {}
    print(f"   {h}: buy mean {v.get('buy_mean')}% vs base {v.get('baseline_mean')}% | edge {v.get('edge_pp')}pp (n_buys {v.get('n_buys')})")
print("EVENT STUDY — Puell (low vs high fwd BTC):",(es.get("puell") or {}).get("verdict"))
for h in ("fwd30d","fwd90d","fwd180d"):
    v=(es.get("puell") or {}).get(h) or {}
    print(f"   {h}: low {v.get('low_puell_mean')}% vs high {v.get('high_puell_mean')}% | edge L-H {v.get('edge_low_minus_high_pp')}pp (n_low {v.get('n_low')}, n_high {v.get('n_high')})")
print("\nread:",d.get("interpretation"),"| history_n:",d.get("history_n"),"| _diag:",d.get("_diag"))
print("DONE 2387")
