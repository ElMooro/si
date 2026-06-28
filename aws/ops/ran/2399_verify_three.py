import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=120,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
def rd(k):
    return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
# COT
print("=== CME crypto COT ===")
r=lam.invoke(FunctionName="justhodl-crypto-cot",InvocationType="RequestResponse",Payload=b"{}");print("err:",r.get("FunctionError"))
time.sleep(2); d=rd("data/crypto-cot.json")
for a in ("btc","eth"):
    s=d.get(a) or {};am=s.get("asset_mgr") or {};lf=s.get("lev_funds") or {}
    print(f"  {a.upper()} ({s.get('report_date')}): OI {s.get('open_interest')} | AsstMgr {am.get('read')} net {am.get('net')} ({am.get('net_pctile_3y')}th{', '+am.get('extreme') if am.get('extreme') else ''}) | LevFund {lf.get('read')} net {lf.get('net')} ({lf.get('net_pctile_3y')}th)")
print("  divergence:",(d.get("btc") or {}).get("divergence"))
# Coinbase premium
print("=== Coinbase premium ===")
r=lam.invoke(FunctionName="justhodl-coinbase-premium",InvocationType="RequestResponse",Payload=b"{}");print("err:",r.get("FunctionError"))
time.sleep(2); d=rd("data/coinbase-premium.json")
for a in ("btc","eth"):
    s=d.get(a) or {};print(f"  {a.upper()}: prem {s.get('premium_pct')}% ({s.get('read')}) | CB {s.get('coinbase')} vs KR {s.get('kraken')}")
# Stablecoin peg
print("=== Stablecoin peg ===")
r=lam.invoke(FunctionName="justhodl-crypto-stablecoin-peg",InvocationType="RequestResponse",Payload=b"{}");print("err:",r.get("FunctionError"))
time.sleep(2); d=rd("data/crypto-stablecoin-peg.json")
print("  status:",d.get("status"),"| worst:",d.get("worst_coin"),d.get("worst_depeg_pct"),"%")
for sym,v in (d.get("coins") or {}).items(): print(f"    {sym}: ${v.get('price')} ({v.get('depeg_pct')}% {v.get('status')})")
print("DONE 2399")
