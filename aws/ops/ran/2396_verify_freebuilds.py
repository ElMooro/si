import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=150,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
def rd(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception as e: return {"_readerr":str(e)[:50]}
# 1) onchain-ratios extension
print("=== onchain-ratios (NEW: realized price, NUPL, exchange netflows) ===")
lam.invoke(FunctionName="justhodl-onchain-ratios",InvocationType="RequestResponse",Payload=b"{}")
time.sleep(2)
o=rd("data/onchain-ratios.json")
btc=o.get("btc") or o  # structure may nest under btc
def find(d,*ks):
    for k in ks:
        if isinstance(d,dict) and k in d: return d[k]
    # search nested
    if isinstance(d,dict):
        for v in d.values():
            if isinstance(v,dict):
                for k in ks:
                    if k in v: return v[k]
    return None
print("  realized_price:",find(o,"realized_price"),"| price_vs_realized_pct:",find(o,"price_vs_realized_pct"))
print("  nupl:",find(o,"nupl"),"| nupl_zone:",find(o,"nupl_zone"))
print("  exchange_netflow_usd:",find(o,"exchange_netflow_usd"),"| netflow_regime:",find(o,"netflow_regime"))
# 2) dvol IV-RV
print("\n=== crypto-dvol (NEW: realized vol + variance premium) ===")
lam.invoke(FunctionName="justhodl-crypto-dvol",InvocationType="RequestResponse",Payload=b"{}")
time.sleep(2)
dv=rd("data/crypto-dvol.json")
print("  realized_vol_30d:",dv.get("realized_vol_30d"),"| variance_risk_premium:",dv.get("variance_risk_premium"))
print("  vrp_read:",dv.get("vrp_read"))
# 3) pre-existing engines live?
print("\n=== pre-existing free-gap engines — live + producing? ===")
for fn,key in [("justhodl-crypto-cot","data/crypto-cot.json"),
               ("justhodl-coinbase-premium","data/coinbase-premium.json"),
               ("justhodl-crypto-stablecoin-peg","data/crypto-stablecoin-peg.json")]:
    try:
        lam.invoke(FunctionName=fn,InvocationType="Event",Payload=b"{}")
        d=rd(key); age=d.get("generated_at") or d.get("updated_at") or d.get("_readerr") or "?"
        print(f"  {fn}: {'LIVE' if '_readerr' not in d else 'NO DATA'} (asof {age})")
    except Exception as e:
        print(f"  {fn}: invoke err {str(e)[:50]}")
print("DONE 2396")
