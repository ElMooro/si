import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=90,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
r=lam.invoke(FunctionName="justhodl-onchain-ratios",InvocationType="RequestResponse",Payload=b"{}")
print("onchain-ratios err:",r.get("FunctionError"))
time.sleep(3)
# find where onchain-ratios writes
import urllib.request
for key in ["data/onchain-ratios.json","onchain-ratios.json"]:
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=key)["Body"].read())
        b=d.get("btc") or d
        print(f"[{key}] price {b.get('price')} | MVRV {b.get('mvrv')} | realized_price {b.get('realized_price')} | price_vs_realized {b.get('price_vs_realized_pct')}% | NUPL {b.get('nupl')} ({b.get('nupl_zone')}) | errors {b.get('errors')}")
        break
    except Exception as e: print(key,"->",str(e)[:50])
# re-verify COT label
d2=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-cot.json")["Body"].read())
am=(d2.get("btc") or {}).get("asset_mgr") or {}
print("COT btc asset_mgr: net",am.get("net"),"pctile",am.get("net_pctile_3y"),"extreme:",am.get("extreme"),"read:",am.get("read"))
print("DONE 2400")
