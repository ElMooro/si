import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
for _ in range(20):
    c=lam.get_function(FunctionName="justhodl-inventory-drawdown")["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
env=c.get("Environment",{}).get("Variables",{})
print("FRED_API_KEY set now:",bool(env.get("FRED_API_KEY")))
r=lam.invoke(FunctionName="justhodl-inventory-drawdown",InvocationType="RequestResponse")
print("invoke status:",r.get("StatusCode"))
pl=r["Payload"].read().decode()[:300]
print("payload:",pl)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/inventory-drawdown.json")["Body"].read())
print("generated_at:",d.get("generated_at"))
print("counts:",json.dumps(d.get("counts")))
print("top-level keys:",list(d.keys()))
sec=d.get("sector_drawdown") or d.get("sectors") or []
print("\nSECTOR I/S RATIOS (falling = drawdown = bullish):")
for s in (sec if isinstance(sec,list) else [])[:10]:
    print("  ",{k:s.get(k) for k in list(s.keys())[:6]})
bm=d.get("boom_setups") or d.get("boom_book") or d.get("stock_book") or []
print(f"\nSTOCK BOOM SETUPS (DIO falling + demand rising): {len(bm)}")
for r2 in bm[:10]:
    print("  ",{k:r2.get(k) for k in list(r2.keys())[:7]})
print("DONE 2238")
