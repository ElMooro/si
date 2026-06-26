import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
for _ in range(25):
    c=lam.get_function(FunctionName="justhodl-scarcity-radar")["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
lam.invoke(FunctionName="justhodl-scarcity-radar",InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/scarcity-radar.json")["Body"].read())
print("counts:",json.dumps(d.get("counts")))
fb=d.get("stealth_shortage_board") or []
inv=[r for r in fb if "inventory-drawdown" in (r.get("engines") or [])]
print(f"\ndemand-CONFIRMED inventory-drawdown names on board: {len(inv)}")
for r in inv[:12]:
    print(f"  {r.get('ticker'):<6} [{r.get('tier')}] comp={r.get('composite')} scar={r.get('scarcity')} steal={r.get('stealth')} | {str(r.get('why'))[:60]}")
# cross-check the source engine for what got excluded (destockers)
idr=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/inventory-drawdown.json")["Body"].read())
board=idr.get("stock_drawdown_board") or []
destock=[r for r in board if isinstance(r.get("rev_growth_yoy"),(int,float)) and r.get("rev_growth_yoy")<=0 and isinstance(r.get("dio_chg_pct"),(int,float)) and r.get("dio_chg_pct")<0]
print(f"\nexcluded destockers (DIO falling but revenue flat/down): {len(destock)} e.g.",[(r.get('ticker'),f\"dio{r.get('dio_chg_pct')}\",f\"rev{r.get('rev_growth_yoy')}\") for r in destock[:5]])
print("DONE 2240")
