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
print("counts:", json.dumps(d.get("counts")))
fb=d.get("stealth_shortage_board") or []
inv=[r for r in fb if "inventory-drawdown" in (r.get("engines") or [])]
print("demand-CONFIRMED inventory-drawdown names on board:", len(inv))
for r in inv[:12]:
    print("  ", r.get("ticker"), "["+str(r.get("tier"))+"]", "comp="+str(r.get("composite")),
          "scar="+str(r.get("scarcity")), "steal="+str(r.get("stealth")), "::", str(r.get("why"))[:60])
idr=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/inventory-drawdown.json")["Body"].read())
board=idr.get("stock_drawdown_board") or []
destock=[]
for r in board:
    rev=r.get("rev_growth_yoy"); dio=r.get("dio_chg_pct")
    if isinstance(rev,(int,float)) and rev<=0 and isinstance(dio,(int,float)) and dio<0:
        destock.append(r.get("ticker")+" (dio "+str(dio)+"%, rev "+str(rev)+"%)")
print("excluded destockers (DIO falling, demand NOT rising):", len(destock))
for x in destock[:6]: print("   -", x)
print("DONE 2241")
