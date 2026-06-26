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
print(f"\nnames on board carrying inventory-drawdown: {len(inv)}")
for r in inv[:10]:
    print(f"  {r.get('ticker'):<6} [{r.get('tier')}] scar={r.get('scarcity')} steal={r.get('stealth')} comp={r.get('composite')} eng={r.get('engines')} | {str(r.get('why'))[:55]}")
# did MU specifically make it + with what?
mu=[r for r in (d.get('stealth_shortage_board') or []) if r.get('ticker')=='MU']
print("\nMU on board:",mu[:1])
# top of board overall now
print("\nTOP 6 board:")
for r in fb[:6]:
    print(f"  {r.get('ticker'):<6} [{r.get('tier')}] comp={r.get('composite')} scar={r.get('scarcity')} steal={r.get('stealth')} {r.get('vertical')} eng={r.get('engines')}")
print("DONE 2239")
