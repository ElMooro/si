import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
# re-invoke so it reads the UPGRADED supply-inflection (real spot/PPI)
lam.invoke(FunctionName="justhodl-scarcity-radar",InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/scarcity-radar.json")["Body"].read())
print("counts:",json.dumps(d.get("counts")))
fb=d.get("full_board") or []
print("full_board n:",len(fb))
# distribution: top by scarcity, top by stealth, top by composite
print("\nTOP 8 by SCARCITY:")
for r in sorted(fb,key=lambda x:-(x.get('scarcity') or 0))[:8]:
    print(f"  {r.get('ticker'):<6} scar={r.get('scarcity')} steal={r.get('stealth')} comp={r.get('composite')} vert={r.get('vertical')} tight={r.get('vertical_tightness')} eng={r.get('engines')}")
print("\nTOP 8 by STEALTH:")
for r in sorted(fb,key=lambda x:-(x.get('stealth') or 0))[:8]:
    print(f"  {r.get('ticker'):<6} scar={r.get('scarcity')} steal={r.get('stealth')} comp={r.get('composite')} vert={r.get('vertical')}")
print("\nTOP 8 by COMPOSITE:")
for r in sorted(fb,key=lambda x:-(x.get('composite') or 0))[:8]:
    print(f"  {r.get('ticker'):<6} scar={r.get('scarcity')} steal={r.get('stealth')} comp={r.get('composite')} vert={r.get('vertical')} why={str(r.get('why'))[:50]}")
print("\nvertical tightness top:",[(v.get('vertical'),v.get('tightness')) for v in (d.get('vertical_tightness') or [])[:8]])
print("DONE 2231")
