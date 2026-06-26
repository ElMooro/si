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
print("\nVERTICAL TIGHTNESS (which shortage is building):")
for v in (d.get("vertical_tightness") or [])[:8]:
    print(f"  {v.get('vertical'):<26} tight={v.get('tightness')} phase={v.get('phase')} signals={v.get('top_signals')}")
print("\nTOP STEALTH-SHORTAGE BOARD (scarcity x stealth):")
for r in (d.get("stealth_shortage_board") or [])[:12]:
    print(f"  {r.get('ticker'):<6} [{r.get('tier')}] scar={r.get('scarcity')} steal={r.get('stealth')} comp={r.get('composite')} | {r.get('vertical')} (tight {r.get('vertical_tightness')}) | {str(r.get('why'))[:45]}")
print("\nsignals_logged:",d.get("signals_logged"))
print("DONE 2232")
