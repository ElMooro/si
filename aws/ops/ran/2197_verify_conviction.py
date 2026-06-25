import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
for _ in range(25):
    c=lam.get_function(FunctionName="justhodl-conviction-engine")["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
lam.invoke(FunctionName="justhodl-conviction-engine",InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/conviction.json")["Body"].read())
blob=json.dumps(d)
print("Risk Regime ingested:", "Risk Regime" in blob or "risk-regime" in blob or "roro-synth" in blob)
print("Best Setups ingested:", "Best Setups" in blob or "setups-synth" in blob)
# find the evidence trail mentioning them
for sub in (d.get("conviction_sheet") or d.get("setups") or d.get("subjects") or []):
    ev=json.dumps(sub.get("evidence") or sub.get("engines") or sub)
    hits=[n for n in ("Risk Regime","Best Setups") if n in ev]
    if hits:
        print(f"  subject '{sub.get('subject') or sub.get('name')}' score {sub.get('conviction') or sub.get('score')}: includes {hits}")
print("top keys:", list(d.keys())[:12])
print("DONE 2197")
