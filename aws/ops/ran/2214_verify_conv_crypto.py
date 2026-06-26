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
print("Crypto Confluence ingested:", "Crypto Confluence" in blob or "crypto-synth" in blob)
for sub in (d.get("setups") or d.get("single_names") or []):
    ev=json.dumps(sub)
    if "Crypto Confluence" in ev:
        print(f"  subject '{sub.get('subject') or sub.get('name')}' score {sub.get('conviction') or sub.get('score')}: crypto-confluence present")
        break
print("DONE 2214")
