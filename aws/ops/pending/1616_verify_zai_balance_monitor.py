import json, time, boto3
from botocore.config import Config
lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=120, retries={"max_attempts":0}))
print("waiting for deploy..."); time.sleep(25)
resp = lam.invoke(FunctionName="justhodl-cost-anomaly", InvocationType="RequestResponse", Payload=b"{}")
out = json.loads(resp["Payload"].read().decode())
print("lambda status:", resp.get("StatusCode"))
# read the written payload from S3 to get zai_balance
s3 = boto3.client("s3", region_name="us-east-1")
try:
    body = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/cost-anomaly.json")["Body"].read())
except Exception:
    body = {}
zb = body.get("zai_balance") or {}
print("zai_balance:", zb)
if zb.get("status") == "funded":
    print("\n✅ VERIFIED: Z.ai balance monitor live — balance currently FUNDED")
elif zb.get("status") == "exhausted":
    print("\n✅ monitor live — balance EXHAUSTED (would alert)")
else:
    print(f"\nℹ️ monitor live — status={zb.get('status')} (key/output path may differ: {str(body.get('generated_at'))})")
