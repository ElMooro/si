"""ops 4367 — commit the LIVE crypto-intel.json as a test fixture so the real
page render() can be regression-tested headlessly against production data."""
import json, os, boto3
s3=boto3.client("s3",region_name="us-east-1")
raw=s3.get_object(Bucket="justhodl-dashboard-live",Key="crypto-intel.json")["Body"].read()
os.makedirs("aws/ops/reports/fixtures",exist_ok=True)
open("aws/ops/reports/fixtures/crypto-intel-live.json","wb").write(raw)
d=json.loads(raw)
print(json.dumps({"ops":4367,"bytes":len(raw),"version":d.get("version"),
                  "generated_at":d.get("generated_at"),
                  "sections":len([k for k in d])},indent=1))
