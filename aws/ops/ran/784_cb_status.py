"""ops/784 — read-only live status pull of the central-bank liquidity stack.

Reads the deployed outputs of justhodl-ecb-detail and justhodl-cb-injection
so the live ECB / BOJ / Fed / SNB picture can be reported with current data.
"""
import json, os
from datetime import datetime, timezone
import boto3
from botocore.config import Config

s3 = boto3.client("s3", region_name="us-east-1",
                  config=Config(read_timeout=60, retries={"max_attempts": 3}))
BUCKET = "justhodl-dashboard-live"
report = {"ops": 784, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Live central-bank liquidity stack status"}


def grab(key):
    try:
        head = s3.head_object(Bucket=BUCKET, Key=key)
        body = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
        age_h = ((datetime.now(timezone.utc) - head["LastModified"])
                 .total_seconds() / 3600.0)
        return {"age_hours": round(age_h, 1), "data": body}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {str(e)[:200]}"}


report["ecb_detail"] = grab("data/ecb-detail.json")
report["cb_injection"] = grab("data/cb-injection.json")
report["eurodollar_stress"] = grab("data/eurodollar-stress.json")

print(json.dumps(report, indent=2, default=str)[:6000])
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/784_cb_status.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/784_cb_status.json")
