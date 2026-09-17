#!/usr/bin/env python3
"""ops 5615 — invoke justhodl-sovereign-stress after 2.4.7 is live."""
import json, time, boto3
from botocore.config import Config

FN = "justhodl-sovereign-stress"
BUCKET = "justhodl-dashboard-live"
KEY = "data/sovereign-stress.json"

def main():
    lam = boto3.client("lambda", config=Config(read_timeout=400, retries={"max_attempts": 0}))
    s3 = boto3.client("s3")
    raw = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
    feed = json.loads(s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
    print(json.dumps({
        "ops": 5615,
        "invoke_status": raw.get("StatusCode"),
        "fn_error": raw.get("FunctionError"),
        "version": feed.get("version"),
        "generated_at": feed.get("generated_at"),
        "quality": feed.get("quality"),
        "most_stressed_sovereign": feed.get("most_stressed_sovereign"),
        "uk_yoy": ((feed.get("systemic_stress_ciss") or {}).get("united_kingdom") or {}).get("yoy_pct"),
        "taiwan_cds": ((feed.get("asia_sovereigns") or {}).get("taiwan") or {}).get("cds_bp"),
        "taiwan_def": ((feed.get("asia_sovereigns") or {}).get("taiwan") or {}).get("cds_default_prob_pct"),
    }, indent=2, default=str))
    assert feed.get("version") == "2.4.7"

if __name__ == "__main__":
    main()
