#!/usr/bin/env python3
"""ops 5622 — invoke detector so public JSON uses p_soft_demand_30d."""
import json, boto3
from botocore.config import Config

FN = "justhodl-auction-crisis-detector"
BUCKET = "justhodl-dashboard-live"
KEY = "data/auction-crisis.json"

def _walk(o, found):
    if isinstance(o, dict):
        for k, v in o.items():
            if k in ("p_soft_demand_30d", "p_failed_auction_30d"):
                found[k] = v
            _walk(v, found)
    elif isinstance(o, list):
        for x in o:
            _walk(x, found)

def main():
    lam = boto3.client("lambda", config=Config(read_timeout=180, retries={"max_attempts": 0}))
    s3 = boto3.client("s3")
    raw = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
    feed = json.loads(s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
    keys = {}
    _walk(feed, keys)
    print(json.dumps({
        "ops": 5622,
        "invoke_status": raw.get("StatusCode"),
        "fn_error": raw.get("FunctionError"),
        "generated_at": feed.get("generated_at"),
        "keys_found": list(keys),
        "soft": keys.get("p_soft_demand_30d"),
        "old": keys.get("p_failed_auction_30d"),
    }, indent=2, default=str))
    assert "p_soft_demand_30d" in keys
    assert "p_failed_auction_30d" not in keys

if __name__ == "__main__":
    main()
