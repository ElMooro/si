#!/usr/bin/env python3
"""ops 5619 — invoke justhodl-liquidity-flow so quality + formula_note publish."""
import json, boto3
from botocore.config import Config

FN = "justhodl-liquidity-flow"
BUCKET = "justhodl-dashboard-live"
KEY = "data/liquidity-flow.json"

def main():
    lam = boto3.client("lambda", config=Config(read_timeout=60, retries={"max_attempts": 0}))
    s3 = boto3.client("s3")
    raw = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
    feed = json.loads(s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
    print(json.dumps({
        "ops": 5619,
        "invoke_status": raw.get("StatusCode"),
        "fn_error": raw.get("FunctionError"),
        "as_of": feed.get("as_of"),
        "regime": feed.get("regime"),
        "formula": feed.get("formula"),
        "quality": feed.get("quality"),
        "generated_at": feed.get("generated_at"),
    }, indent=2, default=str))
    assert feed.get("formula") == "WALCL - WTREGEN - RRPONTSYD"
    assert (feed.get("quality") or {}).get("status")

if __name__ == "__main__":
    main()
