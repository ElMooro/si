"""Ops 1111: Trigger ticker-360 after 1110 patch to verify improved coverage."""
from __future__ import annotations
import json, os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "shared"))
import boto3

lam = boto3.client("lambda", region_name="us-east-1")

def main():
    resp = lam.invoke(
        FunctionName="justhodl-ticker-360",
        InvocationType="Event",
        Payload=json.dumps({"ops": "1111", "reason": "verify 1110 ticker key patch"}).encode(),
    )
    print(json.dumps({"status_code": resp["StatusCode"], "triggered": True}))

if __name__ == "__main__":
    main()
