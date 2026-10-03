"""Ops 1127: Trigger catalog rebuild after successful deploy."""
from __future__ import annotations
import json
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
def main():
    r = lam.invoke(FunctionName="justhodl-provider-catalog",
                   InvocationType="Event",
                   Payload=json.dumps({"ops": "1127"}).encode())
    print(json.dumps({"triggered": r["StatusCode"] == 202}))
if __name__ == "__main__":
    main()
