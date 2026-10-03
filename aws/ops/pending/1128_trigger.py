"""Ops 1128: Trigger catalog rebuild."""
from __future__ import annotations
import json
import boto3
def main():
    lam = boto3.client("lambda", region_name="us-east-1")
    r = lam.invoke(FunctionName="justhodl-provider-catalog",
                   InvocationType="Event",
                   Payload=json.dumps({"ops": "1128"}).encode())
    print(json.dumps({"triggered": r["StatusCode"] == 202}))
if __name__ == "__main__":
    main()
