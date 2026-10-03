"""Ops 1126: Trigger provider-catalog rebuild (post-deploy)."""
from __future__ import annotations
import json, os, sys
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
def main():
    r = lam.invoke(FunctionName="justhodl-provider-catalog",
                   InvocationType="Event",
                   Payload=json.dumps({"ops": "1126"}).encode())
    print(json.dumps({"triggered": r["StatusCode"] == 202}))
if __name__ == "__main__":
    main()
