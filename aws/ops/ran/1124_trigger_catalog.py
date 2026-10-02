"""Ops 1124: Trigger provider-catalog rebuild."""
from __future__ import annotations
import json, os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "shared"))
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
def main():
    r = lam.invoke(FunctionName="justhodl-provider-catalog",
                   InvocationType="Event",
                   Payload=json.dumps({"ops": "1124", "trigger": "catalog-rebuild"}).encode())
    print(json.dumps({"triggered": r["StatusCode"] == 202}))
if __name__ == "__main__":
    main()
