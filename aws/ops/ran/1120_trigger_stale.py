"""Ops 1120: Manually trigger stale feed Lambdas."""
from __future__ import annotations
import json, os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "shared"))
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
def main():
    out = {}
    for fn in ["justhodl-sec-8k-enrich", "justhodl-master-ranker", "justhodl-conviction-engine"]:
        try:
            r = lam.invoke(FunctionName=fn, InvocationType="Event",
                           Payload=json.dumps({"ops": "1120"}).encode())
            out[fn] = {"triggered": True, "code": r["StatusCode"]}
        except Exception as e:
            out[fn] = {"triggered": False, "error": str(e)[:60]}
    print(json.dumps(out, indent=2))
if __name__ == "__main__":
    main()
