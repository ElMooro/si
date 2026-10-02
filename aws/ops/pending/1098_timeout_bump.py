"""
Ops 1098: bump Lambda timeouts for engines near limits.
Writes: aws/ops/reports/1098_timeout_bump.json. Never raises.
"""
from __future__ import annotations
import json, os, sys
from datetime import datetime, timezone
OUT_PATH = os.path.join("aws", "ops", "reports", "1098_timeout_bump.json")
REGION = "us-east-1"
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "shared"))
import boto3
lam = boto3.client("lambda", region_name=REGION)
BUMPS = [("justhodl-ticker-360", 900), ("justhodl-sec-8k-enrich", 600)]
def main():
    rep = {"script": "1098_timeout_bump",
           "read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "results": []}
    for fn, new_timeout in BUMPS:
        try:
            cur = lam.get_function_configuration(FunctionName=fn)
            old = cur.get("Timeout")
            lam.update_function_configuration(FunctionName=fn, Timeout=new_timeout)
            rep["results"].append({"lambda": fn, "old": old, "new": new_timeout, "status": "updated"})
        except Exception as e:
            rep["results"].append({"lambda": fn, "status": "failed", "error": str(e)[:100]})
    rep["status"] = "COMPLETE"
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(rep, f, indent=2)
    print(json.dumps(rep["results"]))
if __name__ == "__main__":
    main()
