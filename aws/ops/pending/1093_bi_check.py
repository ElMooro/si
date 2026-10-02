"""Ops 1093: check best-ideas artifact version after trigger."""
from __future__ import annotations
import json, os, sys
from datetime import datetime, timezone
OUT = os.path.join("aws", "ops", "reports", "1093_bi_check.json")
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "shared"))
import boto3
s3 = boto3.client("s3", region_name="us-east-1")
def main():
    rep = {"read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    try:
        r = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/best-ideas.json")
        d = json.loads(r["Body"].read())
        stack = d.get("stack", [])
        sample = stack[0] if stack else {}
        rep["version"] = d.get("schema_version")
        rep["modified"] = r["LastModified"].strftime("%Y-%m-%dT%H:%M:%SZ")
        rep["n_ideas"] = len(stack)
        rep["sample_has_t360"] = "t360_coverage" in sample
        rep["sample_keys"] = list(sample.keys())[:12]
    except Exception as e:
        rep["error"] = str(e)[:100]
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    open(OUT, "w").write(json.dumps(rep, indent=2))
    print(json.dumps(rep, indent=2)[:1500])
if __name__ == "__main__":
    main()
