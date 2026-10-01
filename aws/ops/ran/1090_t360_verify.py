"""
Ops 1090: verify t360 fields in flow-confluence and best-ideas artifacts.
Writes: aws/ops/reports/1090_t360_verify.json. Never raises.
"""
from __future__ import annotations
import json, os, sys
from datetime import datetime, timezone
OUT_PATH = os.path.join("aws", "ops", "reports", "1090_t360_verify.json")
BUCKET = "justhodl-dashboard-live"
REGION = "us-east-1"
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "shared"))
import boto3
from botocore.exceptions import ClientError
s3 = boto3.client("s3", region_name=REGION)

def check(key, fn):
    try:
        r = s3.get_object(Bucket=BUCKET, Key=key)
        d = json.loads(r["Body"].read())
        lm = r["LastModified"]
        age = (datetime.now(timezone.utc) - lm).total_seconds() / 3600
        ok = fn(d)
        return {"exists": True, "age_h": round(age, 1), "t360_ok": ok is True, "detail": ok}
    except ClientError:
        return {"exists": False}
    except Exception as e:
        return {"exists": "unknown", "error": str(e)[:80]}

def main():
    rep = {"script": "1090_t360_verify",
           "read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "artifacts": {}}
    def fc(d):
        e = d.get("entries", d.get("flows", []))
        if not e: return "no entries"
        s = e[0] if isinstance(e, list) else list(e.values())[0]
        c = d.get("counts", {})
        return ("t360_coverage" in s and "t360_domains" in s
                and "t360_indexed" in c and "cross_validated_tickers" in d)
    def bi(d):
        ideas = d.get("ideas", d.get("top_ideas", d.get("stack", [])))
        if not ideas: return "no ideas"
        s = ideas[0]
        return "t360_coverage" in s and "t360_domains" in s
    rep["artifacts"]["flow-confluence"] = check("data/flow-confluence.json", fc)
    rep["artifacts"]["best-ideas"] = check("data/best-ideas.json", bi)
    rep["status"] = "COMPLETE"
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(rep, f, indent=2)
    print(json.dumps({k: v.get("t360_ok") for k, v in rep["artifacts"].items()}))

if __name__ == "__main__":
    main()
