"""Ops 1105: audit calibration weights system."""
from __future__ import annotations
import json, os, sys
from datetime import datetime, timezone
OUT = os.path.join("aws", "ops", "reports", "1105_calibration.json")
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "shared"))
import boto3
ssm = boto3.client("ssm", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
def main():
    rep = {"read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    # Check SSM calibration weights
    try:
        r = ssm.get_parameter(Name="/justhodl/calibration/weights")
        w = json.loads(r["Parameter"]["Value"])
        rep["ssm_weights"] = {"exists": True,
                              "n_weights": len(w) if isinstance(w, dict) else "n/a",
                              "keys": list(w.keys())[:10] if isinstance(w, dict) else None}
    except Exception as e:
        rep["ssm_weights"] = {"exists": False, "error": str(e)[:80]}
    # Check for calibration artifacts in S3
    for key in ["data/calibration/weights.json", "data/engine-calibration.json"]:
        try:
            r = s3.head_object(Bucket="justhodl-dashboard-live", Key=key)
            rep[key] = {"exists": True,
                        "modified": r["LastModified"].strftime("%Y-%m-%dT%H:%M:%SZ")}
        except Exception:
            rep[key] = {"exists": False}
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    open(OUT,"w").write(json.dumps(rep,indent=2))
    print(json.dumps(rep, indent=2)[:1500])
if __name__=="__main__":
    main()
