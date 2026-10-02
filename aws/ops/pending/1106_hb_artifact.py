"""Ops 1106: check feed-heartbeat.json artifact."""
from __future__ import annotations
import json, os, sys
from datetime import datetime, timezone
OUT = os.path.join("aws", "ops", "reports", "1106_hb_artifact.json")
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "shared"))
import boto3
s3 = boto3.client("s3", region_name="us-east-1")
def main():
    rep = {"read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    try:
        r = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/feed-heartbeat.json")
        d = json.loads(r["Body"].read())
        rep["exists"] = True
        rep["system_status"] = d.get("system_status")
        rep["n_feeds"] = d.get("n_feeds")
        rep["n_fresh"] = d.get("n_fresh")
        rep["n_stale"] = d.get("n_stale")
        rep["n_missing"] = d.get("n_missing")
        rep["n_alerts"] = len(d.get("alerts", []))
        rep["generated_at"] = d.get("generated_at")
        # List stale/missing for visibility
        rep["problem_feeds"] = [
            {"feed": a["feed"], "status": a["status"]}
            for a in d.get("alerts", [])[:10]]
    except Exception as e:
        rep["exists"] = False
        rep["error"] = str(e)[:120]
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    open(OUT,"w").write(json.dumps(rep,indent=2))
    print(json.dumps(rep, indent=2)[:2000])
if __name__=="__main__":
    main()
