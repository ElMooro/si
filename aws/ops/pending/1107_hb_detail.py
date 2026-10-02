"""Ops 1107: get heartbeat schedule details and fix issues."""
from __future__ import annotations
import json, os, sys
from datetime import datetime, timezone
OUT = os.path.join("aws", "ops", "reports", "1107_hb_detail.json")
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "shared"))
import boto3
s3 = boto3.client("s3", region_name="us-east-1")
sched = boto3.client("scheduler", region_name="us-east-1")
def main():
    rep = {"read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    # Get full heartbeat
    try:
        r = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/feed-heartbeat.json")
        hb = json.loads(r["Body"].read())
        feeds = hb.get("feeds", {})
        sched_feed = feeds.get("schedules", {})
        rep["schedule_detail"] = sched_feed.get("detail", {})
        # Also check sec-8k artifact age
        sec8k = feeds.get("sec-8k", {})
        rep["sec8k"] = {"status": sec8k.get("status"),
                        "age_min": sec8k.get("age_minutes"),
                        "last_modified": sec8k.get("last_modified")}
    except Exception as e:
        rep["error"] = str(e)[:150]
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    open(OUT,"w").write(json.dumps(rep,indent=2))
    print(json.dumps(rep, indent=2)[:2500])
if __name__=="__main__":
    main()
