#!/usr/bin/env python3
"""ops 5643 — if latest Calls verb is UNKNOWN and brief is a stub, append WAIT."""
import json
from datetime import datetime, timezone

import boto3

BUCKET = "justhodl-dashboard-live"
KEY = "data/decisive-call-history.json"


def main():
    s3 = boto3.client("s3", region_name="us-east-1")
    doc = json.loads(s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
    snaps = list(doc.get("snapshots") or [])
    last = snaps[-1] if snaps else {}
    print("last", last.get("timestamp"), last.get("call_verb"), last.get("brief_chars"))
    if last.get("call_verb") == "WAIT" and (last.get("note") or "").startswith("ops 5643"):
        print("already stamped")
        return
    if last.get("call_verb") not in (None, "UNKNOWN") and (last.get("brief_chars") or 0) >= 120:
        print("real brief present; leave history")
        return
    now = datetime.now(timezone.utc).isoformat()
    row = dict(last)
    row.update({
        "timestamp": now,
        "call_verb": "WAIT",
        "note": "ops 5643: LLM brief stub (brief_chars<120). WAIT = abstain, not a forecast.",
        "brief_chars": last.get("brief_chars") or 0,
        "highest_weight_signal": last.get("highest_weight_signal"),
    })
    snaps.append(row)
    doc["snapshots"] = snaps[-500:]
    doc["n_snapshots"] = len(doc["snapshots"])
    doc["last_updated"] = now
    s3.put_object(
        Bucket=BUCKET,
        Key=KEY,
        Body=json.dumps(doc, default=str).encode(),
        ContentType="application/json",
        CacheControl="no-cache",
    )
    print("wrote WAIT", now, "n", doc["n_snapshots"])


if __name__ == "__main__":
    main()
