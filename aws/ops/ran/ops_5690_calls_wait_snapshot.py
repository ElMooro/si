#!/usr/bin/env python3
"""ops 5690 — stamp WAIT when the latest Calls snapshot is an UNKNOWN stub."""
import json
import sys
from datetime import datetime, timezone

import boto3

BUCKET = "justhodl-dashboard-live"
KEY = "data/decisive-call-history.json"


def main():
    try:
        s3 = boto3.client("s3", region_name="us-east-1")
        doc = json.loads(s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
        snaps = list(doc.get("snapshots") or [])
        last = snaps[-1] if snaps else {}
        print("last", last.get("timestamp"), last.get("call_verb"), last.get("brief_chars"))
        if last.get("call_verb") == "WAIT" and "5690" in str(last.get("note") or ""):
            print("already WAIT")
            return 0
        if last.get("call_verb") not in (None, "UNKNOWN") and (last.get("brief_chars") or 0) >= 120:
            print("real brief; skip")
            return 0
        now = datetime.now(timezone.utc).isoformat()
        row = dict(last)
        row.update({
            "timestamp": now,
            "call_verb": "WAIT",
            "note": "ops 5690: LLM brief stub. WAIT = abstain.",
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
        print("wrote WAIT", now)
        return 0
    except Exception as e:
        print("FAIL", type(e).__name__, e)
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main() or 0)
