#!/usr/bin/env python3
"""Step 262 — Dump full schema of the freshly-generated event-study output."""
import json, os, boto3
from datetime import datetime, timezone

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"; KEY = "data/event-study.json"
REPORT_PATH = "aws/ops/reports/262_event_study_schema.json"

s3 = boto3.client("s3", region_name=REGION)
body = json.loads(s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())

# Slim summary so the report is readable
out = {
    "probed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "generated_at": body.get("generated_at"),
    "v": body.get("v"),
    "top_keys": list(body.keys()),
    "active_themes": body.get("active_themes"),
    "expected_21d_return_from_active_pct": body.get("expected_21d_return_from_active_pct"),
}

events = body.get("events") or {}
out["events_summary"] = {}
for name, ev in events.items():
    occ = ev.get("occurrences") or []
    out["events_summary"][name] = {
        "n_occurrences": len(occ),
        "currently_active": ev.get("currently_active"),
        "first_occurrence": (occ[0].get("date") if occ else None),
        "last_occurrence": (occ[-1].get("date") if occ else None),
        "sample_occurrence": occ[-1] if occ else None,
        "summary": ev.get("summary"),
        "all_keys": list(ev.keys()),
    }

os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
open(REPORT_PATH, "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps(out, indent=2, default=str)[:3500])
