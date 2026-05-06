#!/usr/bin/env python3
"""Step 263 — Schema is 'studies' not 'events'. Re-dump."""
import json, os, boto3
from datetime import datetime, timezone

s3 = boto3.client("s3", region_name="us-east-1")
body = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/event-study.json")["Body"].read())

out = {
    "probed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "version": body.get("version"),
    "as_of_date": body.get("as_of_date"),
    "horizons_trading_days": body.get("horizons_trading_days"),
    "active_themes": body.get("active_themes"),
    "n_studies": len(body.get("studies") or {}),
    "data_sources": body.get("data_sources"),
    "methodology": (body.get("methodology") or "")[:500],
}

studies = body.get("studies") or {}
out["studies_summary"] = {}
for name, st in studies.items():
    occ = st.get("occurrences") or []
    out["studies_summary"][name] = {
        "all_keys": list(st.keys()),
        "n_occurrences": len(occ),
        "currently_active": st.get("currently_active"),
        "summary": st.get("summary"),
        "sample_occurrence": occ[-1] if occ else None,
    }

os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/263_event_study_studies.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps(out, indent=2, default=str)[:5000])
