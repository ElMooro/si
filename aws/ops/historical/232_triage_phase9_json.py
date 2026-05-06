#!/usr/bin/env python3
"""Step 232 — triage Phase 9 health audit findings.

The e2e health audit found two real issues:
  - crisis-plumbing.json: 0/6 plumbing tier_2 series populated
  - regime-anomaly.json: reports ka_index_n_obs=0 (or field-mismatch?)

Print FULL structure + key values of both files so we can fix.
"""
import json
from ops_report import report
import boto3

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def dump_struct(obj, prefix="", max_depth=4, depth=0):
    """Recursively print structure with leaf values."""
    out = []
    if depth >= max_depth:
        out.append(f"{prefix}…(truncated at depth {max_depth})")
        return out
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, (dict, list)):
                if isinstance(v, dict) and not v:
                    out.append(f"{prefix}{k}: {{}} (empty dict)")
                elif isinstance(v, list) and not v:
                    out.append(f"{prefix}{k}: [] (empty list)")
                else:
                    out.append(f"{prefix}{k}:")
                    out.extend(dump_struct(v, prefix + "  ", max_depth, depth + 1))
            else:
                out.append(f"{prefix}{k}: {v!r}")
    elif isinstance(obj, list):
        out.append(f"{prefix}[{len(obj)} items, first={obj[0] if obj else None!r}]")
    return out


with report("triage_phase9_json") as r:
    r.heading("Triage Phase 9 JSON outputs — full structure")

    for key in ("data/crisis-plumbing.json", "data/regime-anomaly.json"):
        r.section(f"📄 {key}")
        try:
            body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode("utf-8")
            d = json.loads(body)
            r.log(f"  total bytes: {len(body)}")
            r.log(f"  top-level keys: {list(d.keys())}")
            r.log("")
            for line in dump_struct(d, "    ", max_depth=4):
                r.log(line)
        except Exception as e:
            r.warn(f"  ✗ {e}")
        r.log("")
        r.log("")

    r.log("Done")
