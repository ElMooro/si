#!/usr/bin/env python3
"""Step 271 — Probe data/report.json schema to find FRED series."""
import json, os, boto3
from datetime import datetime, timezone

s3 = boto3.client("s3", region_name="us-east-1")
report = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/report.json")["Body"].read())

out = {
    "probed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "top_keys": list(report.keys())[:50],
    "size_summary": {k: type(v).__name__ for k, v in list(report.items())[:50]},
}

# Look for INDPRO anywhere in the document
def find_series(obj, target, path="", depth=0, max_depth=6):
    found = []
    if depth > max_depth:
        return found
    if isinstance(obj, dict):
        if target in obj:
            sample = obj[target]
            found.append({
                "path": f"{path}.{target}" if path else target,
                "type": type(sample).__name__,
                "keys_or_len": (list(sample.keys())[:10] if isinstance(sample, dict)
                                else len(sample) if isinstance(sample, list) else None),
                "sample": (sample if not isinstance(sample, (dict, list))
                           else (sample[:2] if isinstance(sample, list)
                                 else {k: type(v).__name__ for k, v in list(sample.items())[:8]})),
            })
        for k, v in obj.items():
            if isinstance(v, (dict, list)):
                found.extend(find_series(v, target, f"{path}.{k}" if path else k, depth+1, max_depth))
    elif isinstance(obj, list):
        for i, item in enumerate(obj[:5]):
            if isinstance(item, (dict, list)):
                found.extend(find_series(item, target, f"{path}[{i}]", depth+1, max_depth))
    return found

out["INDPRO_locations"] = find_series(report, "INDPRO")
out["PAYEMS_locations"] = find_series(report, "PAYEMS")
out["UMCSENT_locations"] = find_series(report, "UMCSENT")

# If 'macro' is a top-level key, sample it
if "macro" in report:
    m = report["macro"]
    if isinstance(m, dict):
        out["macro_keys"] = list(m.keys())[:30]
        out["macro_sample"] = {k: type(v).__name__ for k, v in list(m.items())[:5]}
        for k, v in list(m.items())[:3]:
            out[f"macro_{k}_shape"] = (
                {kk: type(vv).__name__ for kk, vv in v.items()} if isinstance(v, dict)
                else f"<{type(v).__name__} len={len(v) if hasattr(v,'__len__') else '?'}>"
            )

os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/271_report_schema_probe.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps(out, indent=2, default=str)[:5000])
