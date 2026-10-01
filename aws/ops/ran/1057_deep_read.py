"""
Ops 1057: deep-read the 1053 (FINRA) and 1054 (prod validation) S3 reports and
commit the actionable details into the repo.

The 1056 summary showed:
  - 1053 overall=FAIL_API (treasury probe HTTP 400) — need body_preview to diagnose
  - 1054 overall=FAIL (6 PASS / 1 DEGRADED / 8 FAIL upgrades, 9/9 R-engines FAIL)
    — need per-upgrade verdicts + notes; the 1056 summary's structure guess missed them

Reads:
  s3://justhodl-dashboard-live/ops/reports/1053.json
  s3://justhodl-dashboard-live/ops/reports/1054.json

Writes:
  aws/ops/reports/1057_details.json  (committed back by the run-ops workflow)

Never raises on a missing/unreadable report — records the gap instead.
Output is capped to stay committable (per-upgrade notes truncated).
"""
from __future__ import annotations

import json
import os
import time

import boto3

S3_BUCKET = "justhodl-dashboard-live"
OUT_PATH = os.path.join("aws", "ops", "reports", "1057_details.json")
NOTE_CAP = 1500  # chars per note field


def read_report(s3, key):
    """Fetch one S3 JSON report. Returns (ok, data_or_error). Never raises."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return True, json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:  # missing key, bad JSON, permissions
        return False, type(e).__name__


def cap(obj):
    """Truncate long strings in a nested structure. Never raises."""
    try:
        if isinstance(obj, str):
            return obj[:NOTE_CAP] + ("..." if len(obj) > NOTE_CAP else "")
        if isinstance(obj, dict):
            return {k: cap(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [cap(v) for v in obj[:50]]
        return obj
    except Exception:  # defensive: never break the report on weird data
        return "<unserializable>"


def finra_details(report):
    """Extract every probe's status + body preview from the 1053 report."""
    out = {"overall": report.get("overall")}
    probes = report.get("api_probes", {})
    for name, probe in probes.items():
        if not isinstance(probe, dict):
            continue
        out[name] = {
            k: probe.get(k) for k in (
                "http_status", "parsed_ok", "parse_error", "rows",
                "content_type", "trade_date_queried", "fields_present",
            )
        }
        out[name]["body_preview"] = (probe.get("body_preview") or "")[:NOTE_CAP]
    out["shared_module"] = report.get("shared_module")
    return out


def prod_details(report):
    """Extract per-upgrade verdicts + notes from the 1054 report, any structure."""
    out = {"overall": report.get("overall"), "checked_at": report.get("checked_at")}
    # Find the per-upgrade mapping wherever it lives
    candidates = []
    for key in ("upgrades", "results", "lambdas", "checks", "engines"):
        val = report.get(key)
        if isinstance(val, dict) and val:
            candidates.append((key, val))
    # Also scan one level deep for dicts that look like verdict maps
    for key, val in report.items():
        if isinstance(val, dict):
            for k2, v2 in val.items():
                if isinstance(v2, dict) and v2:
                    candidates.append((f"{key}.{k2}", v2))
    out["top_level_keys"] = sorted(report.keys())
    found = {}
    for cname, cmap in candidates:
        verdicts = {}
        for uk, uv in cmap.items():
            if isinstance(uv, dict):
                verdicts[uk] = {
                    "verdict": uv.get("verdict", uv.get("status")),
                    "notes": cap(uv.get("notes", uv.get("detail", uv.get("error")))),
                }
            else:
                verdicts[uk] = {"verdict": uv}
        if verdicts:
            found[cname] = verdicts
    out["verdict_maps"] = found
    # R-engine / ops-1051 section, if present under any key
    for key in ("r_engines", "ops_1051", "r1051", "schedules"):
        if key in report:
            out[key] = cap(report[key])
    out["rollup"] = report.get("rollup", report.get("summary"))
    return out


def main():
    """Read both reports, write the details file, print it."""
    s3 = boto3.client("s3", region_name="us-east-1")
    details = {
        "script": "1057_deep_read",
        "read_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    ok, data = read_report(s3, "ops/reports/1053.json")
    details["finra_1053"] = finra_details(data) if ok else {"unreadable": data}
    ok, data = read_report(s3, "ops/reports/1054.json")
    details["prod_1054"] = prod_details(data) if ok else {"unreadable": data}

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(details, f, indent=2)
    print(json.dumps(details, indent=2)[:20000])


if __name__ == "__main__":
    main()
