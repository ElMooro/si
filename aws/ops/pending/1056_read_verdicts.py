"""
Ops 1056: read the 1053/1054/1055 verification reports from S3 and commit a
consolidated summary into the repo so it can be reviewed without S3 access.

Reads:
  s3://justhodl-dashboard-live/ops/reports/1053.json  (FINRA v2 verification)
  s3://justhodl-dashboard-live/ops/reports/1054.json  (production validation sweep)
  s3://justhodl-dashboard-live/ops/reports/1055.json  (OpenFIGI key verification)

Writes:
  aws/ops/reports/1056_summary.json  (committed back by the run-ops workflow)

Never raises on a missing/unreadable report — records the gap instead.
"""
from __future__ import annotations

import json
import os
import time

import boto3

S3_BUCKET = "justhodl-dashboard-live"
REPORT_KEYS = {
    "finra_v2": "ops/reports/1053.json",
    "prod_validation": "ops/reports/1054.json",
    "openfigi": "ops/reports/1055.json",
}
OUT_PATH = os.path.join("aws", "ops", "reports", "1056_summary.json")


def read_report(s3, key):
    """Fetch one S3 JSON report. Returns (ok, data_or_error). Never raises."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return True, json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:  # missing key, bad JSON, permissions
        return False, type(e).__name__


def summarize(name, report):
    """Extract the decision-relevant fields from one verification report."""
    out = {"overall": report.get("overall"), "checked_at": report.get("checked_at")}
    if name == "finra_v2":
        probes = report.get("api_probes", {})
        out["treasury_probe"] = {
            "parsed_ok": probes.get("treasury_filtered", {}).get("parsed_ok"),
            "rows": probes.get("treasury_filtered", {}).get("rows"),
            "http_status": probes.get("treasury_filtered", {}).get("http_status"),
        }
        out["breadth_probe"] = {
            "parsed_ok": probes.get("corporate_breadth", {}).get("parsed_ok"),
            "rows": probes.get("corporate_breadth", {}).get("rows"),
        }
        out["freshness"] = report.get("freshness")
    elif name == "prod_validation":
        upgrades = report.get("upgrades", report.get("results", {}))
        if isinstance(upgrades, dict):
            out["upgrade_verdicts"] = {
                k: (v.get("verdict") if isinstance(v, dict) else v)
                for k, v in upgrades.items()
            }
        out["overall_rollup"] = report.get("rollup", report.get("summary"))
        # keep full per-upgrade notes for anything not PASS
        notes = {}
        if isinstance(upgrades, dict):
            for k, v in upgrades.items():
                if isinstance(v, dict) and v.get("verdict") != "PASS":
                    notes[k] = {kk: vv for kk, vv in v.items()
                                if kk in ("verdict", "notes", "errors", "missing", "stale")}
        out["non_pass_details"] = notes
    elif name == "openfigi":
        out["ssm_ok"] = report.get("ssm", {}).get("ok")
        out["api_key_valid"] = report.get("api_key_valid")
        probe = report.get("openfigi_probe", {})
        out["probe"] = {k: probe.get(k) for k in ("http_status", "results", "valid")}
    return out


def main():
    """Read the three reports, write the consolidated summary, print it."""
    s3 = boto3.client("s3", region_name="us-east-1")
    summary = {
        "script": "1056_read_verdicts",
        "read_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "reports": {},
    }
    for name, key in REPORT_KEYS.items():
        ok, data = read_report(s3, key)
        if ok:
            summary["reports"][name] = summarize(name, data)
        else:
            summary["reports"][name] = {"unreadable": data, "key": key}

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(summary, f, indent=2)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
