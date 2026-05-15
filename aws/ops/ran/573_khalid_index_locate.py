#!/usr/bin/env python3
"""573 — Locate Khalid Index computation Lambda + audit calibration snapshot."""
import io, json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/573_khalid_index_locate.json"
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Read calibration snapshot to understand schema
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="data/calibration-snapshot.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["calibration_snapshot"] = {
            "size_kb": round(len(body)/1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "top_keys": list(p.keys()) if isinstance(p, dict) else f"list({len(p)})",
        }
        if isinstance(p, dict):
            # Sample first level
            for k, v in list(p.items())[:15]:
                if isinstance(v, (dict, list)):
                    out["calibration_snapshot"][f"key_{k}"] = (
                        f"dict[{len(v)}]: {list(v.keys())[:8]}" if isinstance(v, dict)
                        else f"list[{len(v)}]"
                    )
                else:
                    out["calibration_snapshot"][f"key_{k}"] = str(v)[:200]
        out["calibration_sample"] = json.dumps(p, default=str)[:3000]
    except Exception as e:
        out["calibration_err"] = str(e)[:200]

    # Find what writes data/report.json (search Lambdas)
    candidate_lambdas = [
        "justhodl-daily-report-v3",
        "justhodl-daily-report",
        "justhodl-khalid-index",
        "justhodl-secretary",
        "justhodl-aggregator",
        "justhodl-master-ranker",
    ]
    for name in candidate_lambdas:
        try:
            cfg = lam.get_function_configuration(FunctionName=name)
            env = (cfg.get("Environment") or {}).get("Variables", {}) or {}
            out.setdefault("candidates", {})[name] = {
                "exists": True,
                "memory": cfg.get("MemorySize"),
                "timeout": cfg.get("Timeout"),
                "last_modified": cfg.get("LastModified"),
                "env_keys": sorted(env.keys()),
            }
        except Exception:
            out.setdefault("candidates", {})[name] = {"exists": False}

    # Scan report.json for khalid_index structure
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/report.json")
        body = obj["Body"].read().decode("utf-8")
        # Search for khalid_index sub-dict
        import re
        # Find khalid_index value
        m = re.search(r'"khalid_index"\s*:\s*({[^}]{,3000}})', body)
        if m:
            try:
                ki = json.loads(m.group(1))
                out["khalid_index_in_report"] = {
                    "keys": list(ki.keys()),
                    "sample": {k: (str(v)[:120] if not isinstance(v,(dict,list)) else f"{type(v).__name__}[{len(v)}]") for k,v in ki.items()},
                }
            except: out["khalid_index_in_report"] = m.group(1)[:1500]
        else:
            # Try simpler match
            m2 = re.search(r'"khalid_index"\s*:\s*([0-9.]+)', body)
            if m2:
                out["khalid_index_value"] = m2.group(1)
            else:
                out["khalid_index_no_match"] = True
    except Exception as e:
        out["report_scan_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
