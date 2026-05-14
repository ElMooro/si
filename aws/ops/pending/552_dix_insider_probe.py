#!/usr/bin/env python3
"""552 — Probe dix-history.json + insider-clusters.json stats to fix derivations."""
import io, json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/552_dix_insider_probe.json"
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # DIX
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/dix-history.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["dix"] = {
            "size_kb": round(len(body) / 1024, 1),
            "top_keys": list(p.keys())[:10],
        }
        # Find the array of records
        for k, v in p.items():
            if isinstance(v, list):
                out["dix"][f"list_{k}_len"] = len(v)
                if v:
                    out["dix"][f"list_{k}_sample"] = v[:3]
                    out["dix"][f"list_{k}_last"] = v[-3:]
                    if isinstance(v[-1], dict):
                        out["dix"][f"list_{k}_keys"] = list(v[-1].keys())
    except Exception as e:
        out["dix_err"] = str(e)[:200]

    # Insider stats
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/insider-clusters.json")
        p = json.loads(obj["Body"].read())
        stats = p.get("stats") or {}
        out["insider_stats"] = stats
        # Find clusters with signal_type breakdown
        sig_counts = {}
        for c in p.get("clusters") or []:
            st = c.get("signal_type")
            sig_counts[st] = sig_counts.get(st, 0) + 1
        out["insider_signal_type_counts"] = sig_counts
    except Exception as e:
        out["insider_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
