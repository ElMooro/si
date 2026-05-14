#!/usr/bin/env python3
"""545 — Inspect insider-clusters.json schema + audit dead refs cleanup."""
import io, json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/545_insider_clusters_schema.json"

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/insider-clusters.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["sidecar_meta"] = {
            "size_kb": round(len(body) / 1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "top_level_keys": list(p.keys()),
        }
        # Sample each top-level key
        for k in p.keys():
            v = p[k]
            if isinstance(v, list):
                out[f"{k}_summary"] = {
                    "type": "list",
                    "len": len(v),
                    "first_3": v[:3] if v else [],
                }
            elif isinstance(v, dict):
                out[f"{k}_summary"] = {
                    "type": "dict",
                    "keys": list(v.keys())[:30],
                    "n_keys": len(v),
                }
            else:
                out[f"{k}_summary"] = {"type": type(v).__name__, "value": str(v)[:200]}
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    # Check google-trends Lambda + data status (memory says blocked 429)
    try:
        l = lam.get_function(FunctionName="justhodl-google-trends")
        cfg = l["Configuration"]
        out["google_trends_lambda"] = {
            "exists": True,
            "memory": cfg.get("MemorySize"),
            "timeout": cfg.get("Timeout"),
            "last_modified": cfg.get("LastModified"),
            "state": cfg.get("State"),
        }
    except Exception as e:
        out["google_trends_lambda"] = {"exists": False, "err": str(e)[:120]}

    # Check google-trends sidecar
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/google-trends.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["google_trends_sidecar"] = {
            "size_kb": round(len(body) / 1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "top_level_keys": list(p.keys())[:10],
            "looks_empty": len(body) < 500,
        }
    except Exception as e:
        out["google_trends_sidecar"] = {"err": str(e)[:120]}

    # Verify insider-transactions retired
    try:
        lam.get_function(FunctionName="justhodl-insider-transactions")
        out["insider_transactions_lambda"] = "STILL EXISTS — should be retired"
    except Exception as e:
        out["insider_transactions_lambda"] = "correctly retired"
    try:
        s3.head_object(Bucket="justhodl-dashboard-live", Key="data/insider-transactions.json")
        out["insider_transactions_sidecar"] = "STILL EXISTS — should be deleted"
    except Exception:
        out["insider_transactions_sidecar"] = "correctly deleted"

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
