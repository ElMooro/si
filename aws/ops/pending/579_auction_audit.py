#!/usr/bin/env python3
"""579 — Examine data/auction-crisis.json schema to design A-F grader on top."""
import io, json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/579_auction_audit.json"
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Look for auction-related sidecars
    for k in ["data/auction-crisis.json", "data/auctions.json",
               "data/treasury-auctions.json", "data/auction-grades.json"]:
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=k)
            body = obj["Body"].read()
            p = json.loads(body)
            out.setdefault("sidecars", {})[k] = {
                "size_kb": round(len(body)/1024, 1),
                "modified": obj["LastModified"].isoformat()[:19],
                "top_keys": list(p.keys()) if isinstance(p, dict) else f"list({len(p)})",
            }
            # Try to extract the most relevant fields
            if isinstance(p, dict):
                # Look for auctions list
                for ak in ["auctions", "results", "recent_auctions", "scored_auctions",
                           "summary", "stats", "current_state"]:
                    if ak in p:
                        v = p[ak]
                        if isinstance(v, list) and v:
                            out["sidecars"][k][f"{ak}_n"] = len(v)
                            out["sidecars"][k][f"{ak}_sample"] = v[0] if v else None
                        elif isinstance(v, dict):
                            out["sidecars"][k][f"{ak}_keys"] = list(v.keys())[:15]
                # Sample first 1500 chars
                out["sidecars"][k]["raw_preview"] = json.dumps(p, default=str)[:1500]
        except Exception as e:
            out.setdefault("sidecars", {})[k] = f"err: {str(e)[:80]}"

    # Look for Treasury auction Lambdas
    lam = boto3.client("lambda", region_name="us-east-1")
    try:
        paginator = lam.get_paginator("list_functions")
        matches = []
        for page in paginator.paginate():
            for f in page.get("Functions", []):
                n = f["FunctionName"].lower()
                if any(kw in n for kw in ["auction", "treasury", "tga", "ust",
                                              "fiscal-data", "bond-yield"]):
                    matches.append({
                        "name": f["FunctionName"],
                        "memory": f.get("MemorySize"),
                        "timeout": f.get("Timeout"),
                        "last_modified": f.get("LastModified")[:10],
                    })
        out["auction_lambdas"] = matches
    except Exception as e:
        out["lambda_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
