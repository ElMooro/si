#!/usr/bin/env python3
"""Step 312 — Inspect pairs-scanner.json schema to design pairs.html."""
import json
import os
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
KEY = "data/pairs-scanner.json"
REPORT = "aws/ops/reports/312_pairs_schema.json"

s3 = boto3.client("s3", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    obj = s3.get_object(Bucket=BUCKET, Key=KEY)
    data = json.loads(obj["Body"].read())

    out["size_kb"] = round(obj["ContentLength"] / 1024, 1)
    out["last_modified"] = obj["LastModified"].isoformat()
    out["top_keys"] = list(data.keys())
    out["summary"] = data.get("summary")

    # Sample first pair fully
    pairs = data.get("pairs", [])
    if pairs:
        out["n_pairs"] = len(pairs)
        out["sample_pair_fields"] = list(pairs[0].keys())
        out["sample_pair_full"] = pairs[0]

    # Get top 10 by abs(z_score)
    valid = [p for p in pairs if p.get("z_score") is not None]
    valid.sort(key=lambda p: abs(p.get("z_score", 0)), reverse=True)
    out["top_10_stretched"] = [
        {k: p.get(k) for k in ("name", "leg_a", "leg_b", "category",
                                "z_score", "half_life_days", "correlation_252d",
                                "classification", "ratio", "ratio_60d_mean",
                                "polarity", "implication")}
        for p in valid[:10]
    ]

    # Distribution by classification
    from collections import Counter
    cls_counts = Counter(p.get("classification") for p in pairs)
    out["classification_counts"] = dict(cls_counts)
    cat_counts = Counter(p.get("category") for p in pairs)
    out["category_counts"] = dict(cat_counts)

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:5500])


if __name__ == "__main__":
    main()
