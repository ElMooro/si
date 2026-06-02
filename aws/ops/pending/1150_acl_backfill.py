"""1150 — backfill public-read ACL on all existing equity-research files.

Without public-read ACL the CF proxy can't serve them, defeating the
purpose of the S3 cache + pre-warm strategy.
"""
import json, pathlib
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1150_acl_backfill.json"
S3_BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "prefix_results": {}}

    for prefix in ["equity-research/", "equity-prewarm/runs/"]:
        results = {"fixed": [], "failed": [], "already_public": []}
        # List all objects under prefix
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in (page.get("Contents") or []):
                key = obj["Key"]
                try:
                    # Quick check: does the current ACL already include public read?
                    acl = s3.get_object_acl(Bucket=S3_BUCKET, Key=key)
                    has_public = any(
                        g.get("Grantee", {}).get("URI", "").endswith("AllUsers")
                        and g.get("Permission") in ("READ", "FULL_CONTROL")
                        for g in acl.get("Grants", [])
                    )
                    if has_public:
                        results["already_public"].append(key)
                        continue
                    # Apply public-read
                    s3.put_object_acl(Bucket=S3_BUCKET, Key=key, ACL="public-read")
                    results["fixed"].append(key)
                except Exception as e:
                    results["failed"].append({"key": key, "error": str(e)[:200]})
        out["prefix_results"][prefix] = {
            "n_fixed":          len(results["fixed"]),
            "n_already_public": len(results["already_public"]),
            "n_failed":         len(results["failed"]),
            "fixed_keys":       results["fixed"][:10],
            "failed":           results["failed"][:5],
        }
        print(f"[1150] {prefix}: fixed={len(results['fixed'])} already_public={len(results['already_public'])} failed={len(results['failed'])}")

    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1150] DONE")


if __name__ == "__main__":
    main()
