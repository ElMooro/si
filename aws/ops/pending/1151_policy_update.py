"""1151 — update bucket policy to allow public read on equity-research/* and equity-prewarm/*.

The bucket has ACLs disabled (BucketOwnerEnforced ownership). All public
access goes through the bucket policy. Current policy allows data/* and
screener/*. We need to add the research prefixes.

Strategy:
  1. Read the current bucket policy
  2. Add new Statements for equity-research/* (publicly readable)
                          and equity-prewarm/* (publicly readable)
  3. Write the updated policy back
"""
import json, pathlib, copy
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1151_policy_update.json"
S3_BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # 1. Read existing policy
    try:
        existing = json.loads(s3.get_bucket_policy(Bucket=S3_BUCKET)["Policy"])
        out["existing_policy_sids"] = [s.get("Sid") for s in existing.get("Statement", [])]
    except s3.exceptions.from_code("NoSuchBucketPolicy"):
        existing = {"Version": "2012-10-17", "Statement": []}
        out["existing_policy_sids"] = ["(no existing policy)"]
    except Exception as e:
        out["error_reading_policy"] = str(e)[:300]
        existing = {"Version": "2012-10-17", "Statement": []}

    statements = existing.get("Statement", [])

    # Helper to upsert a statement
    def upsert(sid, prefix):
        # Replace existing or append
        new_stmt = {
            "Sid":       sid,
            "Effect":    "Allow",
            "Principal": "*",
            "Action":    "s3:GetObject",
            "Resource":  f"arn:aws:s3:::{S3_BUCKET}/{prefix}",
        }
        for i, s in enumerate(statements):
            if s.get("Sid") == sid:
                statements[i] = new_stmt
                return "updated"
        statements.append(new_stmt)
        return "added"

    out["actions"] = {
        "PublicReadEquityResearch":  upsert("PublicReadEquityResearch",  "equity-research/*"),
        "PublicReadEquityPrewarm":   upsert("PublicReadEquityPrewarm",   "equity-prewarm/*"),
    }

    new_policy = {"Version": "2012-10-17", "Statement": statements}
    out["new_policy_sids"] = [s.get("Sid") for s in statements]
    out["new_policy_size"] = len(json.dumps(new_policy))

    # 2. Write updated policy
    try:
        s3.put_bucket_policy(Bucket=S3_BUCKET, Policy=json.dumps(new_policy))
        out["put_policy_result"] = "ok"
    except Exception as e:
        out["put_policy_result"] = f"error: {str(e)[:300]}"

    # 3. Verify by reading back
    try:
        verify = json.loads(s3.get_bucket_policy(Bucket=S3_BUCKET)["Policy"])
        out["verified_sids"] = [s.get("Sid") for s in verify.get("Statement", [])]
        # Print the new policy statements that were added
        out["new_statements_text"] = [
            s for s in verify.get("Statement", [])
            if s.get("Sid") in ("PublicReadEquityResearch", "PublicReadEquityPrewarm")
        ]
    except Exception as e:
        out["verify_error"] = str(e)[:300]

    # 4. Test CF proxy on a previously-403'd file
    import urllib.request, urllib.error
    out["cf_proxy_post_fix"] = {}
    for t in ["UBER", "CRWD", "ZM"]:
        url = f"https://justhodl-data-proxy.raafouis.workers.dev/equity-research/{t}.json"
        try:
            with urllib.request.urlopen(url, timeout=5) as r:
                body = r.read()
                out["cf_proxy_post_fix"][t] = {"http": r.status, "size_kb": round(len(body)/1024, 1)}
        except urllib.error.HTTPError as e:
            out["cf_proxy_post_fix"][t] = {"http_error": e.code, "msg": e.reason}
        except Exception as e:
            out["cf_proxy_post_fix"][t] = {"error": str(e)[:200]}

    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1151] DONE")


if __name__ == "__main__":
    main()
