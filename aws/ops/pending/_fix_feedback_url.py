"""Create Function URL for justhodl-feedback (CORS-compliant) + write feedback-url.json to S3."""
import json
import time
import boto3
from ops_report import report

REGION = "us-east-1"
FN = "justhodl-feedback"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    with report("fix_feedback_url") as r:
        r.heading("Create Function URL for justhodl-feedback + publish manifest")

        # AWS Lambda Function URL CORS: allowMethods items must be ≤6 chars or "*"
        # OPTIONS handled automatically by CORS preflight; no need to list it
        cors = {
            "AllowOrigins": ["*"],
            "AllowMethods": ["GET", "POST", "*"],  # all ≤6 chars
            "AllowHeaders": ["content-type", "x-justhodl-token"],
            "MaxAge": 3600,
        }

        try:
            existing = lam.get_function_url_config(FunctionName=FN)
            r.log(f"  ✓ existing URL: {existing['FunctionUrl']}")
            lam.update_function_url_config(
                FunctionName=FN, AuthType="NONE", Cors=cors,
            )
            url = existing["FunctionUrl"]
            r.ok(f"  ✓ updated CORS")
        except lam.exceptions.ResourceNotFoundException:
            resp = lam.create_function_url_config(
                FunctionName=FN, AuthType="NONE", Cors=cors,
            )
            url = resp["FunctionUrl"]
            r.ok(f"  ✓ created URL: {url}")

        # Add public invoke permission
        try:
            lam.add_permission(
                FunctionName=FN,
                StatementId="public-invoke",
                Action="lambda:InvokeFunctionUrl",
                Principal="*",
                FunctionUrlAuthType="NONE",
            )
            r.ok("  ✓ public invoke permission added")
        except lam.exceptions.ResourceConflictException:
            r.log("  ✓ permission already exists")

        # Publish URL manifest
        manifest = {"feedback_url": url, "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ")}
        s3.put_object(
            Bucket=BUCKET, Key="feedback-url.json",
            Body=json.dumps(manifest).encode(),
            ContentType="application/json",
            CacheControl="public, max-age=300",
        )
        r.ok(f"  ✓ published s3://{BUCKET}/feedback-url.json")

        # Smoke test
        time.sleep(3)
        import urllib.request
        try:
            req = urllib.request.Request(f"{url}signals?limit=2")
            with urllib.request.urlopen(req, timeout=15) as resp:
                body = resp.read().decode()
            r.log(f"  ✓ smoke {url}signals → {resp.status}")
            r.log(f"    body[:200]: {body[:200]}")
        except Exception as e:
            r.log(f"  ✗ smoke fail: {e}")


if __name__ == "__main__":
    main()
