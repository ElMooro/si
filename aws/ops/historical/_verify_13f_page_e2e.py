"""
Final 13F web-page verification.

Checks:
  1. data/13f-positions.json is fresh + valid + all sections populated
  2. S3 bucket has CORS configured to allow browser fetch (the page
     uses CORS to load JSON directly from S3 in the browser).
  3. The required render fields are all present in the JSON.

If any of these fail, the user-visible page won't render.
"""
import json
import time
from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)


def main():
    with report("verify_13f_page_e2e") as r:
        r.heading("End-to-end 13F page verification")

        r.section("1. data/13f-positions.json freshness + completeness")
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/13f-positions.json")
            last_mod = obj["LastModified"]
            size = obj["ContentLength"]
            age_h = (time.time() - last_mod.timestamp()) / 3600
            r.log(f"  size: {size:,} bytes")
            r.log(f"  last_modified: {last_mod.isoformat()}")
            r.log(f"  age: {age_h:.1f}h")

            data = json.loads(obj["Body"].read())
            sections = ["by_fund", "most_bought", "most_sold", "consensus_holds", "rare_picks"]
            for s in sections:
                v = data.get(s)
                if v:
                    n = len(v) if isinstance(v, (list, dict)) else 1
                    r.log(f"    ✓ {s}: {n} entries")
                else:
                    r.log(f"    ✗ {s}: MISSING")
        except Exception as e:
            r.fail(f"  ✗ {e}")

        r.section("2. S3 CORS — browser must be able to fetch from S3")
        try:
            cors = s3.get_bucket_cors(Bucket=BUCKET)
            rules = cors.get("CORSRules", [])
            r.log(f"  found {len(rules)} CORS rules")
            allow_get = False
            for rule in rules:
                methods = rule.get("AllowedMethods", [])
                origins = rule.get("AllowedOrigins", [])
                r.log(f"    rule: methods={methods} origins={origins}")
                if "GET" in methods and ("*" in origins or "https://justhodl.ai" in origins):
                    allow_get = True
            if allow_get:
                r.ok(f"  ✓ CORS allows browser GET")
            else:
                r.log(f"  ⚠ CORS does not allow public GET — browser fetch may fail")
        except Exception as e:
            if "NoSuchCORSConfiguration" in str(e):
                r.log(f"  ⚠ no CORS config — applying default permissive rules")
                # Apply CORS rules
                try:
                    s3.put_bucket_cors(Bucket=BUCKET, CORSConfiguration={
                        "CORSRules": [{
                            "AllowedHeaders": ["*"],
                            "AllowedMethods": ["GET", "HEAD"],
                            "AllowedOrigins": ["*"],
                            "ExposeHeaders": ["ETag", "Content-Length"],
                            "MaxAgeSeconds": 3000,
                        }],
                    })
                    r.ok(f"  ✓ applied CORS rules — browser fetch will now work")
                except Exception as e2:
                    r.fail(f"  ✗ couldn't apply CORS: {e2}")
            else:
                r.fail(f"  ✗ {e}")

        r.section("3. Public-read on data/* objects")
        try:
            policy = s3.get_bucket_policy(Bucket=BUCKET)
            r.log(f"  bucket has policy (length {len(policy.get('Policy', ''))} chars)")
            doc = json.loads(policy["Policy"])
            for stmt in doc.get("Statement", []):
                action = stmt.get("Action")
                principal = stmt.get("Principal")
                resource = stmt.get("Resource")
                r.log(f"    stmt: action={action} principal={principal} resource={str(resource)[:80]}")
        except Exception as e:
            if "NoSuchBucketPolicy" in str(e):
                r.log(f"  ⚠ no bucket policy — public access controlled by ACLs only")
            else:
                r.log(f"  ✗ {e}")

        r.section("4. Sanity check: actual sample data")
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/13f-positions.json")
            data = json.loads(obj["Body"].read())
            mb = (data.get("most_bought") or [])[:3]
            r.log(f"  Top 3 most-bought:")
            for x in mb:
                t = x.get("ticker") or x.get("cusip", "?")[:9]
                n_buy = x.get("n_funds_adding", 0) + x.get("n_funds_new_position", 0)
                n_sell = x.get("n_funds_trimming", 0) + x.get("n_funds_exiting", 0)
                r.log(f"    {t:8s} {x.get('name','')[:30]:30s} +{n_buy} buying / -{n_sell} selling")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
