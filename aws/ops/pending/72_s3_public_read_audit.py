#!/usr/bin/env python3
"""
The justhodl-intelligence fix landed half-correctly — data/report.json
loads (HTTP 200), but repo-data.json and edge-data.json return HTTP 403
to anonymous public requests.

Investigate:
  A. What's the bucket policy?
  B. Per-object ACL for the affected keys
  C. Easiest fix: switch the Lambda from HTTPS public URL to boto3 SDK
     (which uses IAM credentials and doesn't need public read)
"""
import json
from datetime import datetime, timezone
import urllib.request
import urllib.error
from ops_report import report
import boto3

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def http_check(url):
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            return {"status": r.status, "size": int(r.headers.get("content-length", 0))}
    except urllib.error.HTTPError as e:
        return {"status": e.code, "error": str(e)}
    except Exception as e:
        return {"status": "err", "error": str(e)}


with report("s3_public_read_audit") as r:
    r.heading("S3 public-read audit — why does repo-data.json 403?")

    # ─── A. Bucket policy ───
    r.section("A. Bucket policy")
    try:
        pol = s3.get_bucket_policy(Bucket=BUCKET)
        policy = json.loads(pol["Policy"])
        r.log(json.dumps(policy, indent=2)[:1500])
    except s3.exceptions.from_code("NoSuchBucketPolicy"):
        r.log("  No bucket policy attached")
    except Exception as e:
        r.warn(f"  {e}")

    # ─── B. Public access block ───
    r.section("B. Public access block config")
    try:
        pab = s3.get_public_access_block(Bucket=BUCKET)
        r.log(f"  {pab.get('PublicAccessBlockConfiguration')}")
    except Exception as e:
        r.log(f"  {e}")

    # ─── C. Test public HTTP for each affected key ───
    r.section("C. HTTP test for each data file the Lambda needs")
    keys = [
        "data/report.json",
        "repo-data.json",
        "edge-data.json",
        "flow-data.json",
        "predictions.json",
        "intelligence-report.json",
    ]
    for k in keys:
        url = f"https://justhodl-dashboard-live.s3.amazonaws.com/{k}"
        res = http_check(url)
        flag = "✓" if res.get("status") == 200 else "✗"
        r.log(f"  {flag} {k:40} HTTP {res.get('status')} {('— ' + str(res.get('size','?')) + ' bytes') if res.get('status')==200 else ''}")

    # ─── D. Check object ACLs (if any) ───
    r.section("D. Per-object ACLs")
    for k in ["data/report.json", "repo-data.json", "edge-data.json"]:
        try:
            acl = s3.get_object_acl(Bucket=BUCKET, Key=k)
            grants = acl.get("Grants", [])
            public_read = any(
                g.get("Grantee", {}).get("URI", "").endswith("AllUsers") and g.get("Permission") == "READ"
                for g in grants
            )
            r.log(f"  {k:40} grants={len(grants)} public_read={public_read}")
        except Exception as e:
            r.warn(f"  {k}: {e}")

    r.log("Done")
