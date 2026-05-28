"""jhcore.s3io — S3 read/write helpers for JustHodl Lambdas."""
import json
import boto3
from botocore.exceptions import ClientError

BUCKET = "justhodl-dashboard-live"
REGION = "us-east-1"
_s3 = boto3.client("s3", region_name=REGION)


def get_json(key, bucket=BUCKET, default=None):
    """Read a JSON object from S3. Returns default on any error."""
    try:
        r = _s3.get_object(Bucket=bucket, Key=key)
        return json.loads(r["Body"].read())
    except ClientError:
        return default
    except Exception:
        return default


def put_json(key, obj, bucket=BUCKET, cache_control="public, max-age=300", content_type="application/json"):
    """Write a JSON object to S3. Returns True/False."""
    try:
        body = json.dumps(obj, default=str).encode("utf-8") if not isinstance(obj, (bytes, str)) else obj
        if isinstance(body, str):
            body = body.encode("utf-8")
        _s3.put_object(Bucket=bucket, Key=key, Body=body,
                       ContentType=content_type, CacheControl=cache_control)
        return True
    except Exception as e:
        print(f"[jhcore.s3io] put_json {key} err: {e}")
        return False


def head(key, bucket=BUCKET):
    """Return HEAD response or None."""
    try:
        return _s3.head_object(Bucket=bucket, Key=key)
    except ClientError:
        return None


def exists(key, bucket=BUCKET):
    return head(key, bucket) is not None


def get_text(key, bucket=BUCKET, default=""):
    try:
        r = _s3.get_object(Bucket=bucket, Key=key)
        return r["Body"].read().decode("utf-8", errors="replace")
    except Exception:
        return default


def put_text(key, text, bucket=BUCKET, cache_control="public, max-age=300", content_type="text/plain"):
    return put_json(key, text, bucket=bucket, cache_control=cache_control, content_type=content_type)


def list_keys(prefix, bucket=BUCKET, max_keys=1000):
    """List keys under a prefix."""
    keys = []
    paginator = _s3.get_paginator("list_objects_v2")
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, PaginationConfig={"MaxItems": max_keys}):
            for o in page.get("Contents", []):
                keys.append(o["Key"])
    except Exception:
        pass
    return keys
