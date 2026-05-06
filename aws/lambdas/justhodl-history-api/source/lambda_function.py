"""justhodl-history-api

Public read-only API for the time-series snapshot table.

Exposed via Lambda Function URL (no auth — read-only surface,
rate-limited via reserved concurrency=5). The audit page calls this
to fetch actual content at any historical timestamp.

ENDPOINTS

  GET / | /index
    Returns the slim history index (passthrough of data/history-index.json).
    Cached at the page layer.

  GET /snapshot?key=<feed_key>&ts=<iso8601>
    Returns the snapshot whose sk == ts (exact match) for pk=feed#<key>.
    Decompresses gzip+base64 content. If the body was archived to S3
    (oversize), returns a redirect-style pointer with archive URL.

  GET /latest?key=<feed_key>
    Returns the most recent snapshot for that feed.

  GET /timestamps?key=<feed_key>&limit=<n>
    Returns just the list of timestamps (sk values) for a feed,
    newest first, up to limit (default 100, max 500).

RESPONSE

  Always returns JSON with CORS headers. Content type is application/json.
  Errors return {"error": "...", "code": "<short>"}.
"""
from __future__ import annotations
import base64
import gzip
import json
import os
from urllib.parse import urlparse, parse_qs

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
DDB_TABLE = os.environ.get("DDB_TABLE", "justhodl-history")
INDEX_KEY = "data/history-index.json"

s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)

CORS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Cache-Control": "public, max-age=60",
}


def _resp(status, body, extra_headers=None):
    headers = {"Content-Type": "application/json", **CORS}
    if extra_headers:
        headers.update(extra_headers)
    return {"statusCode": status, "headers": headers,
            "body": json.dumps(body, default=str)}


def _err(status, msg, code="bad_request"):
    return _resp(status, {"error": msg, "code": code})


def _decode_content(item: dict):
    """Reverse the encoding done by the snapshotter."""
    encoding = item.get("encoding", {}).get("S")
    content = item.get("content", {}).get("S")
    archive_key = item.get("content_archive_key", {}).get("S")
    if encoding == "s3-archive-gzip":
        # Body is in S3 as gzipped JSON
        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=archive_key)
            raw = gzip.decompress(obj["Body"].read())
            return raw.decode("utf-8"), encoding
        except Exception as e:
            return None, f"archive-fetch-failed:{e}"
    if not content:
        return None, "no-content"
    if encoding == "utf8":
        return content, encoding
    if encoding == "gzip+base64":
        try:
            return gzip.decompress(base64.b64decode(content)).decode("utf-8"), encoding
        except Exception as e:
            return None, f"gzip-decode-failed:{e}"
    if encoding == "base64":
        try:
            return base64.b64decode(content).decode("utf-8", errors="replace"), encoding
        except Exception:
            return content, encoding
    # Unknown — return as-is
    return content, encoding or "unknown"


def _parse_qs(event):
    """Function URL events put the query in rawQueryString or queryStringParameters."""
    qs = event.get("queryStringParameters") or {}
    if qs:
        return qs
    raw = event.get("rawQueryString") or ""
    if raw:
        parsed = parse_qs(raw)
        return {k: v[0] for k, v in parsed.items() if v}
    return {}


def handle_index():
    try:
        body = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=INDEX_KEY)["Body"].read())
        return _resp(200, body)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return _err(404, "history-index.json not yet generated", "index_missing")
        return _err(500, str(e), "s3_error")


def handle_snapshot(qs):
    key = qs.get("key")
    ts = qs.get("ts")
    if not key:
        return _err(400, "missing key", "missing_key")
    if not ts:
        return _err(400, "missing ts (ISO8601)", "missing_ts")
    pk = f"feed#{key}" if not key.startswith("feed#") else key
    try:
        resp = ddb.get_item(
            TableName=DDB_TABLE,
            Key={"pk": {"S": pk}, "sk": {"S": ts}},
        )
    except Exception as e:
        return _err(500, str(e), "ddb_error")
    item = resp.get("Item")
    if not item:
        return _err(404, f"no snapshot at exact ts={ts} for {pk}", "snapshot_not_found")

    content_str, enc_used = _decode_content(item)
    out = {
        "pk": pk,
        "feed_key": key,
        "ts": ts,
        "content_hash": item.get("content_hash", {}).get("S"),
        "size_bytes": int(item.get("size_bytes", {}).get("N", "0") or 0),
        "encoding": item.get("encoding", {}).get("S"),
        "encoding_used_for_decode": enc_used,
        "etag": item.get("etag", {}).get("S"),
        "generated_at": item.get("generated_at", {}).get("S"),
    }
    # Try parsing content as JSON, else return as text
    if content_str is not None:
        try:
            out["content"] = json.loads(content_str)
            out["content_format"] = "json"
        except Exception:
            out["content"] = content_str[:200_000]   # cap response at 200KB
            out["content_format"] = "text"
    else:
        out["content"] = None
        out["content_format"] = "unavailable"
    return _resp(200, out)


def handle_latest(qs):
    key = qs.get("key")
    if not key:
        return _err(400, "missing key", "missing_key")
    pk = f"feed#{key}" if not key.startswith("feed#") else key
    try:
        resp = ddb.query(
            TableName=DDB_TABLE,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": pk}},
            ScanIndexForward=False,
            Limit=1,
        )
    except Exception as e:
        return _err(500, str(e), "ddb_error")
    items = resp.get("Items", [])
    if not items:
        return _err(404, f"no snapshots for {pk}", "feed_not_found")
    item = items[0]
    ts = item.get("sk", {}).get("S")
    return handle_snapshot({"key": key, "ts": ts})


def handle_timestamps(qs):
    key = qs.get("key")
    if not key:
        return _err(400, "missing key", "missing_key")
    try:
        limit = int(qs.get("limit", "100"))
    except Exception:
        limit = 100
    limit = max(1, min(500, limit))
    pk = f"feed#{key}" if not key.startswith("feed#") else key
    try:
        resp = ddb.query(
            TableName=DDB_TABLE,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": pk}},
            ScanIndexForward=False,
            Limit=limit,
            ProjectionExpression="sk, content_hash, size_bytes",
        )
    except Exception as e:
        return _err(500, str(e), "ddb_error")
    items = resp.get("Items", [])
    return _resp(200, {
        "pk": pk,
        "feed_key": key,
        "n_returned": len(items),
        "limit": limit,
        "timestamps": [
            {
                "ts": it.get("sk", {}).get("S"),
                "content_hash": it.get("content_hash", {}).get("S"),
                "size_bytes": int(it.get("size_bytes", {}).get("N", "0") or 0),
            }
            for it in items
        ],
    })


def lambda_handler(event=None, context=None):
    method = (event.get("requestContext", {})
                  .get("http", {}).get("method", "GET")) if isinstance(event, dict) else "GET"
    if method == "OPTIONS":
        return {"statusCode": 200, "headers": CORS, "body": "{}"}

    raw_path = event.get("rawPath") or event.get("path") or "/"
    path = raw_path.rstrip("/").lower() or "/"
    qs = _parse_qs(event)

    if path in ("/", "/index"):
        return handle_index()
    if path == "/snapshot":
        return handle_snapshot(qs)
    if path == "/latest":
        return handle_latest(qs)
    if path == "/timestamps":
        return handle_timestamps(qs)

    return _err(404, f"unknown path {raw_path}. valid: /, /snapshot, /latest, /timestamps",
                "unknown_path")
