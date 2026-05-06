"""justhodl-history-snapshotter

Time-series persistence for every live JustHodl feed. Runs every 5 min.

For each feed in FEEDS_TO_SNAPSHOT:
  1. Read the current S3 object
  2. Compute SHA256 of body
  3. Query DDB for the LATEST snapshot of this feed (most recent sk)
  4. If hash matches latest → skip (deduped)
  5. Otherwise insert a new row with:
       pk      = "feed#<key>"           (e.g., "feed#data/report.json")
       sk      = "<ISO8601 timestamp>"  (e.g., "2026-05-06T16:30:00Z")
       hash    = SHA256 of body
       size    = bytes
       gen_at  = body.generated_at if present
       ttl     = epoch + 365 days
       content = the JSON body (gzipped + base64 if >100KB)

This unblocks:
  • "what did the AI brief say at 08:00 on 2025-09-15?"
  • Walk-forward calibration (next ship — needs historical weight series)
  • LP-grade audit trail (every decisive call is now immutably timestamped)
  • Time-series analytics (regime sweeps, signal evolution charts)

Schema notes:
  - PAY_PER_REQUEST billing — costs scale only with actual writes
  - Dedup by hash → typical day writes ~50 rows (only when a feed changes)
  - 365d TTL means rows auto-delete; for longer history, archive to S3
    via a separate job before TTL expires
  - Compressed content stored as base64 of gzip; decompress on read

Schedule: rate(5 minutes) via EventBridge rule justhodl-history-snapshotter-5m
"""
from __future__ import annotations
import base64
import gzip
import hashlib
import json
import os
import time
from datetime import datetime, timezone, timedelta

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
DDB_TABLE = os.environ.get("DDB_TABLE", "justhodl-history")
TTL_DAYS = int(os.environ.get("TTL_DAYS", "365"))
COMPRESS_THRESHOLD = 100 * 1024   # gzip+b64 anything >100KB
DDB_ITEM_MAX = 380 * 1024          # leave 20KB margin under DDB's 400KB hard limit

# Feeds to snapshot. This list is the canonical "what powers the website?"
# inventory. Adding a new feed here = it gets historized automatically.
FEEDS_TO_SNAPSHOT = [
    # Core macro / regime
    "data/report.json",
    "data/khalid-metrics.json",
    "data/khalid-analysis.json",
    "data/khalid-config.json",
    "data/morning-intel.json",
    "data/morning-brief-latest.json",
    "data/ai-brief.json",
    "data/secretary-latest.json",
    "data/decisive-call-history.json",
    "data/eurodollar-stress.json",
    # Signals & opportunities
    "data/asymmetric-scorer.json",
    "data/compound-signals.json",
    "data/eps-revision-velocity.json",
    "data/insider-clusters.json",
    "data/smart-money-clusters.json",
    "data/etf-flows.json",
    "data/themes-detected.json",
    "data/theme-tiers.json",
    "data/nobrainers.json",
    "data/nobrainers-rationale.json",
    "data/supply-inflection.json",
    "data/deep-value.json",
    "data/universe.json",
    # Backtest
    "backtest/results.json",
    "backtest/summary.json",
    # Calibration — for walk-forward backtest history
    "calibration/latest.json",
    "calibration/history-index.json",
    # Alerts + forensic — added 2026-05-06
    "data/alert-history.json",
    "data/forensic-screen.json",
    # Macro nowcast — added 2026-05-06 (composite z-score)
    "data/macro-nowcast.json",
    # Major dashboards
    "screener/data.json",
    # Crypto / flows / edge (best-effort — skipped silently if missing)
    "data/crypto-intel.json",
    "data/options-flow.json",
    "data/edge-data.json",
    "data/flow-data.json",
]

s3 = boto3.client("s3", region_name=REGION)
ddb_client = boto3.client("dynamodb", region_name=REGION)
ddb_resource = boto3.resource("dynamodb", region_name=REGION)


def ensure_table_exists():
    """Create the DDB table on first run with TTL enabled."""
    try:
        ddb_resource.Table(DDB_TABLE).load()
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
    print(f"[history] creating DDB table {DDB_TABLE}…")
    ddb_client.create_table(
        TableName=DDB_TABLE,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb_client.get_waiter("table_exists").wait(TableName=DDB_TABLE)
    # Enable TTL on attribute "ttl"
    try:
        ddb_client.update_time_to_live(
            TableName=DDB_TABLE,
            TimeToLiveSpecification={"Enabled": True, "AttributeName": "ttl"},
        )
    except Exception as e:
        print(f"[history] warn: failed to enable TTL: {e}")
    return True


def get_latest_hash(pk: str):
    """Query latest snapshot for this feed, returning its content_hash or None."""
    try:
        # Reverse-scan by sk (timestamp), limit 1
        resp = ddb_client.query(
            TableName=DDB_TABLE,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": pk}},
            ScanIndexForward=False,
            Limit=1,
            ProjectionExpression="content_hash",
        )
        items = resp.get("Items", [])
        if not items:
            return None
        return items[0].get("content_hash", {}).get("S")
    except Exception as e:
        print(f"[history] query err pk={pk}: {e}")
        return None


def encode_content(body_bytes: bytes):
    """Return (encoded_value, encoding_label, original_size). Compresses if large."""
    n = len(body_bytes)
    if n < COMPRESS_THRESHOLD:
        # Plain UTF-8 string (DDB-safe)
        try:
            txt = body_bytes.decode("utf-8")
            return txt, "utf8", n
        except UnicodeDecodeError:
            return base64.b64encode(body_bytes).decode("ascii"), "base64", n
    # Compress
    gz = gzip.compress(body_bytes, compresslevel=6)
    enc = base64.b64encode(gz).decode("ascii")
    return enc, "gzip+base64", n


def write_snapshot(pk: str, key: str, body: bytes, etag: str):
    now = datetime.now(timezone.utc)
    sk = now.isoformat(timespec="seconds").replace("+00:00", "Z")
    h = hashlib.sha256(body).hexdigest()

    encoded, encoding, orig_size = encode_content(body)

    # Try to extract generated_at from JSON if present
    generated_at = None
    try:
        parsed = json.loads(body.decode("utf-8"))
        if isinstance(parsed, dict):
            generated_at = parsed.get("generated_at") or parsed.get("ts") or parsed.get("updated_at")
    except Exception:
        pass

    item = {
        "pk":            {"S": pk},
        "sk":            {"S": sk},
        "feed_key":      {"S": key},
        "content_hash":  {"S": h},
        "size_bytes":    {"N": str(orig_size)},
        "encoding":      {"S": encoding},
        "etag":          {"S": etag or ""},
        "ttl":           {"N": str(int((now + timedelta(days=TTL_DAYS)).timestamp()))},
    }
    if generated_at:
        item["generated_at"] = {"S": str(generated_at)[:64]}

    # Estimate DDB item size — base meta + content. If too big, store body in
    # S3 archive bucket prefix instead and just record a pointer.
    est_size = sum(len(v.get("S", v.get("N", ""))) for v in item.values()) + len(encoded)
    if est_size > DDB_ITEM_MAX:
        # Store full body in S3 archive, only a reference in DDB
        archive_key = f"history/archive/{pk.replace('#', '/')}/{sk}.gz"
        try:
            s3.put_object(
                Bucket=S3_BUCKET, Key=archive_key,
                Body=gzip.compress(body, compresslevel=6),
                ContentType="application/json",
                ContentEncoding="gzip",
            )
            item["content_archive_key"] = {"S": archive_key}
            item["encoding"] = {"S": "s3-archive-gzip"}
            print(f"[history] archived large body ({orig_size}b) to {archive_key}")
        except Exception as e:
            print(f"[history] archive err: {e}")
            return False
    else:
        item["content"] = {"S": encoded}

    try:
        ddb_client.put_item(TableName=DDB_TABLE, Item=item)
        return True
    except Exception as e:
        print(f"[history] put err pk={pk} sk={sk}: {e}")
        return False


def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[history] starting {datetime.now(timezone.utc).isoformat()}")
    ensure_table_exists()

    n_checked = 0
    n_skipped_nochange = 0
    n_skipped_missing = 0
    n_written = 0
    n_errors = 0
    write_log = []

    for key in FEEDS_TO_SNAPSHOT:
        n_checked += 1
        pk = f"feed#{key}"
        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("NoSuchKey", "404"):
                n_skipped_missing += 1
                continue
            print(f"[history] s3 err {key}: {e}")
            n_errors += 1
            continue
        body = obj["Body"].read()
        etag = obj.get("ETag", "").strip('"')
        body_hash = hashlib.sha256(body).hexdigest()

        latest = get_latest_hash(pk)
        if latest == body_hash:
            n_skipped_nochange += 1
            continue

        if write_snapshot(pk, key, body, etag):
            n_written += 1
            write_log.append({"key": key, "size": len(body), "hash": body_hash[:12]})
        else:
            n_errors += 1

    duration = round(time.time() - started, 2)
    summary = {
        "n_feeds_checked": n_checked,
        "n_skipped_no_change": n_skipped_nochange,
        "n_skipped_missing": n_skipped_missing,
        "n_written": n_written,
        "n_errors": n_errors,
        "duration_s": duration,
    }
    print(f"[history] done: {summary}")
    if write_log:
        print(f"[history] writes: {write_log}")

    # Heartbeat: write a small summary to S3 so /system.html can monitor
    try:
        s3.put_object(
            Bucket=S3_BUCKET, Key="data/history-snapshotter-status.json",
            Body=json.dumps({
                "last_run": datetime.now(timezone.utc).isoformat(),
                **summary,
                "write_log": write_log[-10:],
            }, default=str).encode(),
            ContentType="application/json",
            CacheControl="no-cache",
        )
    except Exception as e:
        print(f"[history] heartbeat err: {e}")

    # History index: lists every feed with snapshot count + newest 50 timestamps.
    # Powers /audit.html. Scanning DDB on every 5-min run is wasteful, so we
    # fire once per hour at the top of the hour (when minute < 5). Total
    # output is small (tens of KB) regardless of total snapshot count
    # because we cap recent_timestamps to 50 per feed.
    try:
        if datetime.now(timezone.utc).minute < 5:
            _build_history_index()
    except Exception as e:
        print(f"[history] index build err: {e}")

    return {"statusCode": 200, "body": json.dumps(summary)}


def _build_history_index():
    """Scan DDB, group by pk, write data/history-index.json for /audit.html.

    Schema (matches what audit.html expects):
      {
        "generated_at": ISO timestamp,
        "n_feeds": int,
        "n_snapshots_total": int,
        "ddb_pages_scanned": int,
        "duration_s": float,
        "feeds": [
          {
            "feed_key": "data/report.json",
            "pk": "feed#data/report.json",
            "snapshot_count": int,
            "first_seen": ISO,
            "last_seen": ISO,
            "latest_hash": sha256,
            "recent_timestamps": [ISO, ...]   # newest 50
          },
          ...
        ]
      }
    """
    started = time.time()
    print(f"[history-index] scanning DDB {DDB_TABLE}…")

    feeds_data = {}  # pk -> aggregate info
    pages = 0
    last_key = None

    try:
        while True:
            kw = {
                "TableName": DDB_TABLE,
                "ProjectionExpression": "pk, sk, content_hash",
            }
            if last_key:
                kw["ExclusiveStartKey"] = last_key
            resp = ddb_client.scan(**kw)
            pages += 1
            for item in resp.get("Items", []):
                pk = item.get("pk", {}).get("S", "")
                sk = item.get("sk", {}).get("S", "")
                h = item.get("content_hash", {}).get("S", "")
                if not pk or not sk:
                    continue
                bucket = feeds_data.setdefault(pk, {
                    "pk": pk,
                    "feed_key": pk[5:] if pk.startswith("feed#") else pk,
                    "snapshot_count": 0,
                    "first_seen": sk,
                    "last_seen": sk,
                    "latest_hash": h,
                    "_all_sks": [],
                })
                bucket["snapshot_count"] += 1
                if sk < bucket["first_seen"]:
                    bucket["first_seen"] = sk
                if sk > bucket["last_seen"]:
                    bucket["last_seen"] = sk
                    bucket["latest_hash"] = h  # most recent hash
                bucket["_all_sks"].append(sk)
            last_key = resp.get("LastEvaluatedKey")
            if not last_key or pages > 100:
                break
    except Exception as e:
        print(f"[history-index] scan err: {e}")
        return

    # Finalize: cap recent_timestamps at 50 per feed (newest first)
    feeds_list = []
    n_total = 0
    for pk, b in feeds_data.items():
        b["_all_sks"].sort(reverse=True)
        b["recent_timestamps"] = b["_all_sks"][:50]
        del b["_all_sks"]
        feeds_list.append(b)
        n_total += b["snapshot_count"]

    # Sort feeds by snapshot count desc (most-tracked first)
    feeds_list.sort(key=lambda x: -x["snapshot_count"])

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "n_feeds": len(feeds_list),
        "n_snapshots_total": n_total,
        "ddb_pages_scanned": pages,
        "duration_s": round(time.time() - started, 2),
        "feeds": feeds_list,
    }

    s3.put_object(
        Bucket=S3_BUCKET, Key="data/history-index.json",
        Body=json.dumps(out, default=str).encode(),
        ContentType="application/json",
        CacheControl="public, max-age=1800",
    )
    print(f"[history-index] wrote data/history-index.json: "
          f"{len(feeds_list)} feeds, {n_total} total snapshots, "
          f"{pages} DDB pages, {round(time.time()-started,2)}s")
