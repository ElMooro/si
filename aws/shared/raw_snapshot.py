"""aws/shared/raw_snapshot.py — F4: immutable raw-data snapshot layer (4432).

Ground truth for the provenance chain. Every provider fetch can be archived
before parsing, so any published number is verifiable against the exact
bytes the provider returned:

    from raw_snapshot import snapshot
    key = snapshot("fred", url, raw_bytes)   # -> data/raw/fred/2026-08-05/<sha12>.json.gz
    val = prov.wrap(3.42, "hy_oas", source="fred", raw_key=key)

Append-only by construction: the key is content-addressed (sha256 of bytes),
so re-snapshotting identical bytes is a no-op and nothing is ever
overwritten. gzip keeps the layer cheap; S3 lifecycle can tier it later (E9).
"""
import gzip
import hashlib
import json
from datetime import datetime, timezone

import boto3

_s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def snapshot(provider, url, raw_bytes, bucket=None, meta=None):
    """Archive raw provider bytes. Returns the S3 key (or None on failure —
    snapshotting must never break the fetch path)."""
    try:
        if isinstance(raw_bytes, str):
            raw_bytes = raw_bytes.encode()
        h = hashlib.sha256(raw_bytes).hexdigest()[:12]
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        key = f"data/raw/{provider}/{day}/{h}.json.gz"
        b = bucket or BUCKET
        try:  # content-addressed: if it exists, done
            _s3.head_object(Bucket=b, Key=key)
            return key
        except Exception:
            pass
        body = gzip.compress(raw_bytes)
        _s3.put_object(Bucket=b, Key=key, Body=body,
                       ContentType="application/gzip",
                       Metadata={"provider": provider,
                                 "url": (url or "")[:900],
                                 "sha256_12": h,
                                 "fetched_at": datetime.now(
                                     timezone.utc).isoformat(
                                     timespec="seconds"),
                                 **(meta or {})})
        return key
    except Exception as e:
        print(f"[raw_snapshot] {provider}: {type(e).__name__}: "
              f"{str(e)[:80]}")
        return None


def read_snapshot(key, bucket=None):
    """Read archived bytes back (verification path)."""
    try:
        o = _s3.get_object(Bucket=bucket or BUCKET, Key=key)
        return gzip.decompress(o["Body"].read())
    except Exception:
        return None
