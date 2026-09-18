"""Content-addressed, conditional source capture. No credentials in references.

The caller supplies the authenticated client. A returned receipt means the exact
bytes were stored (or read back and verified after a conditional-write conflict).
This is application-level write protection, not a claim of S3 Object Lock.
"""
import gzip
import hashlib
import re
from datetime import datetime, timezone
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

CONTRACT = "source-evidence.v1"
SAFE_QUERY_KEYS = {"series_id", "file_type", "sort_order", "limit", "units", "frequency",
                   "observation_start", "observation_end", "realtime_start", "realtime_end",
                   "symbol", "symbols", "range", "interval"}


def public_source_url(url):
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.hostname:
        raise ValueError("source requires an HTTPS URL")
    # Reconstruct authority so userinfo and fragments can never escape.
    authority = parsed.hostname + (":" + str(parsed.port) if parsed.port else "")
    query = urlencode([(k, v) for k, v in parse_qsl(parsed.query) if k.lower() in SAFE_QUERY_KEYS])
    return urlunsplit(("https", authority, parsed.path, query, ""))


def capture(client, bucket, provider, url, raw, received_at=None):
    if not re.fullmatch(r"[a-z0-9_-]{1,40}", provider):
        raise ValueError("invalid evidence provider")
    if not isinstance(raw, bytes) or not raw:
        raise ValueError("nonempty exact response bytes required")
    stamp = received_at or datetime.now(timezone.utc)
    if stamp.tzinfo is None:
        raise ValueError("receipt time must be timezone-aware")
    sha = hashlib.sha256(raw).hexdigest()
    received = stamp.astimezone(timezone.utc).isoformat()
    source_url = public_source_url(url)
    request_sha = hashlib.sha256(source_url.encode()).hexdigest()
    key = f"data/evidence/{provider}/{request_sha}/{sha}.bin.gz"
    metadata = {"sha256": sha, "received_at": received, "provider": provider,
                "source_url": source_url, "contract": CONTRACT}
    try:
        client.put_object(Bucket=bucket, Key=key, Body=gzip.compress(raw, mtime=0),
                          ContentType="application/gzip", IfNoneMatch="*", Metadata=metadata)
    except Exception as exc:
        code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
        if code not in ("PreconditionFailed", "412", "ConditionalRequestConflict", "409"):
            raise
        old = client.get_object(Bucket=bucket, Key=key)
        if hashlib.sha256(gzip.decompress(old["Body"].read())).hexdigest() != sha:
            raise ValueError("stored evidence hash mismatch") from exc
        if old.get("Metadata", {}).get("source_url") != source_url:
            raise ValueError("stored request identity mismatch") from exc
        received = old.get("Metadata", {}).get("received_at")
        if not received:
            raise ValueError("stored evidence lacks receipt time") from exc
    return {"contract": CONTRACT, "key": key, "sha256": sha, "bytes": len(raw),
            "provider": provider, "source_url": source_url, "first_received_at": received,
            "captured": True}


def read_verified(client, bucket, evidence):
    if evidence.get("contract") != CONTRACT or evidence.get("captured") is not True:
        raise ValueError("verified evidence receipt required")
    raw = gzip.decompress(client.get_object(Bucket=bucket, Key=evidence["key"])["Body"].read())
    if len(raw) != evidence["bytes"] or hashlib.sha256(raw).hexdigest() != evidence["sha256"]:
        raise ValueError("evidence bytes do not match receipt")
    return raw
