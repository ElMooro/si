"""Write a repo history file only when its bytes changed.

Khalid 2026-11-21 ("i want all my ... data to stay accurate without my monthly budget going over $250", and yes to
opening this engine): every run rebuilt and re-wrote every data/repo-history/{id}.json - about 2,300 files a day -
although most series gain an observation weekly or monthly, so the bytes were identical (ops 6872: the newest two
versions of a sampled file are the same bytes).  On the versioned bucket each of those writes is billed twice.

The body is a pure function of the series (no timestamp inside), so "same bytes" is exact: one listing of the prefix
gives every file's ETag - for a single-part object under the bucket's default encryption that is the md5 of its bytes -
and a file whose new bytes hash to its ETag is left alone.  Anything else is written exactly as before.  If the listing
fails, or an ETag is not an md5 (multipart, KMS), the comparison simply never matches and the file is written.
"""
import hashlib

PREFIX = "data/repo-history/"


def history_etags(s3, bucket, prefix=PREFIX, log=print):
    """{key: ETag without quotes} for every object under the prefix; {} when the listing cannot be read."""
    out, token = {}, None
    try:
        while True:
            kw = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
            if token:
                kw["ContinuationToken"] = token
            page = s3.list_objects_v2(**kw)
            for o in page.get("Contents") or []:
                out[o["Key"]] = str(o.get("ETag") or "").strip('"')
            token = page.get("NextContinuationToken")
            if not token:
                break
    except Exception as e:  # noqa: BLE001 - a failed listing costs the saving, never a write
        log("[repo] history listing failed (%s): every history file is written this run" % type(e).__name__)
        return {}
    return out


def put_history(s3, bucket, key, body, etags, stats):
    """Write `body` to `key` unless the stored bytes are already these bytes.  -> True when written."""
    digest = hashlib.md5(body).hexdigest()
    if etags.get(key) == digest:
        stats["unchanged"] = stats.get("unchanged", 0) + 1
        return False
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json", CacheControl="public, max-age=1800")
    etags[key] = digest
    stats["written"] = stats.get("written", 0) + 1
    return True
