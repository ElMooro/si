"""List reviewed public archive families; never read or transform archive payloads.

Registry membership is source-reviewed, not inferred from arbitrary S3 names or
caller inputs. Listing is not an immutability, freshness, valuation or historical
point-in-time certification. A failed family publishes an explicit unavailable
index with no partial keys. Catalog publication follows individual indexes.
"""
import json
import re
from datetime import datetime, timezone

import boto3
from private_artifact import is_private_source

BUCKET = "justhodl-dashboard-live"
PUBLISHER = "justhodl-public-archive-index"
SCHEMA = "public-engine-archive-index.v1"
S3 = boto3.client("s3", region_name="us-east-1")

# Exact unique source writers reviewed 2026-09-09. Shared auction-crisis output
# and all private/redacted historical families are deliberately excluded.
# pump-radar-brief is also excluded: some historical narrative donors have no
# source-proven writer/public provenance. Metadata does not certify their text.
REGISTRY = (
    ("justhodl-auction-crisis-ai", "data/archive/auction-crisis-ai/*.json"),
    ("justhodl-catalyst-classifier", "data/archive/catalysts/*.json"),
    ("justhodl-convergence-radar", "data/archive/convergence-radar/*.json"),
    ("justhodl-correlation-breaks", "data/archive/correlation-breaks/*.json"),
    ("justhodl-crisis-plumbing", "data/archive/crisis-plumbing/*.json"),
    ("justhodl-dark-pool", "data/archive/dark-pool/week-*.json"),
    ("justhodl-freight-pulse", "data/archive/freight-pulse/*.json"),
    ("justhodl-grid-queue", "data/archive/grid-queue/*.json"),
    ("justhodl-momentum-leaders", "data/archive/momentum-leaders/*.json"),
    ("justhodl-pair-trades", "data/archive/pair-trades/*.json"),
    ("justhodl-pump-earnings-nlp", "data/archive/pump-earnings-nlp/*.json"),
    ("justhodl-pump-mechanics", "data/archive/pump-mechanics/*.json"),
    ("justhodl-pump-positioning", "data/archive/pump-positioning/*.json"),
    ("justhodl-signal-fabric", "data/archive/feature-bus/*.json"),
    ("justhodl-ticker-deep-research", "data/archive/ticker-research/*.json"),
    ("justhodl-velocity-acceleration", "data/archive/velocity-acceleration/*.json"),
)


def _stamp(value):
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.astimezone(timezone.utc).isoformat()
    return None


def _error_code(exc):
    code = getattr(exc, "response", {}).get("Error", {}).get("Code", "LISTING_FAILED")
    return code if isinstance(code, str) and re.fullmatch(r"[A-Za-z0-9_]{1,64}", code) else "LISTING_FAILED"


def build_index(engine, pattern):
    if (engine, pattern) not in REGISTRY:
        raise ValueError("archive family is not in the reviewed registry")
    prefix = pattern.rsplit("/", 1)[0] + "/"
    if not prefix.startswith("data/archive/") or is_private_source(prefix):
        raise ValueError("archive family is not public")
    matcher = re.compile(re.escape(pattern).replace(r"\*", r"[^/]+"))
    started = datetime.now(timezone.utc).isoformat()
    rows, seen_keys, seen_tokens, errors = [], set(), set(), []
    token = None
    pages = 0
    complete = False
    try:
        while True:
            params = {"Bucket": BUCKET, "Prefix": prefix, "MaxKeys": 1000}
            if token is not None:
                params["ContinuationToken"] = token
            page = S3.list_objects_v2(**params)
            pages += 1
            if type(page.get("IsTruncated")) is not bool or not isinstance(page.get("Contents", []), list):
                raise ValueError("invalid listing envelope")
            for obj in page.get("Contents", []):
                key = obj.get("Key")
                if not isinstance(key, str) or not matcher.fullmatch(key) or is_private_source(key):
                    continue
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                size = obj.get("Size")
                modified = _stamp(obj.get("LastModified"))
                rows.append({"key": key, "last_modified": modified,
                             "size_bytes": size if type(size) is int and size >= 0 else None,
                             "immutable": False, "availability_basis": "listed metadata only",
                             "content_status": "NOT_READ", "point_in_time_certified": False})
            if not page["IsTruncated"]:
                complete = True
                break
            token = page.get("NextContinuationToken")
            if not isinstance(token, str) or not token or token in seen_tokens:
                raise ValueError("missing or repeating continuation token")
            seen_tokens.add(token)
    except Exception as exc:
        # Never publish a partial row collection as a complete timeline or
        # expose arbitrary exception bodies, service headers or payload text.
        rows = []
        errors = [{"phase": "LIST_METADATA", "prefix": prefix, "code": _error_code(exc)}]
    observed = datetime.now(timezone.utc).isoformat()
    rows.sort(key=lambda row: row["key"])
    return {"schema_version": SCHEMA, "engine": engine, "publisher_engine": PUBLISHER,
            "generated_at": observed, "index_observed_at": observed, "listing_started_at": started,
            "complete": complete, "completeness_scope": "paginated object listing only",
            "listing_is_atomic": False, "status": "INDEX_AVAILABLE" if complete else "INDEX_UNAVAILABLE",
            "families": [pattern], "n_snapshots": len(rows), "listing_pages": pages,
            "snapshots": rows, "errors": errors,
            "provenance_note": "LastModified describes the current listed object; it does not prove original release time, immutability, content validity or freshness."}


def lambda_handler(event=None, context=None):
    # Request fields never add a family, change its prefix, select a bucket or
    # read payloads. The only supported control is side-effect-free validation.
    indexes = [build_index(engine, pattern) for engine, pattern in REGISTRY]
    complete = all(doc["complete"] for doc in indexes)
    catalog = {"schema_version": "public-engine-archive-catalog.v1", "publisher_engine": PUBLISHER,
               "generated_at": datetime.now(timezone.utc).isoformat(), "complete": complete,
               "completeness_scope": "reviewed family metadata listings only", "indexes": [
                   {"engine": doc["engine"], "key": "data/archive-indexes/" + doc["engine"] + ".json",
                    "complete": doc["complete"], "n_snapshots": doc["n_snapshots"],
                    "index_observed_at": doc["index_observed_at"], "errors": doc["errors"]}
                   for doc in indexes]}
    bodies = [("data/archive-indexes/" + doc["engine"] + ".json", json.dumps(doc, separators=(",", ":")).encode()) for doc in indexes]
    catalog_body = json.dumps(catalog, separators=(",", ":")).encode()
    if isinstance(event, dict) and event.get("mode") == "validate_only":
        return {"ok": complete, "validation_only": True, "schema_version": SCHEMA,
                "status": "READY" if complete else "BLOCKED", "artifact_size_bytes": len(catalog_body) + sum(len(body) for _, body in bodies)}
    body_by_key = dict(bodies)
    for engine, _pattern in REGISTRY:
        key = "data/archive-indexes/" + engine + ".json"
        S3.put_object(Bucket=BUCKET, Key=key, Body=body_by_key[key], ContentType="application/json", CacheControl="public, max-age=300")
    S3.put_object(Bucket=BUCKET, Key="data/archive-indexes/catalog.json", Body=catalog_body,
                  ContentType="application/json", CacheControl="public, max-age=300")
    return {"ok": complete, "schema_version": SCHEMA, "n_families": len(indexes),
            "n_complete": sum(doc["complete"] for doc in indexes),
            "n_unavailable": sum(not doc["complete"] for doc in indexes),
            "catalog_key": "data/archive-indexes/catalog.json"}
