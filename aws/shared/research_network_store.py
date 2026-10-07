"""S3 transport for the research network. Fixed allowlist, complete reads,
immutable originals/shards, then a conditional publication pointer.
"""
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
import re
from decimal import Decimal
from private_artifact import public_source_allowed
from research_network import CONTRACT, PERMISSIONS, canonical, compose, digest, ingest
from research_network_registry import SOURCES, SUBSCRIPTIONS

KEY = "data/research-network.json"
PREFIX = "data/research-network/"
MAX_BYTES = 64 * 1024 * 1024


def strict_json(raw):
    def pairs(items):
        result = {}
        for k, v in items:
            if k in result:
                raise ValueError("duplicate JSON key")
            result[k] = v
        return result
    def invalid(value):
        raise ValueError("nonfinite JSON number")
    def real(value):
        result = float(value)
        if result == 0 and Decimal(value) != 0:
            raise ValueError("JSON numeric underflow")
        return result
    value = json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid, parse_float=real)
    # json accepts overflow numbers like 1e999 unless the decoded value is checked.
    canonical(value)
    return value


def read(s3, bucket, key, limit=MAX_BYTES, *, not_after=None):
    if not public_source_allowed(key):
        raise ValueError("public source allowlist boundary")
    response = s3.get_object(Bucket=bucket, Key=key)
    body, chunks, size = response["Body"], [], 0
    try:
        if not_after is not None:
            stored = response.get("LastModified")
            if not isinstance(stored, datetime) or stored.tzinfo is None or stored > not_after:
                raise ValueError("research publication was not stored before registration")
        declared = response.get("ContentLength")
        if declared is not None and (type(declared) is not int or not 0 <= declared <= limit):
            raise ValueError("source content length invalid")
        if response.get("ContentEncoding", "identity") not in ("identity", ""):
            raise ValueError("unexpected encoded source")
        while True:
            part = body.read(min(65536, limit + 1 - size))
            if not isinstance(part, bytes):
                raise ValueError("source is not bytes")
            if not part:
                break
            size += len(part)
            if size > limit:
                raise ValueError("source byte bound exceeded")
            chunks.append(part)
        if declared is not None and size != declared:
            raise ValueError("source truncated")
    finally:
        body.close()
    raw = b"".join(chunks)
    return strict_json(raw), raw, response.get("ETag")


def missing(exc):
    return str(getattr(exc, "response", {}).get("Error", {}).get("Code", "")) in ("404", "NoSuchKey")


def immutable(s3, bucket, key, body, *, compressed=False):
    args = {"ContentType": "application/json", "CacheControl": "public,max-age=31536000,immutable"}
    if compressed:
        args["ContentEncoding"] = "gzip"
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=body, IfNoneMatch="*", **args)
    except Exception as exc:
        if str(getattr(exc, "response", {}).get("Error", {}).get("Code", "")) not in ("412", "PreconditionFailed"):
            raise
        # The key is content-addressed; don't trust a collision without bytes.
        response = s3.get_object(Bucket=bucket, Key=key)
        stream = response["Body"]
        try:
            existing = stream.read(MAX_BYTES + 1048577 if compressed else len(body) + 1)
            if compressed:
                if len(existing) > MAX_BYTES + 1048576:
                    raise ValueError('immutable compressed source exceeds bound')
                # Identity is the original bytes. Python/zlib releases can emit
                # different gzip headers for the SAME bytes; those are not an
                # evidence mismatch. Bound decoding and compare every raw byte.
                def unpack(value):
                    with gzip.GzipFile(fileobj=io.BytesIO(value)) as archived:
                        decoded = archived.read(MAX_BYTES + 1)
                    if len(decoded) > MAX_BYTES: raise ValueError('archive expansion exceeds bound')
                    return decoded
                matches = unpack(existing) == unpack(body)
            else:
                matches = existing == body
            if not matches:
                raise ValueError("immutable research artifact differs")
        finally:
            stream.close()


def publish_network(s3, bucket, *, now=None, context=None):
    now = now or datetime.now(timezone.utc)
    try:
        prior, _, prior_etag = read(s3, bucket, KEY, 16 * 1024 * 1024)
        if prior.get("contract") != CONTRACT:
            raise ValueError("previous research network contract mismatch")
    except Exception as exc:
        if not missing(exc):
            raise
        prior, prior_etag = {}, None
    sources, observations = {}, []
    for sid, spec in SOURCES.items():
        if context is not None and context.get_remaining_time_in_millis() < 90000:
            # Leave the previous complete publication in place on a run timeout.
            raise TimeoutError("research publication incomplete; prior retained")
        try:
            if spec.get('access_status') == 'public_access_not_verified':
                raise PermissionError('public publication access not verified')
            doc, raw, _ = read(s3, bucket, spec["key"])
            sha = hashlib.sha256(raw).hexdigest()
            original_key = PREFIX + "originals/" + sha + ".json.gz"
            immutable(s3, bucket, original_key, gzip.compress(raw, mtime=0), compressed=True)
            source, records = ingest(spec, doc, sha, now, original_key)
            sources[sid] = source
            observations.extend(records)
        except Exception as exc:
            sources[sid] = {"source_id": sid, "key": spec["key"], "groups": spec["groups"],
                            "status": "unavailable", "research_available": False,
                            "error_type": type(exc).__name__, "records": 0, **PERMISSIONS}
        finally:
            # Do not retain all native documents (some exceed twenty MB).
            doc, raw = None, None
    manifest, entities = compose(sources, observations, now, prior.get("entity_states"))
    # Identity includes the actual build time, compiler bytes and prior baseline.
    from pathlib import Path
    import research_network, research_network_registry
    manifest["compiler"] = {Path(m.__file__).name: hashlib.sha256(Path(m.__file__).read_bytes()).hexdigest()
                              for m in (research_network, research_network_registry)}
    manifest["compiler"][Path(__file__).name] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    manifest["previous_publication_id"] = prior.get("publication_id")
    manifest["consumer_processing"] = {}
    for consumer in SUBSCRIPTIONS:
        if consumer == "portfolio-risk":
            manifest["consumer_processing"][consumer] = {"status": "private_receipt_in_authenticated_risk_output"}
            continue
        receipt = next((source["consumer_receipt"] for source in sources.values()
                        if (source.get("consumer_receipt") or {}).get("consumer") == consumer), None)
        manifest["consumer_processing"][consumer] = {
            "status": "receipt_observed" if receipt else "no_receipt_observed",
            "receipt": receipt,
            "processed_previous_publication": bool(receipt and receipt.get('status') == 'available'
                                                   and prior.get("publication_id")
                                                   and receipt.get("publication_id") == prior.get("publication_id")),
            "trigger": "existing_schedule; network events do not recursively invoke source engines"}
    manifest["publication_id"] = digest(manifest)
    pub = manifest["publication_id"]
    manifest["entity_states"] = {}
    shards = {}
    for eid, item in entities.items():
        shard = hashlib.sha256(eid.encode()).hexdigest()[:2]
        shards.setdefault(shard, {})[eid] = item
        manifest["entity_states"][eid] = {"symbol": item["identity"]["symbol"], "shard": shard,
                                           "groups": item["groups"], "evidence_ids": item["evidence_ids"],
                                           "observation_fingerprints": item["observation_fingerprints"]}
    manifest["shards"] = {}
    for shard, items in sorted(shards.items()):
        body = canonical({"contract": CONTRACT, "publication_id": pub, "entities": items, **PERMISSIONS})
        key = PREFIX + "publications/" + pub + "/" + shard + ".json"
        immutable(s3, bucket, key, body)
        manifest["shards"][shard] = {"key": key, "sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)}
    manifest["snapshot_key"] = PREFIX + "publications/" + pub + "/manifest.json"
    body = canonical(manifest)
    immutable(s3, bucket, manifest["snapshot_key"], body)
    # No stale writer can replace a publication completed by a concurrent run.
    s3.put_object(Bucket=bucket, Key=KEY, Body=body, ContentType="application/json",
                  CacheControl="public,max-age=60",
                  **({"IfMatch": prior_etag} if prior_etag else {"IfNoneMatch": "*"}))
    return manifest


def validate_manifest(doc):
    if not isinstance(doc, dict) or doc.get("contract") != CONTRACT or doc.get("access") != "PUBLIC_RESEARCH":
        raise ValueError("research network contract unavailable")
    pub = doc.get("publication_id")
    if not isinstance(pub, str) or not re.fullmatch("[0-9a-f]{64}", pub):
        raise ValueError("invalid research publication identity")
    if any(doc.get(k) != v or type(doc.get(k)) is not type(v) for k, v in PERMISSIONS.items()):
        raise ValueError("research network authority mismatch")
    if doc.get("snapshot_key") != PREFIX + "publications/" + pub + "/manifest.json":
        raise ValueError("research publication pointer mismatch")
    return doc


def read_entity(s3, bucket, manifest, eid, cache=None):
    validate_manifest(manifest)
    item = manifest.get("entity_states", {}).get(eid)
    if item is None:
        return None
    shard = hashlib.sha256(eid.encode()).hexdigest()[:2]
    ref = manifest.get("shards", {}).get(shard, {})
    expected = PREFIX + "publications/" + manifest["publication_id"] + "/" + shard + ".json"
    if item.get("shard") != shard or ref.get("key") != expected:
        raise ValueError("entity shard reference mismatch")
    cache = {} if cache is None else cache
    if expected not in cache:
        doc, raw, _ = read(s3, bucket, expected)
        if hashlib.sha256(raw).hexdigest() != ref.get("sha256") or len(raw) != ref.get("bytes"):
            raise ValueError("entity shard content mismatch")
        if doc.get("publication_id") != manifest["publication_id"] or doc.get("contract") != CONTRACT:
            raise ValueError("entity shard publication mismatch")
        cache[expected] = doc
    result = cache[expected].get("entities", {}).get(eid)
    if not isinstance(result, dict) or result.get("entity_id") != eid:
        raise ValueError("entity absent from declared shard")
    return result
