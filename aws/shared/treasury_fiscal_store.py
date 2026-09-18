"""Bounded FiscalData acquisition, immutable replay inputs, conditional history merge."""
import gzip
import hashlib
import io
import json
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

from evidence_store import capture, read_verified
from raw_snapshot import snapshot, snapshot_receipt, read_snapshot
from treasury_fiscal_model import BASE, DATASETS, CONTRACT, build, encoded, digest, latest

MAX_RESPONSE = 12 * 1024 * 1024
MAX_WAREHOUSE = 48 * 1024 * 1024


def now():
    return datetime.now(timezone.utc).isoformat()


def error_code(exc):
    return str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))


def get(client, bucket, key):
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        if error_code(exc) in ("NoSuchKey", "404"):
            return None, None, None
        raise
    blob = obj["Body"].read(MAX_WAREHOUSE + 1)
    if len(blob) > MAX_WAREHOUSE:
        raise ValueError("warehouse exceeds bound")
    if key.endswith(".gz"):
        with gzip.GzipFile(fileobj=io.BytesIO(blob)) as stream:
            blob = stream.read(MAX_WAREHOUSE + 1)
    if len(blob) > MAX_WAREHOUSE:
        raise ValueError("decoded warehouse exceeds bound")
    return json.loads(blob), obj["ETag"], blob


def acquire(dataset, bucket, before=None, page_size=2500):
    params = {"sort": "-record_date", "page[size]": page_size, "page[number]": 1}
    if before:
        # Date keyset traversal is stable as new daily rows arrive; page numbers are not.
        from datetime import date
        if date.fromisoformat(before).isoformat() != before:
            raise ValueError("invalid backfill boundary")
        params["filter"] = "record_date:lt:" + before
    url = BASE + DATASETS[dataset]["path"] + "?" + urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl fiscal research admin@justhodl.ai"})
    with urllib.request.urlopen(req, timeout=40) as response:
        raw = response.read(MAX_RESPONSE + 1)
    acquired_at = now()
    if len(raw) > MAX_RESPONSE:
        raise ValueError("provider response exceeds bound; no partial capture")
    doc = json.loads(raw)
    key = snapshot("treasury", url, raw, bucket=bucket)
    if not key or read_snapshot(key, bucket=bucket) != raw:
        raise ValueError("original response readback failed")
    receipt = snapshot_receipt(key, bucket=bucket)
    if not isinstance(doc.get("data"), list):
        raise ValueError("provider data rows unavailable")
    return {"document": doc, "receipt": receipt, "acquired_at": acquired_at}


def _immutable(client, bucket, key, body, content_type):
    try:
        client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type, IfNoneMatch="*")
    except Exception as exc:
        if error_code(exc) not in ("412", "PreconditionFailed", "409", "ConditionalRequestConflict"):
            raise
        existing = client.get_object(Bucket=bucket, Key=key)["Body"].read(len(body) + 1)
        if existing != body:
            raise ValueError("immutable replay object differs") from exc


def merge(client, bucket, dataset, pages):
    key = "data/warm/treasury/" + dataset + ".json.gz"
    compiler = Path(__file__).with_name("treasury_fiscal_model.py").read_bytes()
    compiler_sha = hashlib.sha256(compiler).hexdigest()
    compiler_key = "data/treasury-fiscal-replay/compilers/" + compiler_sha + ".py"
    _immutable(client, bucket, compiler_key, compiler, "text/plain")
    for attempt in range(4):
        old, etag, previous_bytes = get(client, bucket, key)
        previous_receipt = None
        if previous_bytes is not None:
            previous_receipt = capture(client, bucket, "warehouse", "https://" + bucket + ".s3.amazonaws.com/" + key, previous_bytes)
            if read_verified(client, bucket, previous_receipt) != previous_bytes:
                raise ValueError("previous history readback failed")
        stamp = now()
        out = build(dataset, old, pages, stamp, previous_receipt)
        output_sha = digest(out)
        manifest = {"contract": "treasury-fiscal-replay.v1", "dataset": dataset, "generated_at": stamp,
                    "compiler": {"key": compiler_key, "sha256": compiler_sha},
                    "previous_evidence": previous_receipt,
                    "pages": [{"receipt": p["receipt"], "acquired_at": p["acquired_at"]} for p in pages],
                    "output_sha256": output_sha, "publication_time_verified": False,
                    "scope": "deterministic current-vintage merge of retained previous warehouse and original responses",
                    "sizing_eligible": False}
        manifest_key = "data/treasury-fiscal-replay/runs/" + digest(manifest) + ".json"
        _immutable(client, bucket, manifest_key, encoded(manifest), "application/json")
        # Replay from stored inputs, not the in-memory provider response.
        previous_replay = json.loads(read_verified(client, bucket, previous_receipt)) if previous_receipt else None
        replay_pages = [{"document": json.loads(read_snapshot(p["receipt"]["key"], bucket=bucket)),
                         "receipt": p["receipt"], "acquired_at": p["acquired_at"]} for p in pages]
        if digest(build(dataset, previous_replay, replay_pages, stamp, previous_receipt)) != output_sha:
            raise ValueError("retained input replay differs")
        out["replay"] = {"manifest_key": manifest_key, "output_sha256": output_sha, "compiler_sha256": compiler_sha,
                         "retained_inputs_replayed": True, "legacy_rows_promoted": 0}
        body = encoded(out)
        if len(body) > MAX_WAREHOUSE:
            raise ValueError("merged warehouse exceeds bound; prior data retained")
        try:
            client.put_object(Bucket=bucket, Key=key, Body=gzip.compress(body, mtime=0),
                              ContentType="application/gzip", **({"IfMatch": etag} if etag else {"IfNoneMatch": "*"}))
            return out
        except Exception as exc:
            if error_code(exc) not in ("412", "PreconditionFailed", "409", "ConditionalRequestConflict"):
                raise
    raise RuntimeError("concurrent Treasury writers exceeded conditional retry bound")


def summary(client, bucket, errors=None):
    """Read current warehouse each retry so a backfill cannot regress the summary."""
    key = "data/warm/treasury/latest-summary.json"
    for attempt in range(4):
        _, etag, _ = get(client, bucket, key)
        stamp = now()
        out = {"contract": CONTRACT, "version": "2.0.0", "generated_at": stamp, "as_of": stamp,
               "datasets": {}, "errors": errors or {}, "sizing_eligible": False}
        for ds in DATASETS:
            doc, _, _ = get(client, bucket, "data/warm/treasury/" + ds + ".json.gz")
            if not doc or doc.get("contract") != CONTRACT:
                out["datasets"][ds] = {"data_unavailable": True, "reason": "dimensional migration pending"}
            else:
                entry = latest(doc, stamp)
                entry["replay"] = doc.get("replay")
                entry["acquisition"] = doc.get("acquisition")
                out["datasets"][ds] = entry
        try:
            client.put_object(Bucket=bucket, Key=key, Body=encoded(out), ContentType="application/json", CacheControl="no-cache",
                              **({"IfMatch": etag} if etag else {"IfNoneMatch": "*"}))
            return out
        except Exception as exc:
            if error_code(exc) not in ("412", "PreconditionFailed", "409", "ConditionalRequestConflict"):
                raise
    raise RuntimeError("summary concurrent write limit")
