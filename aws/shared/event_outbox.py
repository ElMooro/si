"""Durable, conditional S3 checkpoint + pending events. At-least-once delivery.

The checkpoint and its outbox are one object, committed before any emission.
A failed acknowledgement write can duplicate an event, never silently lose it.
Consumers must deduplicate detail._event_id. No consumer delivery is inferred
from an EventBridge acceptance. Concurrent stale writers fail the S3 condition.
"""
import hashlib
import json


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def read_state(s3, bucket, key):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        if str(getattr(exc, "response", {}).get("Error", {}).get("Code")) in ("NoSuchKey", "404"):
            return {}, None
        raise
    body = response["Body"]
    try:
        raw = body.read(8_000_001)
        if len(raw) > 8_000_000:
            raise ValueError("outbox size exceeded; checkpoint not advanced")
        state = json.loads(raw)
        if not isinstance(state, dict):
            raise ValueError("invalid outbox checkpoint")
        return state, response.get("ETag")
    finally:
        body.close()


def advance(s3, bucket, key, state, etag, checkpoint, events, publish):
    pending = {r["id"]: r for r in state.get("pending", [])}
    for event in events:
        name, detail, source = event
        event_id = hashlib.sha256(canonical({"before": state.get("checkpoint", state),
                                            "after": checkpoint, "event": event})).hexdigest()
        pending.setdefault(event_id, {"id": event_id, "event": [name, dict(detail, _event_id=event_id), source]})
    next_state = {"contract": "event-outbox.v1", "checkpoint": checkpoint,
                  "pending": list(pending.values())}
    raw = canonical(next_state)
    if len(raw) > 8_000_000:
        raise ValueError("outbox capacity exceeded; checkpoint not advanced")
    response = s3.put_object(Bucket=bucket, Key=key, Body=raw, ContentType="application/json",
                             **({"IfMatch": etag} if etag else {"IfNoneMatch": "*"}))
    result = publish([r["event"] for r in next_state["pending"]])
    accepted = {r["index"] for r in result.get("results", []) if r.get("ok") is True}
    next_state["pending"] = [r for i, r in enumerate(next_state["pending"]) if i not in accepted]
    # An absent ETag cannot safely acknowledge a concurrent state.
    if response.get("ETag"):
        s3.put_object(Bucket=bucket, Key=key, Body=canonical(next_state),
                      ContentType="application/json", IfMatch=response["ETag"])
    return {"accepted": len(accepted), "pending": len(next_state["pending"]),
            "delivery_semantics": "at_least_once", "consumer_processed": False}
