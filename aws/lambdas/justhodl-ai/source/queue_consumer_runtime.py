"""Shared SQS/EventBridge consumer runtime with partial-batch failures.

Consumers process each message independently and report only failed message
identifiers to Lambda.  Successful records are never replayed merely because a
sibling record was malformed.  Stable operation receipts make retries safe
when a process stops after a naturally idempotent downstream write.
"""
from __future__ import annotations

import hashlib
import io
import json
import logging
import re
from copy import deepcopy
from typing import Any, Callable, Dict, Mapping, Optional, Tuple


LOGGER = logging.getLogger(__name__)
_SAFE_ID = re.compile(r"^[A-Za-z0-9._:/#-]{1,256}$")


class ConsumerError(ValueError):
    """A queue message or idempotency receipt is invalid."""


class IdempotencyConflict(ConsumerError):
    """A stable operation identifier was reused for different content."""


def canonical_json(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ConsumerError("event must be finite JSON") from exc


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def _conditional_failure(exc: Exception) -> bool:
    text = (type(exc).__name__ + " " + str(exc)).lower()
    return (
        "precondition" in text
        or "conditional" in text
        or "already exists" in text
        or "412" in text
    )


def _body_bytes(response: Mapping[str, Any]) -> bytes:
    body = response.get("Body")
    if hasattr(body, "read"):
        body = body.read()
    if isinstance(body, str):
        body = body.encode("utf-8")
    if not isinstance(body, (bytes, bytearray)):
        raise ConsumerError("stored receipt body is not readable")
    return bytes(body)


class S3IdempotencyStore:
    """Small receipt store whose retry semantics tolerate uncertain completion."""

    def __init__(self, s3_client: Any, bucket: str, prefix: str) -> None:
        if not bucket or not prefix:
            raise ConsumerError("idempotency bucket and prefix are required")
        self.s3 = s3_client
        self.bucket = bucket
        self.prefix = prefix.strip("/") + "/"

    def key(self, operation_id: str) -> str:
        if not isinstance(operation_id, str) or not _SAFE_ID.fullmatch(operation_id):
            raise ConsumerError("operation_id is invalid")
        return self.prefix + operation_id + ".json"

    def _get(self, operation_id: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        try:
            response = self.s3.get_object(
                Bucket=self.bucket, Key=self.key(operation_id)
            )
        except Exception as exc:
            text = (type(exc).__name__ + " " + str(exc)).lower()
            if "nosuchkey" in text or "not found" in text or isinstance(exc, KeyError):
                return None, None
            raise
        try:
            value = json.loads(_body_bytes(response))
        except (TypeError, ValueError) as exc:
            raise ConsumerError("idempotency receipt is invalid JSON") from exc
        if not isinstance(value, dict):
            raise ConsumerError("idempotency receipt must be an object")
        return value, response.get("ETag")

    def begin(self, operation_id: str, payload: Mapping[str, Any]) -> Dict[str, Any]:
        payload_digest = digest(payload)
        pending = {
            "schema_version": "1.0",
            "operation_id": operation_id,
            "payload_digest": payload_digest,
            "status": "PENDING",
        }
        try:
            response = self.s3.put_object(
                Bucket=self.bucket,
                Key=self.key(operation_id),
                Body=canonical_json(pending),
                ContentType="application/json",
                CacheControl="private, no-store",
                ServerSideEncryption="AES256",
                IfNoneMatch="*",
            )
            return {"duplicate": False, "complete": False, "etag": response.get("ETag")}
        except Exception as exc:
            if not _conditional_failure(exc):
                raise
        current, etag = self._get(operation_id)
        if current is None:
            raise ConsumerError("conditional receipt conflict could not be read")
        if current.get("payload_digest") != payload_digest:
            raise IdempotencyConflict(
                "operation identity collides with different event content"
            )
        status = current.get("status")
        if status not in ("PENDING", "COMPLETED"):
            raise ConsumerError("idempotency receipt has an invalid status")
        return {"duplicate": True, "complete": status == "COMPLETED", "etag": etag}

    def complete(
        self,
        operation_id: str,
        payload: Mapping[str, Any],
        result: Mapping[str, Any],
    ) -> None:
        current, etag = self._get(operation_id)
        if current is None or current.get("payload_digest") != digest(payload):
            raise IdempotencyConflict("pending idempotency receipt was replaced")
        if current.get("status") == "COMPLETED":
            return
        completed = dict(current)
        completed.update(
            {
                "status": "COMPLETED",
                "result_digest": digest(result),
            }
        )
        request = {
            "Bucket": self.bucket,
            "Key": self.key(operation_id),
            "Body": canonical_json(completed),
            "ContentType": "application/json",
            "CacheControl": "private, no-store",
            "ServerSideEncryption": "AES256",
        }
        if etag:
            request["IfMatch"] = etag
        try:
            self.s3.put_object(**request)
        except Exception as exc:
            if not _conditional_failure(exc):
                raise
            latest, _ = self._get(operation_id)
            if (
                latest is None
                or latest.get("payload_digest") != current.get("payload_digest")
                or latest.get("status") != "COMPLETED"
            ):
                raise IdempotencyConflict("receipt completion raced with different content")


def eventbridge_event(record: Mapping[str, Any]) -> Tuple[str, Dict[str, Any]]:
    if not isinstance(record, Mapping):
        raise ConsumerError("SQS record must be an object")
    message_id = str(record.get("messageId") or "").strip()
    if not message_id or not _SAFE_ID.fullmatch(message_id):
        raise ConsumerError("SQS record messageId is missing or invalid")
    body = record.get("body")
    if not isinstance(body, str):
        raise ConsumerError("SQS record body must be a JSON string")
    try:
        event = json.loads(body)
    except ValueError as exc:
        raise ConsumerError("SQS record body is invalid JSON") from exc
    if not isinstance(event, dict) or not isinstance(event.get("detail"), dict):
        raise ConsumerError("SQS body must contain an EventBridge detail object")
    return message_id, event


def handle_sqs_batch(
    event: Mapping[str, Any],
    processor: Callable[[Mapping[str, Any]], Mapping[str, Any]],
    *,
    logger: logging.Logger = LOGGER,
) -> Dict[str, Any]:
    """Process a Lambda SQS batch and return ReportBatchItemFailures shape."""
    records = event.get("Records") if isinstance(event, Mapping) else None
    if not isinstance(records, list):
        raise ConsumerError("Lambda event Records must be a list")
    failures = []
    seen = set()
    for record in records:
        message_id = None
        try:
            message_id, parsed = eventbridge_event(record)
            if message_id in seen:
                raise ConsumerError("duplicate messageId in one Lambda batch")
            seen.add(message_id)
            processor(parsed)
        except Exception as exc:
            candidate = message_id
            if candidate is None and isinstance(record, Mapping):
                raw = str(record.get("messageId") or "").strip()
                candidate = raw if _SAFE_ID.fullmatch(raw) else None
            # Lambda cannot redrive a record without an item identifier.  Raising
            # fails the complete batch, which is safer than acknowledging poison.
            if candidate is None:
                raise
            failures.append({"itemIdentifier": candidate})
            logger.error(
                "queue record failed",
                extra={
                    "message_id": candidate,
                    "error_type": type(exc).__name__,
                },
            )
    return {"batchItemFailures": failures}
