"""Durable S3 outbox for governed events.

The outbox record is written before the compatibility archive and before
EventBridge publication.  A versioned state object moves through
PENDING -> DELIVERING -> PUBLISHED using S3 conditional writes.  A process
failure can therefore leave a retryable record, never an archive-only event
that future duplicate submissions silently ignore.

Delivery remains intentionally at-least-once: a crash after EventBridge accepts
an event but before the PUBLISHED transition can cause a redrive duplicate.
Consumers must deduplicate on the stable message/fingerprint ID.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Mapping, Optional, Tuple


class OutboxError(RuntimeError):
    pass


class OutboxConflictError(OutboxError):
    pass


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise OutboxError("outbox clock must return a timezone-aware datetime")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_time(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise OutboxError("%s is missing from outbox state" % field)
    text = value.strip()
    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00" if text.endswith("Z") else text)
    except ValueError as exc:
        raise OutboxError("%s is invalid in outbox state" % field) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise OutboxError("%s is not timezone-aware in outbox state" % field)
    return parsed.astimezone(timezone.utc)


def _json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise OutboxError("outbox payload must contain finite JSON") from exc


def _digest(value: Any) -> str:
    return hashlib.sha256(_json_bytes(value)).hexdigest()


def _exception_text(exc: Exception) -> str:
    response = getattr(exc, "response", None)
    code = ""
    status = ""
    if isinstance(response, Mapping):
        error = response.get("Error") or {}
        code = str(error.get("Code") or "")
        status = str((response.get("ResponseMetadata") or {}).get("HTTPStatusCode") or "")
    return ("%s %s %s %s" % (type(exc).__name__, str(exc), code, status)).lower()


def _missing(exc: Exception) -> bool:
    text = _exception_text(exc)
    return isinstance(exc, KeyError) or any(
        marker in text for marker in ("nosuchkey", "not found", "404")
    )


def _conditional_conflict(exc: Exception) -> bool:
    text = _exception_text(exc)
    return any(
        marker in text
        for marker in ("preconditionfailed", "precondition failed", "conditional", "412")
    )


class S3EventOutbox:
    """S3-CAS outbox with bounded retries and dependency-injected delivery."""

    VALID_STATES = frozenset(("PENDING", "DELIVERING", "PUBLISHED"))

    def __init__(
        self,
        s3_client: Any,
        *,
        outbox_bucket: str,
        archive_bucket: str,
        prefix: str = "ai/signals/outbox/v1",
        lease_seconds: int = 60,
        max_cas_attempts: int = 4,
        now: Callable[[], datetime] = _now_utc,
    ) -> None:
        if not outbox_bucket or not archive_bucket:
            raise OutboxError("outbox_bucket and archive_bucket are required")
        if isinstance(lease_seconds, bool) or not 1 <= int(lease_seconds) <= 900:
            raise OutboxError("lease_seconds is outside the reviewed bound")
        if isinstance(max_cas_attempts, bool) or not 1 <= int(max_cas_attempts) <= 10:
            raise OutboxError("max_cas_attempts is outside the reviewed bound")
        self.s3 = s3_client
        self.outbox_bucket = outbox_bucket
        self.archive_bucket = archive_bucket
        self.prefix = prefix.strip("/")
        self.lease_seconds = int(lease_seconds)
        self.max_cas_attempts = int(max_cas_attempts)
        self.now = now

    def state_key(self, message_id: str) -> str:
        if (
            not isinstance(message_id, str)
            or len(message_id) != 64
            or any(char not in "0123456789abcdef" for char in message_id)
        ):
            raise OutboxError("message_id must be a lowercase SHA-256 digest")
        return "%s/%s.json" % (self.prefix, message_id)

    def _get(self, message_id: str) -> Optional[Tuple[Dict[str, Any], str]]:
        key = self.state_key(message_id)
        try:
            response = self.s3.get_object(Bucket=self.outbox_bucket, Key=key)
        except Exception as exc:
            if _missing(exc):
                return None
            raise
        try:
            state = json.loads(response["Body"].read())
        except Exception as exc:
            raise OutboxError("outbox state is not valid JSON") from exc
        etag = response.get("ETag")
        if not isinstance(etag, str) or not etag:
            raise OutboxError("outbox state is missing an ETag")
        self._validate_state(state, message_id)
        return state, etag

    def _validate_state(self, state: Any, message_id: str) -> None:
        if not isinstance(state, Mapping):
            raise OutboxError("outbox state must be an object")
        if state.get("schema_version") != "1.0" or state.get("message_id") != message_id:
            raise OutboxError("outbox state identity does not match")
        if state.get("status") not in self.VALID_STATES:
            raise OutboxError("outbox state has an invalid status")
        if _digest(state.get("payload")) != state.get("payload_digest"):
            raise OutboxError("outbox payload digest does not match")
        attempts = state.get("attempts")
        if isinstance(attempts, bool) or not isinstance(attempts, int) or attempts < 0:
            raise OutboxError("outbox attempts is invalid")
        _parse_time(state.get("created_at"), "created_at")
        _parse_time(state.get("updated_at"), "updated_at")

    def _put_new(self, message_id: str, state: Mapping[str, Any]) -> bool:
        try:
            self.s3.put_object(
                Bucket=self.outbox_bucket,
                Key=self.state_key(message_id),
                Body=_json_bytes(state),
                ContentType="application/json",
                CacheControl="private, no-store",
                IfNoneMatch="*",
            )
            return True
        except Exception as exc:
            if _conditional_conflict(exc):
                return False
            raise

    def _replace(self, message_id: str, state: Mapping[str, Any], etag: str) -> str:
        try:
            response = self.s3.put_object(
                Bucket=self.outbox_bucket,
                Key=self.state_key(message_id),
                Body=_json_bytes(state),
                ContentType="application/json",
                CacheControl="private, no-store",
                IfMatch=etag,
            )
        except Exception as exc:
            if _conditional_conflict(exc):
                raise OutboxConflictError("outbox state changed concurrently") from exc
            raise
        next_etag = response.get("ETag") if isinstance(response, Mapping) else None
        if isinstance(next_etag, str) and next_etag:
            return next_etag
        loaded = self._get(message_id)
        if not loaded:
            raise OutboxError("outbox state disappeared after transition")
        return loaded[1]

    def stage(
        self,
        *,
        message_id: str,
        payload: Mapping[str, Any],
        archive_key: str,
        event_contract: Mapping[str, str],
    ) -> Tuple[Dict[str, Any], bool]:
        if not isinstance(payload, Mapping):
            raise OutboxError("outbox payload must be an object")
        if not isinstance(archive_key, str) or not archive_key:
            raise OutboxError("archive_key is required")
        if not isinstance(event_contract, Mapping):
            raise OutboxError("event_contract must be an object")
        timestamp = _iso(self.now())
        state: Dict[str, Any] = {
            "schema_version": "1.0",
            "message_id": message_id,
            "payload_digest": _digest(payload),
            "status": "PENDING",
            "attempts": 0,
            "created_at": timestamp,
            "updated_at": timestamp,
            "lease_until": None,
            "archive_key": archive_key,
            "event_contract": dict(event_contract),
            "payload": dict(payload),
        }
        created = self._put_new(message_id, state)
        loaded = self._get(message_id)
        if not loaded:
            raise OutboxError("outbox state was not durable after staging")
        existing = loaded[0]
        if (
            existing.get("payload_digest") != state["payload_digest"]
            or existing.get("archive_key") != archive_key
            or existing.get("event_contract") != state["event_contract"]
        ):
            raise OutboxConflictError("outbox message ID already has different content")
        return existing, created

    def ensure_archive(self, state: Mapping[str, Any]) -> bool:
        key = str(state.get("archive_key") or "")
        payload = state.get("payload")
        try:
            self.s3.put_object(
                Bucket=self.archive_bucket,
                Key=key,
                Body=_json_bytes(payload),
                ContentType="application/json",
                CacheControl="private, no-store",
                IfNoneMatch="*",
            )
            return True
        except Exception as exc:
            if not _conditional_conflict(exc):
                raise
        try:
            existing = json.loads(
                self.s3.get_object(Bucket=self.archive_bucket, Key=key)["Body"].read()
            )
        except Exception as exc:
            raise OutboxConflictError("existing archive could not be verified") from exc
        if _digest(existing) != state.get("payload_digest"):
            raise OutboxConflictError("immutable archive key contains different content")
        return False

    def _claim(self, message_id: str) -> Tuple[Dict[str, Any], Optional[str], str]:
        for _ in range(self.max_cas_attempts):
            loaded = self._get(message_id)
            if not loaded:
                raise OutboxError("outbox message does not exist")
            state, etag = loaded
            if state["status"] == "PUBLISHED":
                return state, None, "PUBLISHED"
            now = self.now()
            if state["status"] == "DELIVERING":
                lease = _parse_time(state.get("lease_until"), "lease_until")
                if lease > now.astimezone(timezone.utc):
                    return state, None, "IN_FLIGHT"
            claimed = dict(state)
            claimed.update({
                "status": "DELIVERING",
                "attempts": state["attempts"] + 1,
                "updated_at": _iso(now),
                "lease_until": _iso(now + timedelta(seconds=self.lease_seconds)),
            })
            claimed.pop("last_failure_at", None)
            try:
                next_etag = self._replace(message_id, claimed, etag)
                return claimed, next_etag, "CLAIMED"
            except OutboxConflictError:
                continue
        raise OutboxConflictError("could not claim outbox message after bounded retries")

    def _release_after_failure(
        self, message_id: str, state: Mapping[str, Any], etag: str
    ) -> None:
        pending = dict(state)
        timestamp = _iso(self.now())
        pending.update({
            "status": "PENDING",
            "updated_at": timestamp,
            "lease_until": None,
            "last_failure_at": timestamp,
        })
        try:
            self._replace(message_id, pending, etag)
        except OutboxConflictError:
            # A competing redrive may already have advanced the state.  The
            # durable DELIVERING lease also makes an unsuccessful release safe.
            pass

    def deliver(
        self,
        message_id: str,
        publisher: Callable[[Mapping[str, Any], Mapping[str, str]], str],
    ) -> Dict[str, Any]:
        state, etag, claim_status = self._claim(message_id)
        if claim_status in ("PUBLISHED", "IN_FLIGHT"):
            return {
                "status": claim_status,
                "published": claim_status == "PUBLISHED",
                "attempts": state["attempts"],
                "event_id": state.get("event_id"),
                "delivery_attempted": False,
            }
        assert etag is not None
        try:
            event_id = publisher(state["payload"], state["event_contract"])
            if not isinstance(event_id, str) or not event_id:
                raise OutboxError("publisher did not return an event ID")
        except Exception:
            self._release_after_failure(message_id, state, etag)
            raise
        published = dict(state)
        timestamp = _iso(self.now())
        published.update({
            "status": "PUBLISHED",
            "updated_at": timestamp,
            "published_at": timestamp,
            "lease_until": None,
            "event_id": event_id,
        })
        try:
            self._replace(message_id, published, etag)
        except OutboxConflictError:
            current = self._get(message_id)
            if current and current[0].get("status") == "PUBLISHED":
                published = current[0]
            else:
                raise OutboxError(
                    "event was accepted but outbox receipt was not committed; redrive may duplicate"
                )
        return {
            "status": "PUBLISHED",
            "published": True,
            "attempts": published["attempts"],
            "event_id": published.get("event_id"),
            "delivery_attempted": True,
        }

    def ingest(
        self,
        *,
        message_id: str,
        payload: Mapping[str, Any],
        archive_key: str,
        event_contract: Mapping[str, str],
        publisher: Callable[[Mapping[str, Any], Mapping[str, str]], str],
    ) -> Dict[str, Any]:
        state, created = self.stage(
            message_id=message_id,
            payload=payload,
            archive_key=archive_key,
            event_contract=event_contract,
        )
        archive_created = self.ensure_archive(state)
        delivery = self.deliver(message_id, publisher)
        return {
            **delivery,
            "created": created,
            "duplicate": not created,
            "redriven": not created and bool(delivery["delivery_attempted"]),
            "archived": True,
            "archive_created": archive_created,
            "outbox_key": self.state_key(message_id),
        }

    def redrive(
        self,
        message_id: str,
        publisher: Callable[[Mapping[str, Any], Mapping[str, str]], str],
    ) -> Dict[str, Any]:
        loaded = self._get(message_id)
        if not loaded:
            raise OutboxError("outbox message does not exist")
        archive_created = self.ensure_archive(loaded[0])
        delivery = self.deliver(message_id, publisher)
        return {
            **delivery,
            "created": False,
            "duplicate": True,
            "redriven": bool(delivery["delivery_attempted"]),
            "archived": True,
            "archive_created": archive_created,
            "outbox_key": self.state_key(message_id),
        }
