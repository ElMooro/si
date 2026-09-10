"""Versioned prediction ledger with archive-first, idempotent write contracts.

The immutable S3 object is authoritative. ``ArchiveFirstPredictionLedger``
therefore confirms that object before it attempts the DynamoDB query-index
write. An archive failure cannot leave an index row behind, while an index
failure leaves a recoverable authoritative object that the same request can
safely verify and retry.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, MutableMapping, Optional

PREDICTION_SCHEMA_VERSION = "1.0"
OUTCOME_SCHEMA_VERSION = "1.0"
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/#-]{0,255}$")


class PredictionLedgerError(ValueError):
    pass


class PredictionConflictError(PredictionLedgerError):
    pass


def _timestamp(value: Any, field: str) -> str:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise PredictionLedgerError("%s must be RFC3339" % field) from exc
    else:
        raise PredictionLedgerError("%s must be RFC3339" % field)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PredictionLedgerError("%s must include a timezone" % field)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _identifier(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not _ID.fullmatch(text):
        raise PredictionLedgerError("%s is missing or invalid" % field)
    return text


def _finite(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        raise PredictionLedgerError("%s must be finite" % field)
    return float(value)


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def make_prediction_id(
    *,
    model_id: str,
    model_version: str,
    entity_id: str,
    prediction_time: Any,
    horizon_value: int,
    horizon_unit: str,
) -> str:
    identity = {
        "model_id": _identifier(model_id, "model_id"),
        "model_version": _identifier(model_version, "model_version"),
        "entity_id": _identifier(entity_id, "entity_id"),
        "prediction_time": _timestamp(prediction_time, "prediction_time"),
        "horizon": {"value": horizon_value, "unit": horizon_unit},
    }
    return "pred-" + _digest(identity)[:40]


def validate_prediction(record: Mapping[str, Any]) -> Dict[str, Any]:
    if not isinstance(record, Mapping):
        raise PredictionLedgerError("prediction must be an object")
    if record.get("schema_version") != PREDICTION_SCHEMA_VERSION:
        raise PredictionLedgerError("unsupported prediction schema_version")
    model_id = _identifier(record.get("model_id"), "model_id")
    model_version = _identifier(record.get("model_version"), "model_version")
    entity_id = _identifier(record.get("entity_id"), "entity_id")
    prediction_time = _timestamp(record.get("prediction_time"), "prediction_time")
    created_at = _timestamp(record.get("created_at"), "created_at")
    feature_as_of = _timestamp(record.get("feature_as_of"), "feature_as_of")
    if feature_as_of > prediction_time:
        raise PredictionLedgerError("feature_as_of must not follow prediction_time")
    horizon = record.get("horizon")
    if not isinstance(horizon, Mapping):
        raise PredictionLedgerError("horizon must be an object")
    value, unit = horizon.get("value"), horizon.get("unit")
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise PredictionLedgerError("horizon.value must be a positive integer")
    if unit not in ("calendar_days", "seconds", "observations"):
        raise PredictionLedgerError("horizon.unit is invalid")
    prediction = record.get("prediction")
    if prediction not in ("POSITIVE", "NEGATIVE", "FLAT"):
        raise PredictionLedgerError("prediction is invalid")
    score = _finite(record.get("score"), "score")
    if score < 0.0 or score > 1.0:
        raise PredictionLedgerError("score must be between zero and one")
    fingerprint = str(record.get("feature_fingerprint") or "").lower()
    if not re.fullmatch(r"[a-f0-9]{64}", fingerprint):
        raise PredictionLedgerError("feature_fingerprint must be a SHA-256 digest")
    expected_id = make_prediction_id(
        model_id=model_id,
        model_version=model_version,
        entity_id=entity_id,
        prediction_time=prediction_time,
        horizon_value=value,
        horizon_unit=unit,
    )
    prediction_id = record.get("prediction_id", expected_id)
    if prediction_id != expected_id:
        raise PredictionLedgerError("prediction_id does not match immutable identity fields")
    if record.get("status", "PENDING") != "PENDING":
        raise PredictionLedgerError("new predictions must have PENDING status")
    metadata = record.get("metadata", {})
    if not isinstance(metadata, Mapping):
        raise PredictionLedgerError("metadata must be an object")
    try:
        json.dumps(metadata, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise PredictionLedgerError("metadata must be finite JSON") from exc
    return {
        "schema_version": PREDICTION_SCHEMA_VERSION,
        "prediction_id": expected_id,
        "model_id": model_id,
        "model_version": model_version,
        "entity_id": entity_id,
        "prediction_time": prediction_time,
        "feature_as_of": feature_as_of,
        "feature_fingerprint": fingerprint,
        "horizon": {"value": value, "unit": unit},
        "prediction": prediction,
        "score": score,
        "status": "PENDING",
        "created_at": created_at,
        "metadata": deepcopy(dict(metadata)),
    }


def build_prediction(
    *,
    model_id: str,
    model_version: str,
    entity_id: str,
    prediction_time: Any,
    feature_as_of: Any,
    feature_fingerprint: str,
    horizon_value: int,
    horizon_unit: str,
    prediction: str,
    score: float,
    created_at: Any,
    metadata: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    return validate_prediction({
        "schema_version": PREDICTION_SCHEMA_VERSION,
        "model_id": model_id,
        "model_version": model_version,
        "entity_id": entity_id,
        "prediction_time": prediction_time,
        "feature_as_of": feature_as_of,
        "feature_fingerprint": feature_fingerprint,
        "horizon": {"value": horizon_value, "unit": horizon_unit},
        "prediction": prediction,
        "score": score,
        "status": "PENDING",
        "created_at": created_at,
        "metadata": dict(metadata or {}),
    })


def validate_outcome(outcome: Mapping[str, Any]) -> Dict[str, Any]:
    if not isinstance(outcome, Mapping) or outcome.get("schema_version") != OUTCOME_SCHEMA_VERSION:
        raise PredictionLedgerError("unsupported outcome schema_version")
    label = outcome.get("label")
    if label not in ("POSITIVE", "NEGATIVE", "FLAT"):
        raise PredictionLedgerError("outcome label is invalid")
    entry_time = _timestamp(outcome.get("entry_time"), "outcome.entry_time")
    exit_time = _timestamp(outcome.get("exit_time"), "outcome.exit_time")
    if exit_time <= entry_time:
        raise PredictionLedgerError("outcome.exit_time must follow entry_time")
    net_return = _finite(outcome.get("net_return"), "outcome.net_return")
    transaction_cost = _finite(outcome.get("transaction_cost", 0.0), "outcome.transaction_cost")
    if transaction_cost < 0:
        raise PredictionLedgerError("outcome.transaction_cost must be non-negative")
    return {
        "schema_version": OUTCOME_SCHEMA_VERSION,
        "label": label,
        "entry_time": entry_time,
        "exit_time": exit_time,
        "net_return": net_return,
        "transaction_cost": transaction_cost,
    }


def _is_conditional_failure(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    code = ""
    status = ""
    if isinstance(response, Mapping):
        code = str((response.get("Error") or {}).get("Code") or "")
        status = str((response.get("ResponseMetadata") or {}).get("HTTPStatusCode") or "")
    text = (type(exc).__name__ + " " + str(exc) + " " + code + " " + status).lower()
    return any(marker in text for marker in (
        "conditional",
        "already exists",
        "duplicate",
        "preconditionfailed",
        "precondition failed",
        "412",
    ))


def _json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise PredictionLedgerError("ledger records must contain finite JSON") from exc


def _read_body(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, str):
        return value.encode("utf-8")
    if hasattr(value, "read"):
        body = value.read()
        return body if isinstance(body, bytes) else str(body).encode("utf-8")
    raise PredictionLedgerError("archive response body is unreadable")


def _archive_stamp(created_at: Any) -> str:
    text = _timestamp(created_at, "created_at")
    return re.sub(r"[^0-9A-Za-z_.-]", "-", text)


class ArchiveFirstPredictionLedger:
    """Immutable S3 authority plus a recoverable DynamoDB query index.

    S3 conditional creation and content verification provide idempotency. The
    table is touched only after the authoritative object exists and matches.
    A crash or table error after that point is recoverable by replaying the
    same record: the archive is verified, then the conditional index put is
    retried.
    """

    ARCHIVE_KINDS = frozenset(("records", "grades"))

    def __init__(
        self,
        store: Any,
        s3_client: Any,
        archive_bucket: str,
        *,
        archive_prefix: str = "predictions",
    ) -> None:
        if not archive_bucket:
            raise PredictionLedgerError("archive_bucket is required")
        prefix = str(archive_prefix or "").strip("/")
        if prefix != "predictions":
            raise PredictionLedgerError("archive_prefix must be predictions")
        self.store = store
        self.s3 = s3_client
        self.archive_bucket = archive_bucket
        self.archive_prefix = prefix

    def archive_key(
        self,
        *,
        kind: str,
        identifier: str,
        created_at: Any,
    ) -> str:
        if kind not in self.ARCHIVE_KINDS:
            raise PredictionLedgerError("archive kind is invalid")
        identity = _identifier(identifier, "archive identifier")
        return "%s/%s/%s/%s.json" % (
            self.archive_prefix,
            kind,
            identity,
            _archive_stamp(created_at),
        )

    def _ensure_archive(
        self,
        record: Mapping[str, Any],
        *,
        key: str,
    ) -> bool:
        body = _json_bytes(record)
        try:
            self.s3.put_object(
                Bucket=self.archive_bucket,
                Key=key,
                Body=body,
                ContentType="application/json",
                CacheControl="private, no-store",
                IfNoneMatch="*",
                Metadata={"sha256": hashlib.sha256(body).hexdigest()},
            )
            return True
        except Exception as exc:
            if not _is_conditional_failure(exc):
                # Fail closed: no table operation has occurred yet.
                raise

        try:
            response = self.s3.get_object(Bucket=self.archive_bucket, Key=key)
            existing = json.loads(_read_body(response["Body"]))
        except Exception as exc:
            raise PredictionConflictError(
                "existing immutable archive could not be verified"
            ) from exc
        if _digest(existing) != _digest(record):
            raise PredictionConflictError(
                "immutable archive key contains different content"
            )
        return False

    def _put_index_item(self, item: Mapping[str, Any]) -> Dict[str, Any]:
        prediction_id = _identifier(item.get("prediction_id"), "prediction_id")
        created_at = _timestamp(item.get("created_at"), "created_at")
        normalized = deepcopy(dict(item))
        normalized["prediction_id"] = prediction_id
        normalized["created_at"] = created_at
        written = False
        if isinstance(self.store, MutableMapping):
            current = self.store.get(prediction_id)
            if current is None:
                self.store[prediction_id] = deepcopy(normalized)
                current = normalized
                written = True
        elif hasattr(self.store, "put_if_absent"):
            written = bool(
                self.store.put_if_absent(prediction_id, deepcopy(normalized))
            )
            current = normalized if written else PredictionLedger(self.store)._get(
                prediction_id, created_at
            )
        else:
            try:
                self.store.put_item(
                    Item=deepcopy(normalized),
                    ConditionExpression="attribute_not_exists(prediction_id)",
                )
                written = True
            except Exception as exc:
                if not _is_conditional_failure(exc):
                    raise
            current = normalized if written else PredictionLedger(self.store)._get(
                prediction_id, created_at
            )
        if current is None:
            raise PredictionConflictError(
                "duplicate ledger index row could not be read"
            )
        if _digest(current) != _digest(normalized):
            raise PredictionConflictError(
                "ledger index identity collides with different content"
            )
        return {
            "written": written,
            "duplicate": not written,
            "record": deepcopy(current),
        }

    def append(
        self,
        record: Mapping[str, Any],
        *,
        kind: str,
        identifier: str,
    ) -> Dict[str, Any]:
        if not isinstance(record, Mapping):
            raise PredictionLedgerError("ledger record must be an object")
        key = self.archive_key(
            kind=kind,
            identifier=identifier,
            created_at=record.get("created_at"),
        )
        archived = self._ensure_archive(record, key=key)
        indexed = self._put_index_item(record)
        return {
            **indexed,
            "archived": archived,
            "archive_duplicate": not archived,
            "archive_key": key,
        }

    def write(self, record: Mapping[str, Any]) -> Dict[str, Any]:
        normalized = validate_prediction(record)
        result = self.append(
            normalized,
            kind="records",
            identifier=normalized["prediction_id"],
        )
        result["prediction_id"] = normalized["prediction_id"]
        return result

    def append_grade(self, event: Mapping[str, Any]) -> Dict[str, Any]:
        if event.get("event_type") != "OUTCOME_GRADE":
            raise PredictionLedgerError("grade event_type is invalid")
        grade_id = _identifier(event.get("prediction_id"), "grade prediction_id")
        return self.append(event, kind="grades", identifier=grade_id)


class PredictionLedger:
    """Adapter over a mutable mapping or a DynamoDB-compatible table."""

    def __init__(self, store: Any):
        self.store = store

    def _get(self, prediction_id: str, created_at: Optional[str] = None) -> Optional[Dict[str, Any]]:
        if isinstance(self.store, MutableMapping):
            value = self.store.get(prediction_id)
            return deepcopy(value) if value is not None else None
        if created_at is not None:
            key = {"prediction_id": prediction_id, "created_at": created_at}
            try:
                response = self.store.get_item(Key=key, ConsistentRead=True)
            except TypeError:
                response = self.store.get_item(Key=key)
            value = response.get("Item")
            return deepcopy(value) if value is not None else None
        if hasattr(self.store, "query"):
            response = self.store.query(
                KeyConditionExpression="prediction_id = :prediction_id",
                ExpressionAttributeValues={":prediction_id": prediction_id},
                ConsistentRead=True,
                Limit=2,
            )
            items = response.get("Items") or []
            if len(items) > 1:
                raise PredictionConflictError("prediction_id resolves to multiple ledger rows")
            return deepcopy(items[0]) if items else None
        # Compatibility with hash-only table fakes; production uses query above.
        try:
            response = self.store.get_item(Key={"prediction_id": prediction_id}, ConsistentRead=True)
        except TypeError:
            response = self.store.get_item(Key={"prediction_id": prediction_id})
        value = response.get("Item")
        return deepcopy(value) if value is not None else None

    def write(self, record: Mapping[str, Any]) -> Dict[str, Any]:
        normalized = validate_prediction(record)
        prediction_id = normalized["prediction_id"]
        written = False
        if isinstance(self.store, MutableMapping):
            if prediction_id not in self.store:
                self.store[prediction_id] = deepcopy(normalized)
                written = True
        elif hasattr(self.store, "put_if_absent"):
            written = bool(self.store.put_if_absent(prediction_id, deepcopy(normalized)))
        else:
            try:
                self.store.put_item(
                    Item=deepcopy(normalized),
                    ConditionExpression="attribute_not_exists(prediction_id)",
                )
                written = True
            except Exception as exc:
                if not _is_conditional_failure(exc):
                    raise
        current = normalized if written else self._get(prediction_id, normalized["created_at"])
        if current is None:
            raise PredictionConflictError("duplicate prediction exists but could not be read")
        comparable = tuple(key for key in normalized if key != "status")
        if _digest({key: current.get(key) for key in comparable}) != _digest(
            {key: normalized.get(key) for key in comparable}
        ):
            raise PredictionConflictError("prediction identity collides with different content")
        return {"written": written, "duplicate": not written, "prediction_id": prediction_id, "record": current}

    def grade(self, prediction_id: str, outcome: Mapping[str, Any], *, graded_at: Any) -> Dict[str, Any]:
        prediction_id = _identifier(prediction_id, "prediction_id")
        normalized_outcome = validate_outcome(outcome)
        graded_at_text = _timestamp(graded_at, "graded_at")
        outcome_digest = _digest(normalized_outcome)
        current = self._get(prediction_id)
        if current is None:
            raise PredictionLedgerError("prediction does not exist")
        if current.get("status") == "GRADED":
            if current.get("outcome_digest") != outcome_digest:
                raise PredictionConflictError("prediction already graded with a different outcome")
            return {"graded": False, "duplicate": True, "record": current}
        if current.get("status") != "PENDING":
            raise PredictionConflictError("only PENDING predictions may be graded")
        updated = deepcopy(current)
        updated.update({
            "status": "GRADED",
            "outcome": normalized_outcome,
            "outcome_digest": outcome_digest,
            "graded_at": graded_at_text,
            "correct": current["prediction"] == normalized_outcome["label"],
        })
        if isinstance(self.store, MutableMapping):
            # Re-check immediately before update to make local semantics explicit.
            if self.store[prediction_id].get("status") != "PENDING":
                return self.grade(prediction_id, normalized_outcome, graded_at=graded_at_text)
            self.store[prediction_id] = deepcopy(updated)
        elif hasattr(self.store, "grade_if_pending"):
            if not self.store.grade_if_pending(prediction_id, deepcopy(updated)):
                return self.grade(prediction_id, normalized_outcome, graded_at=graded_at_text)
        else:
            try:
                self.store.update_item(
                    Key={"prediction_id": prediction_id, "created_at": current["created_at"]},
                    UpdateExpression=(
                        "SET #status = :graded, outcome = :outcome, outcome_digest = :digest, "
                        "graded_at = :graded_at, correct = :correct"
                    ),
                    ConditionExpression="#status = :pending",
                    ExpressionAttributeNames={"#status": "status"},
                    ExpressionAttributeValues={
                        ":graded": "GRADED",
                        ":pending": "PENDING",
                        ":outcome": normalized_outcome,
                        ":digest": outcome_digest,
                        ":graded_at": graded_at_text,
                        ":correct": updated["correct"],
                    },
                )
            except Exception as exc:
                if not _is_conditional_failure(exc):
                    raise
                return self.grade(prediction_id, normalized_outcome, graded_at=graded_at_text)
        return {"graded": True, "duplicate": False, "record": updated}
