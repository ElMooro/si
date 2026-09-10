"""Deployable SQS Lambda consumer for SignalEnvelope/v1 materialization."""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
from typing import Any, Dict, Mapping

from queue_consumer_runtime import S3IdempotencyStore, digest, handle_sqs_batch
from signal_envelope import (
    envelope_fingerprint,
    parse_timestamp,
    require_clean,
)


class SignalMaterializationError(ValueError):
    pass


_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/#-]{0,255}$")


def _required_text(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not _NAME.fullmatch(text):
        raise SignalMaterializationError("%s is missing or invalid" % field)
    return text


def _finite(value: Any, field: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
    ):
        raise SignalMaterializationError("%s must be finite" % field)
    return float(value)


def build_feature_record(envelope: Mapping[str, Any]) -> Dict[str, Any]:
    """Convert one clean envelope to the reviewed signal Feature Group schema."""
    normalized = require_clean(envelope, purpose="Feature Store materialization")
    payload = normalized["payload"]
    signal_name = _required_text(
        payload.get("signal_name", normalized["signal_id"]), "payload.signal_name"
    )
    value = _finite(payload.get("value"), "payload.value")
    confidence = _finite(payload.get("confidence", 1.0), "payload.confidence")
    if not 0 <= confidence <= 1:
        raise SignalMaterializationError(
            "payload.confidence must be between zero and one"
        )
    quality = str(payload.get("quality_status", "VERIFIED")).strip().upper()
    if quality not in ("VERIFIED", "PROVISIONAL", "REVISED"):
        raise SignalMaterializationError("payload.quality_status is invalid")
    source_uri = str(payload.get("source_uri") or normalized["source"]).strip()
    if not source_uri or len(source_uri) > 2048:
        raise SignalMaterializationError("payload.source_uri is invalid")

    fingerprint = envelope_fingerprint(normalized)
    lineage_id = hashlib.sha256(
        json.dumps(
            normalized["provenance"], sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()
    event_epoch = parse_timestamp(normalized["event_time"]).timestamp()
    available_epoch = parse_timestamp(normalized["available_at"]).timestamp()
    values = {
        "record_id": fingerprint,
        "event_time": "%.6f" % event_epoch,
        "available_time": "%.6f" % available_epoch,
        "asset_id": normalized["entity_id"],
        "signal_name": signal_name,
        "signal_value": repr(value),
        "confidence": repr(confidence),
        "source_uri": source_uri,
        "content_hash": fingerprint,
        "schema_version": normalized["schema_version"],
        "lineage_id": lineage_id,
        "quality_status": quality,
        "taint": "CLEAN",
    }
    return {
        "operation_id": fingerprint,
        "record": [
            {"FeatureName": name, "ValueAsString": value}
            for name, value in values.items()
        ],
        "normalized_envelope": normalized,
    }


def process_event(
    event: Mapping[str, Any],
    *,
    feature_store_runtime: Any,
    idempotency_store: S3IdempotencyStore,
    feature_group_name: str,
) -> Dict[str, Any]:
    if event.get("detail-type") != "SignalEnvelope/v1":
        raise SignalMaterializationError("unexpected EventBridge detail-type")
    built = build_feature_record(event["detail"])
    payload = built["normalized_envelope"]
    receipt = idempotency_store.begin(built["operation_id"], payload)
    if receipt["complete"]:
        return {"duplicate": True, "materialized": False}
    feature_store_runtime.put_record(
        FeatureGroupName=feature_group_name,
        Record=built["record"],
    )
    result = {
        "duplicate": receipt["duplicate"],
        "materialized": True,
        "feature_group_name": feature_group_name,
        "record_id": built["operation_id"],
    }
    idempotency_store.complete(built["operation_id"], payload, result)
    return result


def handle(
    event: Mapping[str, Any],
    *,
    feature_store_runtime: Any,
    s3_client: Any,
    feature_group_name: str,
    receipt_bucket: str,
) -> Dict[str, Any]:
    store = S3IdempotencyStore(
        s3_client, receipt_bucket, "ai/consumers/signal-feature/v1"
    )
    return handle_sqs_batch(
        event,
        lambda item: process_event(
            item,
            feature_store_runtime=feature_store_runtime,
            idempotency_store=store,
            feature_group_name=feature_group_name,
        ),
    )


def lambda_handler(event: Mapping[str, Any], context: Any) -> Dict[str, Any]:
    """AWS Lambda entry point; tests use ``handle`` with injected clients."""
    import boto3

    feature_group = os.environ.get("SIGNAL_FEATURE_GROUP", "").strip()
    receipt_bucket = os.environ.get("AI_PRIVATE_BUCKET", "").strip()
    if not feature_group or not receipt_bucket:
        raise SignalMaterializationError(
            "SIGNAL_FEATURE_GROUP and AI_PRIVATE_BUCKET are required"
        )
    return handle(
        event,
        feature_store_runtime=boto3.client("sagemaker-featurestore-runtime"),
        s3_client=boto3.client("s3"),
        feature_group_name=feature_group,
        receipt_bucket=receipt_bucket,
    )
