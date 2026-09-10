"""Deployable SQS Lambda consumer for SageMaker model-governance events."""
from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, Mapping

from queue_consumer_runtime import (
    IdempotencyConflict,
    S3IdempotencyStore,
    canonical_json,
    digest,
    handle_sqs_batch,
)
from signal_envelope import format_timestamp, parse_timestamp


class ModelGovernanceConsumerError(ValueError):
    pass


_EVENT_ID = re.compile(r"^[A-Za-z0-9._:/#-]{1,256}$")


def normalize_event(
    event: Mapping[str, Any], *, package_group_name: str
) -> Dict[str, Any]:
    if event.get("source") != "aws.sagemaker":
        raise ModelGovernanceConsumerError("unexpected EventBridge source")
    if event.get("detail-type") != "SageMaker Model Package State Change":
        raise ModelGovernanceConsumerError("unexpected EventBridge detail-type")
    event_id = str(event.get("id") or "").strip()
    if not _EVENT_ID.fullmatch(event_id):
        raise ModelGovernanceConsumerError("EventBridge id is missing or invalid")
    detail = event["detail"]
    status = detail.get("ModelApprovalStatus") or detail.get("modelApprovalStatus")
    if status not in ("PendingManualApproval", "Approved", "Rejected"):
        raise ModelGovernanceConsumerError("model approval status is invalid")
    arn = str(
        detail.get("ModelPackageArn") or detail.get("modelPackageArn") or ""
    ).strip()
    expected_fragment = ":model-package/%s/" % package_group_name
    if not arn.startswith("arn:aws:sagemaker:") or expected_fragment not in arn:
        raise ModelGovernanceConsumerError(
            "model package is outside the configured package group"
        )
    event_time = format_timestamp(parse_timestamp(event.get("time"), "time"))
    return {
        "schema_version": "1.0",
        "event_id": event_id,
        "event_time": event_time,
        "event_type": "MODEL_PACKAGE_APPROVAL_STATE_CHANGED",
        "model_package_arn": arn,
        "model_package_group": package_group_name,
        "approval_status": status,
        "account": str(event.get("account") or ""),
        "region": str(event.get("region") or ""),
        "detail_digest": digest(detail),
    }


def _put_immutable(s3_client: Any, bucket: str, key: str, value: Mapping[str, Any]) -> bool:
    body = canonical_json(value)
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
            CacheControl="private, no-store",
            ServerSideEncryption="AES256",
            IfNoneMatch="*",
            Metadata={"sha256": digest(value)},
        )
        return True
    except Exception as exc:
        text = (type(exc).__name__ + " " + str(exc)).lower()
        if not any(
            marker in text
            for marker in ("precondition", "conditional", "already exists", "412")
        ):
            raise
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        raw = response["Body"].read()
        existing = json.loads(raw)
    except Exception as exc:
        raise IdempotencyConflict(
            "existing governance archive could not be verified"
        ) from exc
    if digest(existing) != digest(value):
        raise IdempotencyConflict(
            "governance event identity collides with different content"
        )
    return False


def process_event(
    event: Mapping[str, Any],
    *,
    s3_client: Any,
    archive_bucket: str,
    package_group_name: str,
    idempotency_store: S3IdempotencyStore,
) -> Dict[str, Any]:
    normalized = normalize_event(event, package_group_name=package_group_name)
    operation_id = normalized["event_id"]
    receipt = idempotency_store.begin(operation_id, normalized)
    if receipt["complete"]:
        return {"duplicate": True, "archived": False, "event_id": operation_id}
    key = "ai/governance/model-package-events/v1/%s.json" % operation_id
    written = _put_immutable(s3_client, archive_bucket, key, normalized)
    result = {
        "duplicate": not written,
        "archived": True,
        "event_id": operation_id,
        "archive_key": key,
    }
    idempotency_store.complete(operation_id, normalized, result)
    return result


def handle(
    event: Mapping[str, Any],
    *,
    s3_client: Any,
    archive_bucket: str,
    package_group_name: str,
) -> Dict[str, Any]:
    store = S3IdempotencyStore(
        s3_client, archive_bucket, "ai/consumers/model-governance/v1"
    )
    return handle_sqs_batch(
        event,
        lambda item: process_event(
            item,
            s3_client=s3_client,
            archive_bucket=archive_bucket,
            package_group_name=package_group_name,
            idempotency_store=store,
        ),
    )


def lambda_handler(event: Mapping[str, Any], context: Any) -> Dict[str, Any]:
    """AWS Lambda entry point; tests use ``handle`` with an injected client."""
    import boto3

    bucket = os.environ.get("AI_PRIVATE_BUCKET", "").strip()
    package_group = os.environ.get("MODEL_PACKAGE_GROUP", "").strip()
    if not bucket or not package_group:
        raise ModelGovernanceConsumerError(
            "AI_PRIVATE_BUCKET and MODEL_PACKAGE_GROUP are required"
        )
    return handle(
        event,
        s3_client=boto3.client("s3"),
        archive_bucket=bucket,
        package_group_name=package_group,
    )
