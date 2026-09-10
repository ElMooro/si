"""Deployable SQS Lambda consumer for prediction ingestion and grading."""
from __future__ import annotations

import os
import re
from typing import Any, Dict, Mapping

from prediction_ledger import (
    ArchiveFirstPredictionLedger,
    PredictionLedger,
    PredictionLedgerError,
    _digest,
    _timestamp,
    validate_outcome,
    validate_prediction,
)
from queue_consumer_runtime import S3IdempotencyStore, handle_sqs_batch


class PredictionConsumerError(ValueError):
    pass


def _grade_event(
    detail: Mapping[str, Any],
    *,
    table: Any,
) -> Dict[str, Any]:
    prediction_id = str(detail.get("prediction_id") or "").strip()
    if not re.fullmatch(r"pred-[a-f0-9]{40}", prediction_id):
        raise PredictionConsumerError("prediction_id is invalid")
    original = PredictionLedger(table)._get(prediction_id)
    if original is None:
        raise PredictionConsumerError("prediction does not exist")
    outcome = validate_outcome(detail.get("outcome"))
    graded_at = _timestamp(detail.get("graded_at"), "graded_at")
    grade_id = "grade-" + _digest(
        {"prediction_id": prediction_id, "outcome": outcome}
    )[:40]
    return {
        "schema_version": "1.0",
        "event_type": "OUTCOME_GRADE",
        "prediction_id": grade_id,
        "subject_prediction_id": prediction_id,
        "created_at": outcome["exit_time"],
        "graded_at": graded_at,
        "outcome": outcome,
        "outcome_digest": _digest(outcome),
        "correct": original.get("prediction") == outcome["label"],
        "model_id": original.get("model_id"),
        "model_version": original.get("model_version"),
        "entity_id": original.get("entity_id"),
    }


def process_event(
    event: Mapping[str, Any],
    *,
    table: Any,
    s3_client: Any,
    archive_bucket: str,
    idempotency_store: S3IdempotencyStore,
) -> Dict[str, Any]:
    if event.get("detail-type") != "Prediction/v2":
        raise PredictionConsumerError("unexpected EventBridge detail-type")
    detail = event["detail"]
    action = str(detail.get("action") or "WRITE").strip().upper()
    authority = ArchiveFirstPredictionLedger(table, s3_client, archive_bucket)
    if action == "WRITE":
        raw = detail.get("prediction", detail)
        prediction = validate_prediction(raw)
        operation_id = prediction["prediction_id"]
        payload = {"action": action, "prediction": prediction}
        receipt = idempotency_store.begin(operation_id, payload)
        if receipt["complete"]:
            return {"duplicate": True, "ingested": False, "operation_id": operation_id}
        appended = authority.write(prediction)
        result = {
            "duplicate": appended["duplicate"],
            "ingested": True,
            "operation_id": operation_id,
            "archive_key": appended["archive_key"],
        }
    elif action == "GRADE":
        grade = _grade_event(detail, table=table)
        operation_id = grade["prediction_id"]
        payload = {"action": action, "grade": grade}
        receipt = idempotency_store.begin(operation_id, payload)
        if receipt["complete"]:
            return {"duplicate": True, "graded": False, "operation_id": operation_id}
        appended = authority.append_grade(grade)
        result = {
            "duplicate": appended["duplicate"],
            "graded": True,
            "correct": grade["correct"],
            "operation_id": operation_id,
            "archive_key": appended["archive_key"],
        }
    else:
        raise PredictionConsumerError("prediction event action must be WRITE or GRADE")
    idempotency_store.complete(operation_id, payload, result)
    return result


def handle(
    event: Mapping[str, Any],
    *,
    table: Any,
    s3_client: Any,
    archive_bucket: str,
    receipt_bucket: str,
) -> Dict[str, Any]:
    store = S3IdempotencyStore(
        s3_client, receipt_bucket, "ai/consumers/prediction-ledger/v1"
    )
    return handle_sqs_batch(
        event,
        lambda item: process_event(
            item,
            table=table,
            s3_client=s3_client,
            archive_bucket=archive_bucket,
            idempotency_store=store,
        ),
    )


def lambda_handler(event: Mapping[str, Any], context: Any) -> Dict[str, Any]:
    """AWS Lambda entry point; tests use ``handle`` with injected clients."""
    import boto3

    table_name = os.environ.get("PREDICTION_LEDGER_TABLE", "").strip()
    archive_bucket = os.environ.get(
        "PREDICTION_LEDGER_ARCHIVE_BUCKET", ""
    ).strip()
    receipt_bucket = os.environ.get("AI_PRIVATE_BUCKET", "").strip()
    if not table_name or not archive_bucket or not receipt_bucket:
        raise PredictionConsumerError(
            "PREDICTION_LEDGER_TABLE, PREDICTION_LEDGER_ARCHIVE_BUCKET, "
            "and AI_PRIVATE_BUCKET are required"
        )
    return handle(
        event,
        table=boto3.resource("dynamodb").Table(table_name),
        s3_client=boto3.client("s3"),
        archive_bucket=archive_bucket,
        receipt_bucket=receipt_bucket,
    )
