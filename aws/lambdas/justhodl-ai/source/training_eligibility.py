"""Mandatory outcome-label and leakage-aware evaluation gate for training.

Classifier and fine-tune launchers accept only the opaque ``TrainingEligibility``
returned here.  This prevents HTTP handlers and future callers from treating
outcome labels or purged evaluation as optional request metadata.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Mapping, Sequence, Tuple
from urllib.parse import urlparse

from prediction_ledger import validate_outcome
from validation_splits import (
    SampleInterval,
    Split,
    combinatorial_purged_cv_splits,
    purged_walk_forward_splits,
)


ELIGIBILITY_SCHEMA_VERSION = "1.0"
PURPOSE = "MARKET_OUTCOME_CLASSIFICATION"
_DIGEST = re.compile(r"^[a-fA-F0-9]{64}$")
_VERIFICATION_TOKEN = object()
_TRUSTED_EVIDENCE_TOKEN = object()
EVIDENCE_PREFIX = "ai/governance/training-evidence/v1/"
IMMUTABLE_INPUT_PREFIX = "sha256/"
RECEIPT_ISSUER = "justhodl-ai-evaluation-pipeline"


class TrainingEligibilityError(ValueError):
    """Training evidence is incomplete, unverifiable, or leakage-prone."""


@dataclass(frozen=True)
class ImmutableTrainingEvidence:
    """Evidence assembled only from digest-verified, versioned server artifacts."""

    document: Mapping[str, Any]
    receipt_digest: str
    dataset_artifact_digest: str
    evaluation_artifact_digest: str
    training_input_uri: str
    validation_input_uri: str
    training_input_digest: str
    validation_input_digest: str
    training_input_version_id: str
    validation_input_version_id: str
    _verification_token: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._verification_token is not _TRUSTED_EVIDENCE_TOKEN:
            raise TrainingEligibilityError(
                "ImmutableTrainingEvidence must be created by a trusted resolver"
            )


def _read_bytes(value: Any, field: str) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, str):
        return value.encode("utf-8")
    if hasattr(value, "read"):
        body = value.read()
        return body if isinstance(body, bytes) else str(body).encode("utf-8")
    raise TrainingEligibilityError("%s body is unreadable" % field)


def _artifact_ref(
    value: Any, field: str, *, allowed_bucket: str, allowed_prefix: str
) -> Dict[str, str]:
    item = _object(value, field)
    if set(item) != {"uri", "version_id", "sha256"}:
        raise TrainingEligibilityError(
            "%s must contain only uri, version_id and sha256" % field
        )
    uri = str(item.get("uri") or "").strip()
    parsed = urlparse(uri)
    if (
        parsed.scheme != "s3"
        or parsed.netloc != allowed_bucket
        or not parsed.path.lstrip("/").startswith(allowed_prefix)
    ):
        raise TrainingEligibilityError("%s is outside the trusted evidence prefix" % field)
    version_id = str(item.get("version_id") or "").strip()
    digest = str(item.get("sha256") or "").lower()
    if not version_id or version_id == "null":
        raise TrainingEligibilityError("%s.version_id is required" % field)
    if not _DIGEST.fullmatch(digest) or digest == "0" * 64:
        raise TrainingEligibilityError("%s.sha256 must be a non-zero SHA-256 digest" % field)
    return {
        "uri": uri,
        "bucket": parsed.netloc,
        "key": parsed.path.lstrip("/"),
        "version_id": version_id,
        "sha256": digest,
    }


def _source_input_uri(value: Any, role: str, allowed_bucket: str) -> str:
    uri = str(value or "").strip()
    parsed = urlparse(uri)
    key = parsed.path.lstrip("/")
    if parsed.scheme != "s3" or parsed.netloc != allowed_bucket or not key.startswith("ai/"):
        raise TrainingEligibilityError(
            "%s source URI must be in the governed private ai/ boundary" % role
        )
    if uri.endswith("/"):
        uri += "train.csv" if role == "train" else "validation.csv"
    return uri


class S3TrainingEvidenceResolver:
    """Resolve a receipt and its artifacts from a versioned, private S3 boundary."""

    def __init__(
        self,
        s3_client: Any,
        allowed_bucket: str,
        *,
        allowed_prefix: str = EVIDENCE_PREFIX,
        materialization_bucket: str | None = None,
    ) -> None:
        if s3_client is None or not str(allowed_bucket or "").strip():
            raise TrainingEligibilityError("trusted S3 evidence resolver is not configured")
        prefix = str(allowed_prefix or "").lstrip("/")
        if not prefix or not prefix.endswith("/"):
            raise TrainingEligibilityError("allowed evidence prefix must end with /")
        self.s3 = s3_client
        self.allowed_bucket = str(allowed_bucket).strip()
        self.allowed_prefix = prefix
        self.materialization_bucket = str(materialization_bucket or "").strip()
        if not self.materialization_bucket:
            raise TrainingEligibilityError(
                "dedicated immutable training-input bucket is not configured"
            )

    def _get_json(self, ref: Mapping[str, str], field: str) -> Tuple[Mapping[str, Any], str]:
        raw, actual = self._get_bytes(ref, field)
        try:
            value = json.loads(raw)
        except (TypeError, ValueError) as exc:
            raise TrainingEligibilityError("%s must contain JSON" % field) from exc
        return _object(value, field), actual

    def _get_bytes(
        self, ref: Mapping[str, str], field: str
    ) -> Tuple[bytes, str]:
        try:
            response = self.s3.get_object(
                Bucket=ref["bucket"],
                Key=ref["key"],
                VersionId=ref["version_id"],
            )
            returned_version = str(response.get("VersionId") or "")
            if returned_version != ref["version_id"]:
                raise TrainingEligibilityError(
                    "%s returned an unexpected object version" % field
                )
            raw = _read_bytes(response.get("Body"), field)
        except TrainingEligibilityError:
            raise
        except Exception as exc:
            raise TrainingEligibilityError("%s could not be retrieved" % field) from exc
        actual = hashlib.sha256(raw).hexdigest()
        if actual != ref["sha256"]:
            raise TrainingEligibilityError("%s digest does not match immutable content" % field)
        return raw, actual

    def _materialize_input(
        self, ref: Mapping[str, str], role: str
    ) -> Tuple[str, str, str]:
        raw, digest = self._get_bytes(ref, role + " input")
        key = "%s%s/%s/payload.data" % (IMMUTABLE_INPUT_PREFIX, digest, role)
        try:
            self.s3.put_object(
                Bucket=self.materialization_bucket,
                Key=key,
                Body=raw,
                ContentType="application/octet-stream",
                Metadata={"sha256": digest, "immutable": "content-addressed"},
                IfNoneMatch="*",
            )
        except Exception:
            # A pre-existing content-addressed object is acceptable only when
            # its current bytes still match the address. Access errors,
            # missing objects, or mutations all fail at the verification read.
            pass
        try:
            current = self.s3.get_object(
                Bucket=self.materialization_bucket, Key=key
            )
            version_id = str(current.get("VersionId") or "").strip()
            if not version_id or version_id == "null":
                raise TrainingEligibilityError(
                    "%s immutable input has no non-null version ID" % role
                )
            current_raw = _read_bytes(current.get("Body"), role + " immutable input")
            listing = self.s3.list_objects_v2(
                Bucket=self.materialization_bucket,
                Prefix=key.rsplit("/", 1)[0] + "/",
                MaxKeys=2,
            )
            keys = [
                str(item.get("Key") or "")
                for item in listing.get("Contents", [])
                if isinstance(item, Mapping)
            ]
        except Exception as exc:
            raise TrainingEligibilityError(
                "%s immutable input could not be materialized" % role
            ) from exc
        if hashlib.sha256(current_raw).hexdigest() != digest:
            raise TrainingEligibilityError(
                "%s immutable content-addressed input was mutated" % role
            )
        if keys != [key]:
            raise TrainingEligibilityError(
                "%s immutable prefix must contain exactly one payload object" % role
            )
        return (
            "s3://%s/%s" % (self.materialization_bucket, key),
            digest,
            version_id,
        )

    def resolve(self, reference: Any) -> ImmutableTrainingEvidence:
        receipt_ref = _artifact_ref(
            reference,
            "governance_evidence",
            allowed_bucket=self.allowed_bucket,
            allowed_prefix=self.allowed_prefix + "receipts/",
        )
        receipt, receipt_digest = self._get_json(receipt_ref, "governance receipt")
        if (
            receipt.get("schema_version") != ELIGIBILITY_SCHEMA_VERSION
            or receipt.get("issuer") != RECEIPT_ISSUER
            or receipt.get("status") != "VERIFIED"
        ):
            raise TrainingEligibilityError(
                "governance receipt is not a VERIFIED evaluation-pipeline receipt"
            )
        dataset_ref = _artifact_ref(
            receipt.get("dataset_artifact"),
            "receipt.dataset_artifact",
            allowed_bucket=self.allowed_bucket,
            allowed_prefix=self.allowed_prefix + "datasets/",
        )
        evaluation_ref = _artifact_ref(
            receipt.get("evaluation_artifact"),
            "receipt.evaluation_artifact",
            allowed_bucket=self.allowed_bucket,
            allowed_prefix=self.allowed_prefix + "evaluations/",
        )
        dataset, dataset_digest = self._get_json(dataset_ref, "dataset artifact")
        evaluation, evaluation_digest = self._get_json(
            evaluation_ref, "evaluation artifact"
        )
        expected_training_input = _source_input_uri(
            dataset.get("training_uri"), "train", self.allowed_bucket
        )
        expected_validation_input = _source_input_uri(
            dataset.get("validation_uri"), "validation", self.allowed_bucket
        )
        training_ref = _artifact_ref(
            dataset.get("training_input"),
            "dataset.training_input",
            allowed_bucket=self.allowed_bucket,
            allowed_prefix="ai/",
        )
        validation_ref = _artifact_ref(
            dataset.get("validation_input"),
            "dataset.validation_input",
            allowed_bucket=self.allowed_bucket,
            allowed_prefix="ai/",
        )
        if training_ref["uri"] != expected_training_input:
            raise TrainingEligibilityError(
                "dataset.training_input must identify the exact training_uri bytes"
            )
        if validation_ref["uri"] != expected_validation_input:
            raise TrainingEligibilityError(
                "dataset.validation_input must identify the exact validation_uri bytes"
            )
        if (
            dataset.get("schema_version") != ELIGIBILITY_SCHEMA_VERSION
            or evaluation.get("schema_version") != ELIGIBILITY_SCHEMA_VERSION
        ):
            raise TrainingEligibilityError("trusted artifacts have unsupported schema_version")
        for field in ("dataset_id", "training_uri", "validation_uri"):
            if dataset.get(field) != evaluation.get(field):
                raise TrainingEligibilityError(
                    "dataset and evaluation artifacts disagree on %s" % field
                )
        (
            training_input_uri,
            training_input_digest,
            training_input_version_id,
        ) = self._materialize_input(training_ref, "train")
        (
            validation_input_uri,
            validation_input_digest,
            validation_input_version_id,
        ) = self._materialize_input(validation_ref, "validation")
        document = {
            "schema_version": ELIGIBILITY_SCHEMA_VERSION,
            "purpose": dataset.get("purpose"),
            "dataset_id": dataset.get("dataset_id"),
            "training_uri": dataset.get("training_uri"),
            "validation_uri": dataset.get("validation_uri"),
            "immutable_training_uri": training_input_uri,
            "immutable_validation_uri": validation_input_uri,
            "training_input_digest": training_input_digest,
            "validation_input_digest": validation_input_digest,
            "training_input_version_id": training_input_version_id,
            "validation_input_version_id": validation_input_version_id,
            "dataset_digest": hashlib.sha256(
                (
                    "train:"
                    + training_input_digest
                    + "\nvalidation:"
                    + validation_input_digest
                ).encode("ascii")
            ).hexdigest(),
            "samples": dataset.get("samples"),
            "evaluation": evaluation.get("evaluation"),
        }
        return ImmutableTrainingEvidence(
            document=document,
            receipt_digest=receipt_digest,
            dataset_artifact_digest=dataset_digest,
            evaluation_artifact_digest=evaluation_digest,
            training_input_uri=training_input_uri,
            validation_input_uri=validation_input_uri,
            training_input_digest=training_input_digest,
            validation_input_digest=validation_input_digest,
            training_input_version_id=training_input_version_id,
            validation_input_version_id=validation_input_version_id,
            _verification_token=_TRUSTED_EVIDENCE_TOKEN,
        )


@dataclass(frozen=True)
class TrainingEligibility:
    dataset_id: str
    training_uri: str
    validation_uri: str
    source_training_uri: str
    source_validation_uri: str
    training_input_digest: str
    validation_input_digest: str
    training_input_version_id: str
    validation_input_version_id: str
    dataset_digest: str
    evidence_digest: str
    sample_count: int
    labels: Tuple[str, ...]
    walk_forward_split_digest: str
    cpcv_split_digest: str
    _verification_token: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._verification_token is not _VERIFICATION_TOKEN:
            raise TrainingEligibilityError(
                "TrainingEligibility must be created by evaluate_training_eligibility"
            )


def _object(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TrainingEligibilityError("%s must be an object" % field)
    return value


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise TrainingEligibilityError("%s must be a positive integer" % field)
    return value


def _timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise TrainingEligibilityError("%s must be RFC3339" % field)
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise TrainingEligibilityError("%s must be RFC3339" % field) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise TrainingEligibilityError("%s must include a timezone" % field)
    return parsed.astimezone(timezone.utc)


def _canonical_digest(value: Any) -> str:
    try:
        encoded = json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise TrainingEligibilityError("eligibility evidence must be finite JSON") from exc
    return hashlib.sha256(encoded).hexdigest()


def split_digest(values: Sequence[Split]) -> str:
    return _canonical_digest(
        [
            {
                "train": list(item.train_indices),
                "test": list(item.test_indices),
                "test_groups": list(item.test_groups),
            }
            for item in values
        ]
    )


def _gap(config: Mapping[str, Any], field: str) -> timedelta:
    value = config.get(field)
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or value <= 0
    ):
        raise TrainingEligibilityError(
            "%s must be a finite positive number; zero-gap evaluation is ineligible"
            % field
        )
    return timedelta(seconds=float(value))


def _completed_folds(
    value: Any, splits: Sequence[Split], field: str
) -> Tuple[Dict[str, Any], ...]:
    if not isinstance(value, list) or len(value) != len(splits):
        raise TrainingEligibilityError(
            "%s must contain one completed result per generated split" % field
        )
    normalized = []
    for index, (item, split) in enumerate(zip(value, splits)):
        row = _object(item, "%s[%d]" % (field, index))
        if row.get("split") != index or row.get("status") != "COMPLETED":
            raise TrainingEligibilityError(
                "%s[%d] must identify a COMPLETED split" % (field, index)
            )
        if row.get("test_count") != len(split.test_indices):
            raise TrainingEligibilityError(
                "%s[%d].test_count does not match the generated split"
                % (field, index)
            )
        metric = _object(row.get("metric"), "%s[%d].metric" % (field, index))
        name = str(metric.get("name") or "").strip()
        score = metric.get("value")
        if not name:
            raise TrainingEligibilityError("%s[%d].metric.name is required" % (field, index))
        if (
            isinstance(score, bool)
            or not isinstance(score, (int, float))
            or not math.isfinite(float(score))
        ):
            raise TrainingEligibilityError(
                "%s[%d].metric.value must be finite" % (field, index)
            )
        normalized.append(
            {
                "split": index,
                "status": "COMPLETED",
                "test_count": len(split.test_indices),
                "metric": {"name": name, "value": float(score)},
            }
        )
    return tuple(normalized)


def _verified_split_digest(
    config: Mapping[str, Any], values: Sequence[Split], field: str
) -> Tuple[str, Tuple[Dict[str, Any], ...]]:
    actual = split_digest(values)
    supplied = str(config.get("split_digest") or "").lower()
    if supplied != actual:
        raise TrainingEligibilityError(
            "%s.split_digest does not match recomputed purged splits" % field
        )
    folds = _completed_folds(
        config.get("fold_results"), values, field + ".fold_results"
    )
    return actual, folds


def evaluate_training_eligibility(
    evidence: ImmutableTrainingEvidence,
    *,
    expected_dataset_id: str | None = None,
    expected_training_uri: str | None = None,
    expected_validation_uri: str | None = None,
) -> TrainingEligibility:
    """Validate labels, recompute both split families, and return launch proof."""
    if (
        not isinstance(evidence, ImmutableTrainingEvidence)
        or evidence._verification_token is not _TRUSTED_EVIDENCE_TOKEN
    ):
        raise TrainingEligibilityError(
            "governance evidence must be server-resolved from immutable artifacts"
        )
    root = _object(evidence.document, "governance_evidence")
    if root.get("schema_version") != ELIGIBILITY_SCHEMA_VERSION:
        raise TrainingEligibilityError(
            "governance_evidence.schema_version must equal %s"
            % ELIGIBILITY_SCHEMA_VERSION
        )
    if root.get("purpose") != PURPOSE:
        raise TrainingEligibilityError(
            "only MARKET_OUTCOME_CLASSIFICATION training is eligible"
        )
    dataset_id = str(root.get("dataset_id") or "").strip()
    source_training_uri = str(root.get("training_uri") or "").strip()
    source_validation_uri = str(root.get("validation_uri") or "").strip()
    training_uri = str(root.get("immutable_training_uri") or "").strip()
    validation_uri = str(root.get("immutable_validation_uri") or "").strip()
    training_input_digest = str(root.get("training_input_digest") or "").lower()
    validation_input_digest = str(root.get("validation_input_digest") or "").lower()
    training_input_version_id = str(
        root.get("training_input_version_id") or ""
    ).strip()
    validation_input_version_id = str(
        root.get("validation_input_version_id") or ""
    ).strip()
    digest = str(root.get("dataset_digest") or "").lower()
    if not dataset_id:
        raise TrainingEligibilityError("dataset_id is required")
    if not source_training_uri.startswith("s3://") or not source_validation_uri.startswith(
        "s3://"
    ):
        raise TrainingEligibilityError("source training and validation URIs must be s3 URIs")
    materialization_bucket = urlparse(training_uri).netloc
    expected_prefix = "s3://%s/%s" % (
        materialization_bucket, IMMUTABLE_INPUT_PREFIX
    )
    if (
        not training_uri.startswith(expected_prefix)
        or not validation_uri.startswith(expected_prefix)
    ):
        raise TrainingEligibilityError(
            "training inputs must use the immutable content-addressed boundary"
        )
    if any(
        not _DIGEST.fullmatch(value)
        for value in (digest, training_input_digest, validation_input_digest)
    ):
        raise TrainingEligibilityError("training input digests must be SHA-256 digests")
    if (
        not materialization_bucket
        or urlparse(validation_uri).netloc != materialization_bucket
        or not training_input_version_id
        or training_input_version_id == "null"
        or not validation_input_version_id
        or validation_input_version_id == "null"
    ):
        raise TrainingEligibilityError(
            "immutable training inputs require exact non-null version IDs"
        )
    if expected_dataset_id is not None and dataset_id != expected_dataset_id:
        raise TrainingEligibilityError("dataset_id does not match the requested dataset")
    if (
        expected_training_uri is not None
        and source_training_uri != expected_training_uri
    ):
        raise TrainingEligibilityError("training_uri does not match the requested input")
    if (
        expected_validation_uri is not None
        and source_validation_uri != expected_validation_uri
    ):
        raise TrainingEligibilityError(
            "validation_uri does not match the requested input"
        )

    samples = root.get("samples")
    if not isinstance(samples, list) or len(samples) < 8:
        raise TrainingEligibilityError(
            "at least eight outcome-labelled samples are required"
        )
    seen = set()
    intervals = []
    label_values = []
    normalized_samples = []
    for index, raw in enumerate(samples):
        item = _object(raw, "samples[%d]" % index)
        sample_id = str(item.get("sample_id") or "").strip()
        if not sample_id or sample_id in seen:
            raise TrainingEligibilityError("sample_id values must be non-empty and unique")
        seen.add(sample_id)
        outcome = validate_outcome(_object(item.get("outcome"), "samples[%d].outcome" % index))
        start = _timestamp(outcome["entry_time"], "samples[%d].outcome.entry_time" % index)
        end = _timestamp(outcome["exit_time"], "samples[%d].outcome.exit_time" % index)
        intervals.append(SampleInterval(start, end))
        label_values.append(outcome["label"])
        normalized_samples.append(
            {"sample_id": sample_id, "outcome": outcome}
        )
    if any(right.start < left.start for left, right in zip(intervals, intervals[1:])):
        raise TrainingEligibilityError("outcome-labelled samples must be sorted by entry_time")
    labels = tuple(sorted(set(label_values)))
    if len(labels) < 2:
        raise TrainingEligibilityError(
            "outcome-labelled samples must contain at least two outcome classes"
        )

    evaluation = _object(root.get("evaluation"), "evaluation")
    walk = _object(evaluation.get("walk_forward"), "evaluation.walk_forward")
    walk_purge = _gap(walk, "purge_seconds")
    walk_embargo = _gap(walk, "embargo_seconds")
    walk_values = purged_walk_forward_splits(
        intervals,
        n_splits=_positive_int(walk.get("n_splits"), "walk_forward.n_splits"),
        test_size=_positive_int(walk.get("test_size"), "walk_forward.test_size"),
        min_train_size=_positive_int(
            walk.get("min_train_size"), "walk_forward.min_train_size"
        ),
        purge=walk_purge,
        embargo=walk_embargo,
    )
    walk_digest, walk_folds = _verified_split_digest(
        walk, walk_values, "evaluation.walk_forward"
    )

    cpcv = _object(evaluation.get("cpcv"), "evaluation.cpcv")
    cpcv_values = combinatorial_purged_cv_splits(
        intervals,
        n_groups=_positive_int(cpcv.get("n_groups"), "cpcv.n_groups"),
        test_groups=_positive_int(cpcv.get("test_groups"), "cpcv.test_groups"),
        purge=_gap(cpcv, "purge_seconds"),
        embargo=_gap(cpcv, "embargo_seconds"),
    )
    cpcv_digest, cpcv_folds = _verified_split_digest(
        cpcv, cpcv_values, "evaluation.cpcv"
    )

    canonical = {
        "schema_version": ELIGIBILITY_SCHEMA_VERSION,
        "purpose": PURPOSE,
        "dataset_id": dataset_id,
        "source_training_uri": source_training_uri,
        "source_validation_uri": source_validation_uri,
        "training_uri": training_uri,
        "validation_uri": validation_uri,
        "training_input_digest": training_input_digest,
        "validation_input_digest": validation_input_digest,
        "training_input_version_id": training_input_version_id,
        "validation_input_version_id": validation_input_version_id,
        "dataset_digest": digest,
        "receipt_digest": evidence.receipt_digest,
        "evaluation_artifact_digest": evidence.evaluation_artifact_digest,
        "samples": normalized_samples,
        "evaluation": {
            "walk_forward": {
                "n_splits": walk["n_splits"],
                "test_size": walk["test_size"],
                "min_train_size": walk["min_train_size"],
                "purge_seconds": float(walk["purge_seconds"]),
                "embargo_seconds": float(walk["embargo_seconds"]),
                "split_digest": walk_digest,
                "fold_results": list(walk_folds),
            },
            "cpcv": {
                "n_groups": cpcv["n_groups"],
                "test_groups": cpcv["test_groups"],
                "purge_seconds": float(cpcv["purge_seconds"]),
                "embargo_seconds": float(cpcv["embargo_seconds"]),
                "split_digest": cpcv_digest,
                "fold_results": list(cpcv_folds),
            },
        },
    }
    return TrainingEligibility(
        dataset_id=dataset_id,
        training_uri=training_uri,
        validation_uri=validation_uri,
        source_training_uri=source_training_uri,
        source_validation_uri=source_validation_uri,
        training_input_digest=training_input_digest,
        validation_input_digest=validation_input_digest,
        training_input_version_id=training_input_version_id,
        validation_input_version_id=validation_input_version_id,
        dataset_digest=digest,
        evidence_digest=_canonical_digest(canonical),
        sample_count=len(samples),
        labels=labels,
        walk_forward_split_digest=walk_digest,
        cpcv_split_digest=cpcv_digest,
        _verification_token=_VERIFICATION_TOKEN,
    )


def require_training_eligibility(value: Any) -> TrainingEligibility:
    if (
        not isinstance(value, TrainingEligibility)
        or value._verification_token is not _VERIFICATION_TOKEN
    ):
        raise TrainingEligibilityError(
            "classifier/fine-tune launch requires verified TrainingEligibility"
        )
    return value
