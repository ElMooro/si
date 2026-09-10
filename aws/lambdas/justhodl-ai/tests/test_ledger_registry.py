from __future__ import annotations

import json
import io
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "source"
sys.path.insert(0, str(SRC))

from model_registry import (  # noqa: E402
    ModelGovernanceError,
    build_mlflow_lineage_metadata,
    build_model_card_payload,
    build_model_package_request,
    create_model_card,
    publish_mlflow_lineage,
    register_model_package,
    validate_mlflow_lineage_metadata,
)
from governance_control import (  # noqa: E402
    ControlledExecution,
    ExecutionBudget,
    GovernanceApprovalRequired,
    GovernanceBudgetExceeded,
    GovernanceOwnershipError,
)
from prediction_ledger import (  # noqa: E402
    ArchiveFirstPredictionLedger,
    PredictionConflictError,
    PredictionLedger,
    PredictionLedgerError,
    build_prediction,
)

UTC = timezone.utc
T0 = datetime(2026, 9, 1, 14, 0, tzinfo=UTC)


def prediction(**overrides):
    values = {
        "model_id": "btc-direction",
        "model_version": "v2",
        "entity_id": "BTC-USD",
        "prediction_time": T0,
        "feature_as_of": T0 - timedelta(minutes=1),
        "feature_fingerprint": "a" * 64,
        "horizon_value": 21,
        "horizon_unit": "calendar_days",
        "prediction": "POSITIVE",
        "score": 0.72,
        "created_at": T0,
        "metadata": {"run_id": "run-1"},
    }
    values.update(overrides)
    return build_prediction(**values)


def outcome(label="POSITIVE", net_return=0.04):
    return {
        "schema_version": "1.0",
        "label": label,
        "entry_time": T0.isoformat(),
        "exit_time": (T0 + timedelta(days=21)).isoformat(),
        "net_return": net_return,
        "transaction_cost": 0.001,
    }


class PredictionLedgerTests(unittest.TestCase):
    def test_write_is_idempotent_and_id_is_deterministic(self):
        store = {}
        ledger = PredictionLedger(store)
        first = ledger.write(prediction())
        second = ledger.write(prediction())
        self.assertTrue(first["written"])
        self.assertTrue(second["duplicate"])
        self.assertEqual(first["prediction_id"], second["prediction_id"])
        self.assertEqual(len(store), 1)

    def test_grade_is_idempotent_and_conflicting_regrade_is_rejected(self):
        store = {}
        ledger = PredictionLedger(store)
        pred_id = ledger.write(prediction())["prediction_id"]
        first = ledger.grade(pred_id, outcome(), graded_at=T0 + timedelta(days=22))
        second = ledger.grade(pred_id, outcome(), graded_at=T0 + timedelta(days=23))
        self.assertTrue(first["graded"])
        self.assertTrue(first["record"]["correct"])
        self.assertTrue(second["duplicate"])
        with self.assertRaises(PredictionConflictError):
            ledger.grade(pred_id, outcome("NEGATIVE", -0.04), graded_at=T0 + timedelta(days=23))

    def test_same_prediction_remains_deduplicated_after_grading(self):
        ledger = PredictionLedger({})
        pred = prediction()
        pred_id = ledger.write(pred)["prediction_id"]
        ledger.grade(pred_id, outcome(), graded_at=T0 + timedelta(days=22))
        self.assertTrue(ledger.write(pred)["duplicate"])

    def test_future_features_and_unknown_predictions_fail_closed(self):
        with self.assertRaisesRegex(PredictionLedgerError, "feature_as_of"):
            prediction(feature_as_of=T0 + timedelta(seconds=1))
        with self.assertRaisesRegex(PredictionLedgerError, "does not exist"):
            PredictionLedger({}).grade("pred-missing", outcome(), graded_at=T0)

    def test_dynamodb_adapter_uses_composite_key_and_conditional_writes(self):
        class ConditionalCheckFailedException(Exception):
            pass

        class Table:
            def __init__(self):
                self.rows = {}
                self.update_key = None

            def put_item(self, Item, ConditionExpression):
                self.last_condition = ConditionExpression
                key = (Item["prediction_id"], Item["created_at"])
                if key in self.rows:
                    raise ConditionalCheckFailedException("conditional request failed")
                self.rows[key] = dict(Item)

            def get_item(self, Key, ConsistentRead=True):
                return {"Item": self.rows.get((Key["prediction_id"], Key["created_at"]))}

            def query(self, **kwargs):
                prediction_id = kwargs["ExpressionAttributeValues"][":prediction_id"]
                return {"Items": [
                    row for (row_id, _), row in self.rows.items() if row_id == prediction_id
                ][:kwargs["Limit"]]}

            def update_item(self, Key, ExpressionAttributeValues, **kwargs):
                self.update_key = Key
                row = self.rows[(Key["prediction_id"], Key["created_at"])]
                if row["status"] != ExpressionAttributeValues[":pending"]:
                    raise ConditionalCheckFailedException("conditional request failed")
                row.update({
                    "status": ExpressionAttributeValues[":graded"],
                    "outcome": ExpressionAttributeValues[":outcome"],
                    "outcome_digest": ExpressionAttributeValues[":digest"],
                    "graded_at": ExpressionAttributeValues[":graded_at"],
                    "correct": ExpressionAttributeValues[":correct"],
                })

        table = Table()
        ledger = PredictionLedger(table)
        record = prediction()
        pred_id = ledger.write(record)["prediction_id"]
        self.assertTrue(ledger.write(record)["duplicate"])
        result = ledger.grade(pred_id, outcome(), graded_at=T0 + timedelta(days=22))
        self.assertTrue(result["graded"])
        self.assertEqual(
            table.update_key,
            {"prediction_id": pred_id, "created_at": record["created_at"]},
        )


class ArchiveFailure(Exception):
    pass


class ConditionalCheckFailedException(Exception):
    pass


class ArchiveS3:
    def __init__(self):
        self.objects = {}
        self.fail_puts = 0
        self.put_calls = 0

    def put_object(self, Bucket, Key, Body, IfNoneMatch=None, **kwargs):
        self.put_calls += 1
        if self.fail_puts:
            self.fail_puts -= 1
            raise ArchiveFailure("archive unavailable")
        object_key = (Bucket, Key)
        if IfNoneMatch == "*" and object_key in self.objects:
            raise ConditionalCheckFailedException("PreconditionFailed")
        self.objects[object_key] = Body
        return {"ETag": '"archive"'}

    def get_object(self, Bucket, Key):
        try:
            body = self.objects[(Bucket, Key)]
        except KeyError:
            raise KeyError("NoSuchKey")
        return {"Body": io.BytesIO(body)}


class ArchiveIndexTable:
    def __init__(self):
        self.rows = {}
        self.put_calls = 0
        self.fail_puts = 0

    def put_item(self, Item, ConditionExpression):
        self.put_calls += 1
        if self.fail_puts:
            self.fail_puts -= 1
            raise RuntimeError("index unavailable")
        key = (Item["prediction_id"], Item["created_at"])
        if key in self.rows:
            raise ConditionalCheckFailedException("ConditionalCheckFailed")
        self.rows[key] = dict(Item)

    def get_item(self, Key, ConsistentRead=True):
        return {"Item": self.rows.get((Key["prediction_id"], Key["created_at"]))}

    def query(self, **kwargs):
        prediction_id = kwargs["ExpressionAttributeValues"][":prediction_id"]
        return {
            "Items": [
                row
                for (row_id, _), row in self.rows.items()
                if row_id == prediction_id
            ][: kwargs["Limit"]]
        }


class ArchiveFirstPredictionLedgerTests(unittest.TestCase):
    def setUp(self):
        self.s3 = ArchiveS3()
        self.table = ArchiveIndexTable()
        self.ledger = ArchiveFirstPredictionLedger(
            self.table,
            self.s3,
            "ledger-archive",
        )
        self.record = prediction()

    def test_archive_failure_leaves_no_index_state_and_retry_is_safe(self):
        self.s3.fail_puts = 1
        with self.assertRaisesRegex(ArchiveFailure, "archive unavailable"):
            self.ledger.write(self.record)

        self.assertEqual(self.table.put_calls, 0)
        self.assertEqual(self.table.rows, {})
        self.assertEqual(self.s3.objects, {})

        recovered = self.ledger.write(self.record)
        duplicate = self.ledger.write(self.record)
        self.assertTrue(recovered["archived"])
        self.assertTrue(recovered["written"])
        self.assertTrue(duplicate["archive_duplicate"])
        self.assertTrue(duplicate["duplicate"])
        self.assertEqual(len(self.table.rows), 1)
        self.assertEqual(len(self.s3.objects), 1)

    def test_index_failure_after_archive_is_recoverable_without_rewrite(self):
        self.table.fail_puts = 1
        with self.assertRaisesRegex(RuntimeError, "index unavailable"):
            self.ledger.write(self.record)

        self.assertEqual(self.table.rows, {})
        self.assertEqual(len(self.s3.objects), 1)
        archived_body = next(iter(self.s3.objects.values()))

        recovered = self.ledger.write(self.record)
        self.assertTrue(recovered["archive_duplicate"])
        self.assertTrue(recovered["written"])
        self.assertEqual(next(iter(self.s3.objects.values())), archived_body)
        self.assertEqual(len(self.s3.objects), 1)
        self.assertEqual(len(self.table.rows), 1)

    def test_conflicting_immutable_archive_never_touches_index(self):
        key = self.ledger.archive_key(
            kind="records",
            identifier=self.record["prediction_id"],
            created_at=self.record["created_at"],
        )
        self.s3.objects[("ledger-archive", key)] = b'{"different":true}'

        with self.assertRaisesRegex(
            PredictionConflictError, "different content"
        ):
            self.ledger.write(self.record)

        self.assertEqual(self.table.put_calls, 0)
        self.assertEqual(self.table.rows, {})

    def test_grade_events_use_the_same_archive_first_boundary(self):
        event = {
            "schema_version": "1.0",
            "event_type": "OUTCOME_GRADE",
            "prediction_id": "grade-abc123",
            "subject_prediction_id": self.record["prediction_id"],
            "created_at": (T0 + timedelta(days=21)).isoformat(),
            "outcome": outcome(),
        }
        result = self.ledger.append_grade(event)
        self.assertTrue(result["archive_key"].startswith("predictions/grades/"))
        self.assertTrue(result["archived"])
        self.assertTrue(result["written"])


class FakeSageMaker:
    def __init__(self):
        self.request = None

    def create_model_package(self, **request):
        self.request = request
        return {"ModelPackageArn": "arn:aws:sagemaker:us-east-1:123:model-package/pkg/1"}

    def describe_model_package(self, ModelPackageName):
        self.described = ModelPackageName
        return {"ModelApprovalStatus": "PendingManualApproval"}


class FakeModelCards:
    def create_model_card(self, **request):
        self.request = request
        return {"ModelCardArn": "arn:aws:sagemaker:us-east-1:123:model-card/card-1"}

    def describe_model_card(self, ModelCardName):
        self.described = ModelCardName
        return {"ModelCardStatus": "Draft"}


class FakeLineageWriter:
    def __init__(self):
        self.metadata = None

    def log_lineage(self, metadata):
        self.metadata = metadata
        return {
            "run_id": metadata["run_id"],
            "artifact_uri": metadata["artifacts"]["model_artifact_uri"],
        }


TOKEN = "approval-token-123456"
OWNER = "ML Platform"


def control(*, calls=8, cost=5.0):
    return ControlledExecution(
        expected_approval_token=TOKEN,
        expected_owner=OWNER,
        budget=ExecutionBudget(
            max_api_calls=calls, max_estimated_cost_usd=cost
        ),
    )


def card_kwargs():
    return dict(
        model_card_name="btc-card",
        model_id="btc-model",
        model_version="v2",
        owner=OWNER,
        intended_use="Directional research canary",
        limitations=["Not investment advice", "Not approved for live trading"],
        risk_rating="HIGH",
        training_dataset={"digest": "a" * 64},
        evaluation={
            "auc": 0.71,
            "training_eligibility_digest": "d" * 64,
            "walk_forward_split_digest": "e" * 64,
            "cpcv_split_digest": "f" * 64,
        },
        lineage={"run_id": "run-1"},
        created_at=T0,
    )


def lineage_metadata():
    return build_mlflow_lineage_metadata(
        run_id="run-012345",
        experiment_name="btc-direction",
        dataset_digest="a" * 64,
        feature_schema_digest="b" * 64,
        walk_forward_split_digest="c" * 64,
        cpcv_split_digest="d" * 64,
        code_revision="deadbeef",
        signal_envelope_version="1.0",
        training_data_uri="s3://datasets/train.parquet",
        artifact_uri="s3://models/model.tar.gz",
        extra_tags={"team": "ml-platform"},
    )


class RegistryAndLineageTests(unittest.TestCase):
    def test_registry_registration_is_always_pending_manual_approval(self):
        client = FakeSageMaker()
        result = register_model_package(
            client,
            live=True,
            control=control(),
            approval_token=TOKEN,
            owner=OWNER,
            package_group_name="btc-models",
            model_data_url="s3://models/model.tar.gz",
            image_uri="123.dkr.ecr.us-east-1.amazonaws.com/inference:1",
            content_types=["application/json", "application/json"],
            response_types=["application/json"],
            customer_metadata={"run_id": "run-1"},
        )
        self.assertEqual(client.request["ModelApprovalStatus"], "PendingManualApproval")
        self.assertEqual(result["approval_status"], "PendingManualApproval")
        self.assertEqual(result["control_evidence"]["api_calls"], 2)
        self.assertEqual(
            client.request["InferenceSpecification"]["SupportedContentTypes"],
            ["application/json"],
        )

    def test_registry_defaults_to_no_side_effect_dry_run(self):
        client = FakeSageMaker()
        result = register_model_package(
            client,
            package_group_name="btc-models",
            model_data_url="s3://models/model.tar.gz",
            image_uri="123.dkr.ecr.us-east-1.amazonaws.com/inference:1",
            content_types=["application/json"],
            response_types=["application/json"],
        )
        self.assertEqual(result["mode"], "DRY_RUN")
        self.assertEqual(result["side_effects"], 0)
        self.assertIsNone(client.request)

    def test_registry_live_mode_enforces_token_owner_and_budget(self):
        kwargs = dict(
            package_group_name="btc-models",
            model_data_url="s3://models/model.tar.gz",
            image_uri="123.dkr.ecr.us-east-1.amazonaws.com/inference:1",
            content_types=["application/json"],
            response_types=["application/json"],
        )
        with self.assertRaises(GovernanceApprovalRequired):
            register_model_package(
                FakeSageMaker(),
                live=True,
                control=control(),
                approval_token="incorrect-token-value",
                owner=OWNER,
                **kwargs,
            )
        with self.assertRaises(GovernanceOwnershipError):
            register_model_package(
                FakeSageMaker(),
                live=True,
                control=control(),
                approval_token=TOKEN,
                owner="other-owner",
                **kwargs,
            )
        with self.assertRaises(GovernanceBudgetExceeded):
            register_model_package(
                FakeSageMaker(),
                live=True,
                control=control(cost=0.0),
                approval_token=TOKEN,
                owner=OWNER,
                estimated_cost_usd=0.01,
                **kwargs,
            )

    def test_registry_request_rejects_non_s3_artifacts(self):
        with self.assertRaisesRegex(ModelGovernanceError, "s3 URI"):
            build_model_package_request(
                package_group_name="btc-models",
                model_data_url="https://example/model.tar.gz",
                image_uri="123.dkr.ecr.us-east-1.amazonaws.com/inference:1",
                content_types=["application/json"],
                response_types=["application/json"],
            )

    def test_model_card_is_deterministic_draft_with_pending_approval(self):
        kwargs = card_kwargs()
        first = build_model_card_payload(**kwargs)
        second = build_model_card_payload(**kwargs)
        self.assertEqual(first, second)
        self.assertEqual(first["ModelCardStatus"], "Draft")
        content = json.loads(first["Content"])
        self.assertEqual(content["approval"]["status"], "PendingManualApproval")
        self.assertEqual(content["risk_rating"], "HIGH")

    def test_model_card_live_mode_is_controlled_and_verified_draft(self):
        client = FakeModelCards()
        kwargs = card_kwargs()
        kwargs.pop("owner")
        result = create_model_card(
            client,
            live=True,
            control=control(),
            approval_token=TOKEN,
            owner=OWNER,
            **kwargs,
        )
        self.assertEqual(result["model_card_status"], "Draft")
        self.assertEqual(client.request["ModelCardStatus"], "Draft")
        self.assertEqual(result["control_evidence"]["api_calls"], 2)

    def test_mlflow_lineage_round_trips_and_protects_governance_tags(self):
        metadata = lineage_metadata()
        self.assertEqual(validate_mlflow_lineage_metadata(metadata), metadata)
        self.assertEqual(metadata["tags"]["governance.code_revision"], "deadbeef")
        with self.assertRaisesRegex(ModelGovernanceError, "override"):
            build_mlflow_lineage_metadata(
                run_id="run-012345",
                experiment_name="btc-direction",
                dataset_digest="a" * 64,
                feature_schema_digest="b" * 64,
                walk_forward_split_digest="c" * 64,
                cpcv_split_digest="d" * 64,
                code_revision="deadbeef",
                signal_envelope_version="1.0",
                training_data_uri="s3://datasets/train.parquet",
                artifact_uri="s3://models/model.tar.gz",
                extra_tags={"governance.code_revision": "bad"},
            )

    def test_mlflow_live_publication_is_dependency_injected_and_receipted(self):
        writer = FakeLineageWriter()
        metadata = lineage_metadata()
        result = publish_mlflow_lineage(
            writer,
            metadata,
            live=True,
            control=control(),
            approval_token=TOKEN,
            owner=OWNER,
            estimated_cost_usd=0.1,
        )
        self.assertEqual(writer.metadata, metadata)
        self.assertEqual(result["receipt"]["run_id"], metadata["run_id"])
        self.assertEqual(result["control_evidence"]["api_calls"], 4)


if __name__ == "__main__":
    unittest.main()
