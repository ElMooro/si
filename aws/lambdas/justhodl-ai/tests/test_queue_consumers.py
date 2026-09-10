from __future__ import annotations

import hashlib
import io
import json
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "source"
sys.path.insert(0, str(SRC))

import model_governance_consumer as model_consumer  # noqa: E402
import prediction_ledger_consumer as prediction_consumer  # noqa: E402
import signal_feature_consumer as signal_consumer  # noqa: E402
from prediction_ledger import build_prediction  # noqa: E402


T0 = datetime(2026, 9, 9, 12, 0, tzinfo=timezone.utc)


class ConditionalFailure(Exception):
    pass


class FakeS3:
    def __init__(self):
        self.objects = {}

    @staticmethod
    def etag(body):
        return '"%s"' % hashlib.md5(body).hexdigest()  # nosec - test ETag only

    def put_object(
        self, Bucket, Key, Body, IfNoneMatch=None, IfMatch=None, **kwargs
    ):
        current = self.objects.get((Bucket, Key))
        if IfNoneMatch == "*" and current is not None:
            raise ConditionalFailure("PreconditionFailed")
        if IfMatch is not None and (
            current is None or self.etag(current) != IfMatch
        ):
            raise ConditionalFailure("PreconditionFailed")
        body = Body if isinstance(Body, bytes) else str(Body).encode()
        self.objects[(Bucket, Key)] = body
        return {"ETag": self.etag(body)}

    def get_object(self, Bucket, Key):
        try:
            body = self.objects[(Bucket, Key)]
        except KeyError as exc:
            raise KeyError("NoSuchKey") from exc
        return {"Body": io.BytesIO(body), "ETag": self.etag(body)}


class FakeFeatureStoreRuntime:
    def __init__(self):
        self.requests = []

    def put_record(self, **request):
        self.requests.append(request)


class FakeTable:
    def __init__(self):
        self.rows = {}

    def put_item(self, Item, ConditionExpression):
        key = (Item["prediction_id"], Item["created_at"])
        if key in self.rows:
            raise ConditionalFailure("ConditionalCheckFailed")
        self.rows[key] = dict(Item)

    def get_item(self, Key, ConsistentRead=True):
        return {
            "Item": self.rows.get(
                (Key["prediction_id"], Key.get("created_at"))
            )
        }

    def query(self, **request):
        prediction_id = request["ExpressionAttributeValues"][":prediction_id"]
        return {
            "Items": [
                item
                for (identifier, _), item in self.rows.items()
                if identifier == prediction_id
            ][: request["Limit"]]
        }


def sqs(message_id, event):
    return {"messageId": message_id, "body": json.dumps(event)}


def envelope():
    return {
        "schema_version": "1.0",
        "signal_id": "momentum-1",
        "source": "justhodl.engine",
        "entity_id": "BTC-USD",
        "event_time": T0.isoformat(),
        "available_at": (T0 + timedelta(seconds=5)).isoformat(),
        "produced_at": (T0 + timedelta(seconds=6)).isoformat(),
        "payload": {
            "signal_name": "momentum",
            "value": 0.7,
            "confidence": 0.8,
            "source_uri": "s3://private/signals/1.json",
        },
        "provenance": [
            {"source_id": "prices", "fingerprint": "a" * 64, "tainted": False}
        ],
        "taint": {"status": "CLEAN", "reasons": []},
    }


class QueueConsumerTests(unittest.TestCase):
    def test_signal_batch_partial_failure_and_retry_deduplication(self):
        s3 = FakeS3()
        feature_store = FakeFeatureStoreRuntime()
        valid = {
            "detail-type": "SignalEnvelope/v1",
            "detail": envelope(),
        }
        invalid = {"detail-type": "Wrong", "detail": {}}
        result = signal_consumer.handle(
            {"Records": [sqs("good", valid), sqs("bad", invalid)]},
            feature_store_runtime=feature_store,
            s3_client=s3,
            feature_group_name="signal-group",
            receipt_bucket="private",
        )
        self.assertEqual(result, {"batchItemFailures": [{"itemIdentifier": "bad"}]})
        self.assertEqual(len(feature_store.requests), 1)
        values = {
            row["FeatureName"]: row["ValueAsString"]
            for row in feature_store.requests[0]["Record"]
        }
        self.assertEqual(values["asset_id"], "BTC-USD")
        self.assertEqual(values["signal_name"], "momentum")
        self.assertEqual(values["taint"], "CLEAN")

        repeated = signal_consumer.handle(
            {"Records": [sqs("good-redrive", valid)]},
            feature_store_runtime=feature_store,
            s3_client=s3,
            feature_group_name="signal-group",
            receipt_bucket="private",
        )
        self.assertEqual(repeated["batchItemFailures"], [])
        self.assertEqual(len(feature_store.requests), 1)

    def test_prediction_write_and_grade_are_archive_first_and_idempotent(self):
        s3 = FakeS3()
        table = FakeTable()
        prediction = build_prediction(
            model_id="btc-direction",
            model_version="v2",
            entity_id="BTC-USD",
            prediction_time=T0,
            feature_as_of=T0 - timedelta(seconds=1),
            feature_fingerprint="b" * 64,
            horizon_value=1,
            horizon_unit="calendar_days",
            prediction="POSITIVE",
            score=0.8,
            created_at=T0,
        )
        write_event = {
            "detail-type": "Prediction/v2",
            "detail": {"action": "WRITE", "prediction": prediction},
        }
        for message_id in ("write-1", "write-redrive"):
            result = prediction_consumer.handle(
                {"Records": [sqs(message_id, write_event)]},
                table=table,
                s3_client=s3,
                archive_bucket="archive",
                receipt_bucket="private",
            )
            self.assertEqual(result["batchItemFailures"], [])
        record_keys = [
            key for bucket, key in s3.objects if bucket == "archive"
            and key.startswith("predictions/records/")
        ]
        self.assertEqual(len(record_keys), 1)

        grade_event = {
            "detail-type": "Prediction/v2",
            "detail": {
                "action": "GRADE",
                "prediction_id": prediction["prediction_id"],
                "graded_at": (T0 + timedelta(days=1, minutes=1)).isoformat(),
                "outcome": {
                    "schema_version": "1.0",
                    "label": "POSITIVE",
                    "entry_time": (T0 + timedelta(minutes=1)).isoformat(),
                    "exit_time": (T0 + timedelta(days=1)).isoformat(),
                    "net_return": 0.03,
                    "transaction_cost": 0.001,
                },
            },
        }
        result = prediction_consumer.handle(
            {"Records": [sqs("grade-1", grade_event)]},
            table=table,
            s3_client=s3,
            archive_bucket="archive",
            receipt_bucket="private",
        )
        self.assertEqual(result["batchItemFailures"], [])
        self.assertEqual(len(table.rows), 2)
        self.assertTrue(
            any(item.get("event_type") == "OUTCOME_GRADE" for item in table.rows.values())
        )

    def test_model_governance_consumer_enforces_package_group_and_archives_once(self):
        s3 = FakeS3()
        event = {
            "id": "event-1",
            "source": "aws.sagemaker",
            "detail-type": "SageMaker Model Package State Change",
            "time": T0.isoformat(),
            "account": "123456789012",
            "region": "us-east-1",
            "detail": {
                "ModelPackageArn": (
                    "arn:aws:sagemaker:us-east-1:123456789012:"
                    "model-package/justhodl-ai-prod/7"
                ),
                "ModelApprovalStatus": "PendingManualApproval",
            },
        }
        for message_id in ("governance-1", "governance-redrive"):
            result = model_consumer.handle(
                {"Records": [sqs(message_id, event)]},
                s3_client=s3,
                archive_bucket="private",
                package_group_name="justhodl-ai-prod",
            )
            self.assertEqual(result["batchItemFailures"], [])
        archives = [
            key
            for bucket, key in s3.objects
            if key.startswith("ai/governance/model-package-events/")
        ]
        self.assertEqual(len(archives), 1)

        wrong = json.loads(json.dumps(event))
        wrong["id"] = "event-2"
        wrong["detail"]["ModelPackageArn"] = wrong["detail"][
            "ModelPackageArn"
        ].replace("justhodl-ai-prod", "other-group")
        result = model_consumer.handle(
            {"Records": [sqs("wrong-group", wrong)]},
            s3_client=s3,
            archive_bucket="private",
            package_group_name="justhodl-ai-prod",
        )
        self.assertEqual(
            result, {"batchItemFailures": [{"itemIdentifier": "wrong-group"}]}
        )

    def test_consumer_template_keeps_every_mapping_disabled(self):
        template = json.loads((ROOT / "consumer-functions.template.json").read_text())
        resources = template["Resources"]
        mappings = [
            value
            for value in resources.values()
            if value["Type"] == "AWS::Lambda::EventSourceMapping"
        ]
        functions = [
            value
            for value in resources.values()
            if value["Type"] == "AWS::Lambda::Function"
        ]
        self.assertEqual(len(mappings), 3)
        self.assertTrue(all(item["Properties"]["Enabled"] is False for item in mappings))
        self.assertTrue(
            all(
                item["Properties"]["FunctionResponseTypes"]
                == ["ReportBatchItemFailures"]
                for item in mappings
            )
        )
        self.assertEqual(
            {item["Properties"]["Handler"] for item in functions},
            {
                "signal_feature_consumer.lambda_handler",
                "prediction_ledger_consumer.lambda_handler",
                "model_governance_consumer.lambda_handler",
            },
        )

    def test_consumer_template_owns_three_exact_queue_roles(self):
        template = json.loads((ROOT / "consumer-functions.template.json").read_text())
        self.assertNotIn("ConsumerExecutionRoleArn", template["Parameters"])
        resources = template["Resources"]
        expected = {
            "SignalFeatureConsumer": (
                "SignalFeatureConsumerRole",
                "SignalQueueArn",
                {
                    "PollOnlySourceQueue",
                    "WriteOnlyOwnLogStreams",
                    "UseReviewedDataKey",
                    "ReadWriteSignalReceipts",
                    "PutOnlyGovernedSignalFeatures",
                },
            ),
            "PredictionLedgerConsumer": (
                "PredictionLedgerConsumerRole",
                "PredictionQueueArn",
                {
                    "PollOnlySourceQueue",
                    "WriteOnlyOwnLogStreams",
                    "UseReviewedDataKey",
                    "ReadWritePredictionReceipts",
                    "AppendPredictionArchive",
                    "AppendPredictionIndex",
                },
            ),
            "ModelGovernanceConsumer": (
                "ModelGovernanceConsumerRole",
                "ModelGovernanceQueueArn",
                {
                    "PollOnlySourceQueue",
                    "WriteOnlyOwnLogStreams",
                    "UseReviewedDataKey",
                    "ReadWriteGovernanceReceiptsAndArchive",
                },
            ),
        }
        queue_actions = {
            "sqs:ReceiveMessage",
            "sqs:DeleteMessage",
            "sqs:ChangeMessageVisibility",
            "sqs:GetQueueAttributes",
        }
        for function_id, (role_id, queue_ref, exact_sids) in expected.items():
            with self.subTest(function=function_id):
                self.assertEqual(
                    resources[function_id]["Properties"]["Role"],
                    {"Fn::GetAtt": [role_id, "Arn"]},
                )
                role = resources[role_id]
                self.assertEqual(role["Type"], "AWS::IAM::Role")
                policy = role["Properties"]["Policies"][0]["PolicyDocument"]
                statements = {item["Sid"]: item for item in policy["Statement"]}
                self.assertEqual(set(statements), exact_sids)
                poll = statements["PollOnlySourceQueue"]
                self.assertEqual(set(poll["Action"]), queue_actions)
                self.assertEqual(poll["Resource"], {"Ref": queue_ref})
                self.assertIn("WriteOnlyOwnLogStreams", statements)
                self.assertIn("UseReviewedDataKey", statements)
                self.assertTrue(
                    all(item.get("Resource") != "*" for item in statements.values())
                )

        signal = resources["SignalFeatureConsumerRole"]["Properties"]["Policies"][0][
            "PolicyDocument"
        ]
        signal = {item["Sid"]: item for item in signal["Statement"]}
        self.assertEqual(
            signal["PutOnlyGovernedSignalFeatures"]["Action"],
            ["sagemaker:PutRecord"],
        )
        self.assertEqual(
            signal["UseReviewedDataKey"]["Condition"]["StringEquals"][
                "kms:ViaService"
            ],
            {"Fn::Sub": "s3.${AWS::Region}.amazonaws.com"},
        )

        prediction = resources["PredictionLedgerConsumerRole"]["Properties"][
            "Policies"
        ][0]["PolicyDocument"]
        prediction = {item["Sid"]: item for item in prediction["Statement"]}
        self.assertEqual(
            set(prediction["AppendPredictionIndex"]["Action"]),
            {"dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:Query"},
        )
        self.assertEqual(
            {
                json.dumps(item, sort_keys=True)
                for item in prediction["AppendPredictionArchive"]["Resource"]
            },
            {
                json.dumps(
                    {
                        "Fn::Sub": (
                            "arn:${AWS::Partition}:s3:::${PredictionArchiveBucket}/"
                            "predictions/records/*"
                        )
                    },
                    sort_keys=True,
                ),
                json.dumps(
                    {
                        "Fn::Sub": (
                            "arn:${AWS::Partition}:s3:::${PredictionArchiveBucket}/"
                            "predictions/grades/*"
                        )
                    },
                    sort_keys=True,
                ),
            },
        )

        model = resources["ModelGovernanceConsumerRole"]["Properties"]["Policies"][
            0
        ]["PolicyDocument"]
        model = {item["Sid"]: item for item in model["Statement"]}
        self.assertEqual(
            model["UseReviewedDataKey"]["Condition"]["StringEquals"][
                "kms:ViaService"
            ],
            {"Fn::Sub": "s3.${AWS::Region}.amazonaws.com"},
        )


if __name__ == "__main__":
    unittest.main()
